package stork.dls.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stork.dls.rabbitmq.Publisher.PublishPackage;
import stork.dls.rest.RestInterface;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

// connection from edge to cloud.
public class EdgeSender implements WaitNotifySndRecv{

  private static final Logger logger = LoggerFactory.getLogger(EdgeSender.class);
  // use edge DLS hostname as the routingKey, so DLS cloud knows where requests come from.
  private static final String bindKey = RestInterface.hostname;
  
  private String remoteHost = "";
  
  private volatile boolean stop = false;
  private boolean upload_waiton = false;
  
  private Queue<JSONObject> request_locks = new ConcurrentLinkedQueue<JSONObject>();
  
  private HashMap<String, JSONObject> pending_requests = new HashMap<String, JSONObject>();
  
  private Object queueing_lock = new Object();
  private Object request_lock = new Object();
  
  private Queue<JSONObject> egress_queue  = new ConcurrentLinkedQueue<JSONObject>();
  private Queue<JSONObject> ingress_queue = new ConcurrentLinkedQueue<JSONObject>();
  
  public final static String UploadRequestTopicExchangeName = "edge-upload-requests";
  public final static String DownloadMetadataTopicExchangeName = "edge-download-metadata";
  
  final static String download_metadata_queuename = "download-metadata-queue";
  
  private Channel upload_channel = null;
  private Channel listen_channel = null;
  
  private WaitNotifyQueue waitnotify_queue = null;
  
  public EdgeSender(String remotehost) throws Exception{
    this.remoteHost = remotehost;
    
    waitnotify_queue = new WaitNotifyQueue(this);
    
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(remotehost);
    Connection connection = factory.newConnection();
    upload_channel = connection.createChannel();
    upload_channel.exchangeDeclare(EdgeSender.UploadRequestTopicExchangeName, "topic");
    
    listen_channel = connection.createChannel();
    listen_channel.exchangeDeclare(EdgeSender.DownloadMetadataTopicExchangeName, "topic");
    Map<String, Object> args = new HashMap<String, Object>();
    AMQP.Queue.DeclareOk ok = listen_channel.queueDeclare(EdgeSender.download_metadata_queuename, true, false, false, args);
    listen_channel.queueBind(EdgeSender.download_metadata_queuename, EdgeSender.DownloadMetadataTopicExchangeName, EdgeSender.bindKey);
    //ok = upload_channel.queueDeclare(EdgeSender.UploadRequestTopicExchangeName, true, false, false, args);
    
    this.listen();
  }
  
  // upload request to queue and then wait for remote server's populating metadata returning back
  // thread-safe in concurrency
  public JSONObject uploadAndwait(JSONObject data) {
    return waitnotify_queue.enqueueAndWait(data);
  }

  public void func(JSONObject data) {
    System.out.println("EdgeSender publish: " + data.toString());
    String msg = data.toString();
    try {
      //upload_channel.basicPublish(EdgeSender.UploadRequestTopicExchangeName, EdgeSender.bindKey, null, msg.getBytes());
    	upload_channel.basicPublish(EdgeSender.UploadRequestTopicExchangeName, EdgeSender.bindKey, null, msg.getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  // await for metadata comming from DLS Cloud
  public void listen() throws Exception{
    boolean autoAck = true;
    Consumer consumer = new DefaultConsumer(listen_channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
        listen_channel.basicAck(envelope.getDeliveryTag(), true);
        
        //TODO: notify the waiting thread for this metadata
        // Assume FIFO
        JSONObject data = new JSONObject(message);
        waitnotify_queue.dequeueAndnotify(data);
      }
    };
    listen_channel.basicConsume(EdgeSender.download_metadata_queuename, autoAck, consumer);
  }
  
  
  public static void main(String[] args) throws Exception{
    
  }
}
