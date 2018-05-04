package stork.dls.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

// on cloud, receiving requests from edge, then call DLS to retrieve metadata, then reply back to sender.
public class CloudReceiver implements WaitNotifySndRecv{

  private static final Logger logger = LoggerFactory.getLogger(CloudReceiver.class);
  
  Channel listen_channel = null;
  WaitNotifyQueue waitandnotify_queue = null;
  
  public final static String listen_edge_requests_queuename = "listen-edge-request-queue";
  
  public CloudReceiver(String remotehost) throws Exception{
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(remotehost);
    Connection connection = factory.newConnection();
    listen_channel = connection.createChannel();
    listen_channel.exchangeDeclare(EdgeSender.UploadRequestTopicExchangeName, "topic");
    Map<String, Object> args = new HashMap<String, Object>();
    AMQP.Queue.DeclareOk ok = listen_channel.queueDeclare(CloudReceiver.listen_edge_requests_queuename, true, false, false, args);
    listen_channel.queueBind(CloudReceiver.listen_edge_requests_queuename, EdgeSender.UploadRequestTopicExchangeName, "#");
    this.listen();
    
    waitandnotify_queue = new WaitNotifyQueue(this);
  }
  // sender/receiver defined func
  public void func(JSONObject data) {
    //TODO: create a thread here to invoke DLS api to retrieve metadata
    System.out.println("");
  }
  
  // listen edge's requests
  public void listen() throws Exception{
    logger.info("CloudReceiver start listen");
    System.out.println("CloudReceiver start listen");
    boolean autoAck = true;
    Consumer consumer = new DefaultConsumer(listen_channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
        listen_channel.basicAck(envelope.getDeliveryTag(), true);
        
        JSONObject data = new JSONObject(message);
        Gson gson = new Gson();
        String uuid = gson.fromJson((String)data.get("uuid"), String.class);
        String metadata = gson.fromJson((String)data.get("path"), String.class);

        waitandnotify_queue.enqueue(data);
      }
    };
    listen_channel.basicConsume(CloudReceiver.listen_edge_requests_queuename, autoAck, consumer);
  }
  
  public static void main(String[] args) throws Exception{
    CloudReceiver rcver = new CloudReceiver("localhost");
  }

}
