package stork.dls.rabbitmq;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stork.dls.config.DLSConfig;

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
  
  CloseableHttpAsyncClient ayncclient = null;
  
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
    
    ayncclient = HttpAsyncClients.createDefault();
    ayncclient.start();
    
    this.listen();
    
    waitandnotify_queue = new WaitNotifyQueue(this);
  }
  // sender/receiver defined func
  public void func(JSONObject data) {
    Gson gson = new Gson();
    String URI = gson.fromJson((String)data.get("URI"), String.class);
    boolean forceRefresh = gson.fromJson((String)data.get("forceRefresh"), boolean.class);
    boolean enablePrefetch = gson.fromJson((String)data.get("enablePrefetch"), boolean.class);
    try {
	URIBuilder builder = new URIBuilder("http://"+DLSConfig.REPLICA_QUEUE_HOST+"/DirectoryListingService/rest/dls/list");
	builder.setParameter("forceRefresh", String.valueOf(forceRefresh)).setParameter("enablePrefetch", String.valueOf(enablePrefetch))
	.setParameter("URI", URI);
	URI uri = builder.build();
	HttpGet request = new HttpGet(uri);
	ayncclient.execute(request, null);
    } catch (Exception ex) {
    	ex.printStackTrace();
    }
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
