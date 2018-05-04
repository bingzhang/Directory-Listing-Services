package stork.dls.rabbitmq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stork.dls.io.local.DBCache;

// helper debug class
class InterfaceCmdLine2 implements Runnable {
  Subscriber sub = null;
  InterfaceCmdLine2(Subscriber sub){
    this.sub = sub;
  }
  public void run() {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      try {
        String line = br.readLine();
        if (null == line) break;
        if(!"".equals(line)) {
          sub.subscribe(line);
          System.out.println(line);
        }
      }catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

public class Subscriber {
  
  private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);
  DBCache db = null;
  Channel listen_publish_channel = null;
  final static String listen_publish_queuename = "listen-publish-queue";
  
  // bind path as the bindkey
  public Subscriber(String hostname, DBCache db) throws Exception{
    this.db = db;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(hostname);
    Connection connection = factory.newConnection();
    listen_publish_channel = connection.createChannel();
    listen_publish_channel.exchangeDeclare(Publisher.PublishTopicExchangeName, "topic");
  }
  // I am not sure about whether it is thread-safe on queueBind.
  public synchronized void subscribe(String bindingKey) throws Exception{
    System.out.println("subscribe: "+bindingKey);
    listen_publish_channel.queueBind(Subscriber.listen_publish_queuename, Publisher.PublishTopicExchangeName, bindingKey);
  }
  
  // await for the updates on bindkey
  public void listen() throws Exception{
    boolean autoAck = true;
    Map<String, Object> args = new HashMap<String, Object>();
    AMQP.Queue.DeclareOk ok = listen_publish_channel.queueDeclare(Subscriber.listen_publish_queuename, true, false, false, args);
    Consumer consumer = new DefaultConsumer(listen_publish_channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
        listen_publish_channel.basicAck(envelope.getDeliveryTag(), true);
        
        //TODO: write back to local DB storage.
        if(null != db) {
          //unpack 
          JSONObject data = new JSONObject(message);
          Gson gson = new Gson();
          String serverName = gson.fromJson((String)data.get("server"), String.class);
          String pathEntry = gson.fromJson((String)data.get("path"), String.class);
          String metadatastr = gson.fromJson((String)data.get("metadata"), String.class);
          db.put(serverName, pathEntry, metadatastr, false);
        }
        //TODO: should do notify if any wait on?
      }
    };
    listen_publish_channel.basicConsume(Subscriber.listen_publish_queuename, autoAck, consumer);
  }
  
  public static void main(String[] args) throws Exception{
    Subscriber sub = new Subscriber("localhost", null);
    sub.listen();
    new Thread(new InterfaceCmdLine2(sub)).start();
  }

}
