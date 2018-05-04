package stork.dls.rabbitmq;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.rabbitmq.client.*;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

class InterfaceCmdLine implements Runnable {
  Publisher pub = null;
  InterfaceCmdLine(Publisher pub){
    this.pub = pub;
  }
  public void run() {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      try {
        String line = br.readLine();
        if (null == line) break;
        if(!"".equals(line)) {
          String[] inputs = line.split(";");
          pub.uploading_updates(inputs[0], inputs[1], inputs[2]);
          System.out.println(line);
        }
      }catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

// master acts as publisher.
public class Publisher {
  
  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
  
  Channel publish_channel = null;
  private volatile boolean stop = false;
  private boolean upload_waiton = false;
  
  final static String PublishTopicExchangeName = "publish";

  private Object queueing_lock = new Object();
  private Queue<PublishPackage> uploading_queue   = new ConcurrentLinkedQueue<PublishPackage>();
  
  class PublishPackage {
    String serverName; 
    String pathEntry;
    String metadataString;
    PublishPackage(String serverName, String pathEntry, String metadataString) {
      this.serverName = serverName;
      this.pathEntry = pathEntry;
      this.metadataString = metadataString;
    }
  }
  
  // publish channel
  public Publisher(String hostname) throws Exception{
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(hostname);
    Connection connection = factory.newConnection();
    publish_channel = connection.createChannel();
    publish_channel.exchangeDeclare(Publisher.PublishTopicExchangeName, "topic");
    
    new Thread(new Snder()).start();
    System.out.println("connect to Rabbitmq queue: " + hostname);
  }
  
  public void uploading_updates(String serverName, String pathEntry, String metadataString) {
    uploading_queue.offer(new PublishPackage(serverName, pathEntry, metadataString));
    synchronized (queueing_lock) {
      if (!uploading_queue.isEmpty() && upload_waiton) {
        queueing_lock.notify();
      }
    }
  }
  
  private class Snder implements Runnable {
    public void run() {
      while(true && !stop) {
        if (!uploading_queue.isEmpty()) {
          PublishPackage data = uploading_queue.poll();
          try {
            publish(data.serverName, data.pathEntry, data.metadataString);
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          synchronized (queueing_lock) {
            if(uploading_queue.isEmpty()) {
              System.out.println();
              logger.debug("service queue waiting to be populated");
              try {
                upload_waiton = true;
                queueing_lock.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } finally {
              upload_waiton = false;
            }
            }
          }
        }
      }
    }
  }
  
  // publish a update with a bindkey(path)
  private void publish(String servername, String path, String metadata) throws Exception{
    //pack into data
    JSONObject data = new JSONObject();
    data.put("server", new Gson().toJson(servername));
    data.put("path", new Gson().toJson(path));
    data.put("metadata", new Gson().toJson(metadata));
    String msg = data.toString();
    publish_channel.basicPublish(Publisher.PublishTopicExchangeName, path, null, msg.getBytes());
  }
  
  public static void main(String[] args) throws Exception{
    System.out.println("publish");
    Publisher pub = new Publisher("localhost");
    new Thread(new InterfaceCmdLine(pub)).start();
  }

}
