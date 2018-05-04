package stork.dls.rabbitmq;

import java.util.HashMap;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class WaitNotifyQueue {
  
  private static final Logger logger = LoggerFactory.getLogger(WaitNotifyQueue.class);
  
  private Queue<JSONObject> pendings  = new ConcurrentLinkedQueue<JSONObject>();
  
  private HashMap<String, JSONObject> to_be_notified_requests = new HashMap<String, JSONObject>();
  
  private volatile boolean stop = false;
  private boolean upload_waiton = false;
  private Object request_lock = new Object();
  private Object queueing_lock = new Object();
  
  private WaitNotifySndRecv snderrecver = null;
  
  public WaitNotifyQueue (WaitNotifySndRecv sndrecv) {
    this.snderrecver = sndrecv;
    new Thread(new RequestsDispatcher()).start();
  }
  
  public void enqueue(JSONObject data) {
    String uuid = UUID.randomUUID().toString();
    data.put("uuid", uuid);
    pendings.offer(data);
    synchronized(request_lock) {
      to_be_notified_requests.put(uuid, data);
    }
    synchronized (queueing_lock) {
      if (!pendings.isEmpty() && upload_waiton) {
        queueing_lock.notify();
      }
    }
  }
  
  // enqueue data and wait for data being processed
  public JSONObject enqueueAndWait(JSONObject data) {
    enqueue(data);
    System.out.println("enqueue: " + data.toString());
    synchronized(data) {
      try {
        data.wait();
        return data;
      } catch (InterruptedException e) {
        e.printStackTrace();
        return null;
      }
    }
  }
  
  private class RequestsDispatcher implements Runnable {
    public void run() {
      while(true && !stop) {
        if (!pendings.isEmpty()) {
          JSONObject data = pendings.poll();
          try {
            snderrecver.func(data);
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          synchronized (queueing_lock) {
            if(pendings.isEmpty()) {
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
  
  // dequeue the pending data and notify who is wating on the completion
  public void dequeueAndnotify(JSONObject result) {
    Gson gson = new Gson();
    String uuid = gson.fromJson((String)result.get("uuid"), String.class);
    String metadata = gson.fromJson((String)result.get("metadata"), String.class);
    JSONObject data = null;

    synchronized(request_lock) {
      // populate data with metadata, then notify.
      data = to_be_notified_requests.remove(uuid);
      data.put("metadata", metadata);
    }
    synchronized(data) {
      data.notify();
    }
  }

  public static void main(String[] args) {

  }

}
