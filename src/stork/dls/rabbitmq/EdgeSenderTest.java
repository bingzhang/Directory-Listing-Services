package stork.dls.rabbitmq;

import org.json.JSONObject;

public class EdgeSenderTest implements Runnable{
  EdgeSender snder = null;
  public EdgeSenderTest(EdgeSender snder) {
    this.snder = snder;
  }
 
  public void run() {
    JSONObject data = new JSONObject();
    data.put("path", "path1");
    snder.uploadAndwait(data);
  }
  
  public static void main(String[] args) throws Exception{
    CloudReceiver recver = new CloudReceiver("localhost");
    EdgeSender snder = new EdgeSender("localhost");
    EdgeSenderTest test = new EdgeSenderTest(snder);
    test.run();
  }

}
