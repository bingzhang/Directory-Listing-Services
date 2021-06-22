package stork.dls.util;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.Exchange;
import com.sleepycat.je.DatabaseException;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.*;
import org.json.JSONObject;

import stork.dls.ad.Ad;
import stork.dls.config.DLSConfig;
import stork.dls.io.local.DBCache;

import java.io.IOException;

class MetaData {
	String key;
	String value;
	public MetaData(String key, String value) {
		this.key = key; this.value = value;
	}
}

class UploadData {
	String serverName; String pathEntry; String metadataString;
	public UploadData (String serverName, String pathEntry, String metadataString) {
		this.serverName = serverName;
		this.pathEntry = pathEntry;
		this.metadataString = metadataString;
	}
}

public class Rabbitmq {
  	private static String HOST_NAME = DLSConfig.REPLICA_QUEUE_HOST;
  	private static String XCHG_NAME = "DLS";
  	private static final String QUEUE_NAME = "replicaQueue";
  	private ConnectionFactory factory;
  	private Connection connection;
    private static Scanner scanner = new Scanner(System.in);
    private static String message = "";
    private Channel producer_channel = null;
    private Channel consumer_channel = null;
    private volatile boolean stopped = false;
    private Queue<MetaData> download_queue = new ConcurrentLinkedQueue<MetaData>();
    private Queue<UploadData> upload_queue = new ConcurrentLinkedQueue<UploadData>();
    private boolean upload_waiton = false;
    private boolean download_waiton = false;
    private DBCache dbcache = null;
    
    public Rabbitmq (DBCache dbcache) throws IOException, TimeoutException {
    	this.dbcache = dbcache;
    	generateChannels();
    	new Thread(new ConsumerThread()).start();
    	new Thread(new WorkerThread()).start();
    	new Thread(new ProducerThread()).start();
    	System.out.println("[Rabbitmq] is loading");
    }
    
	private void generateChannels() throws IOException, TimeoutException{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        Connection connection = factory.newConnection();
        this.producer_channel = connection.createChannel();
        this.consumer_channel = connection.createChannel();
	}    
	// 1 worker thread to get from download_queue to write to local DB.
    private class WorkerThread implements Runnable{
	    public void run () {
	    	while(!stopped) {
			    //System.out.println(" worker thread is running!~");
			    MetaData metadata = null;
			    if (!download_queue.isEmpty()) {
			    	metadata = download_queue.poll();
			    	JSONObject jsonobj = new JSONObject(metadata.value);
			    	String mdtime = jsonobj.getString("mdtm");
			    	System.out.println("[Download from Queue] " + "MDTM: "+ mdtime + "; " + metadata.key);
			    	String[] tmp = metadata.key.split("@");
			    	String serverName = null; String pathEntry = null;
			    	if(null != tmp && 2 == tmp.length) {
			    		serverName = tmp[0]; pathEntry = tmp[1];
			    		try {
			    			if (dbcache.contains(serverName, pathEntry)) {
			    				//compare timestamp
			    				String localcopy = Ad.parse(dbcache.Lookup(serverName, pathEntry)).toJSON();
			    				String local_mdtm = new JSONObject(localcopy).getString("mdtm");
			    				if (TimeConverter.getTimeStamp(mdtime) <= TimeConverter.getTimeStamp(local_mdtm)) {
			    					System.out.println("[Skip] " + "MDTM: "+ mdtime + "; " + metadata.key);
			    					continue;
			    				}
			    			}
			    		} catch (Exception ex) {
			    			ex.printStackTrace();
			    		}
			    	}
			    	// store the metadata into db
			    	try {
						dbcache.put(serverName, pathEntry, metadata.value, false);
					} catch (DatabaseException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
			    	System.out.println("[WorkerThread putinto DB] " + "MDTM: "+ mdtime + "; " + metadata.key);
			    } else {
			    	synchronized ( download_queue ) {
			    		if (download_queue.isEmpty()) {
			    			download_waiton = true;
					    	try {
								download_queue.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
							} finally {
								download_waiton = false;
							}
			    		}
			    	}
			    }
	    	}
		    System.out.println(" worker thread is existing!~");
	    }
	}	
    // 1 consumer thread to get message from replica queue.
	private Consumer getConsumer(final Channel channel, String tag) {
		return new DefaultConsumer(channel) {
	         @Override
	         public void handleDelivery(String consumerTag,
	                                    Envelope envelope,
	                                    AMQP.BasicProperties properties,
	                                    byte[] body)
	             throws IOException
	         {
	        	 String message = new String(body, "UTF-8");
	        	 //System.out.println(tag + " [x] Received '" + message);
	             String routingKey = envelope.getRoutingKey();
	             //String contentType = properties.getContentType();
	             download_queue.offer(new MetaData(routingKey, message));
	             synchronized (download_queue) {
	            	 if(!download_queue.isEmpty() && download_waiton) {
	            		 download_queue.notify();
	            	 }
	             }
	        	 long deliveryTag = envelope.getDeliveryTag();
	             
	             // (process the message components here ...)
	             channel.basicAck(deliveryTag, false);
	         }
	     };
	}private class ConsumerThread implements Runnable {
    	public void run () {
    		String queueName = QUEUE_NAME;
    		try {
    			consumer_channel.basicQos(1);// set prefetching counts
    			consumer_channel.exchangeDeclare("destination", "topic", true, false, null);
    			//consumer_channel.exchangeBind("destination", XCHG_NAME, "#");
    			consumer_channel.queueBind(queueName, "destination", "#");
    			boolean autoAck = false;
        		Consumer consumer1 = getConsumer(consumer_channel, "Consumer1");
        		consumer_channel.basicConsume(queueName, autoAck, "Consumer1", consumer1);
    		} catch (Exception ex) {
    			ex.printStackTrace();
    		}
    	}
    }
	// 1 producer thread to upload metadata to replica queue from local upload_queue.
	private class ProducerThread implements Runnable {
    	public void run () {
    		System.out.println("ProducerThread is running");
    		while (!stopped) {
    			if (!upload_queue.isEmpty()) {
    				UploadData data = upload_queue.poll();
    				String serverName = data.serverName;
    				String pathEntry = data.pathEntry;
    				String metadataString = data.metadataString;
    				try {
			    		producer_channel.exchangeDeclare(XCHG_NAME, "topic", true, false, null);
			    		String routingKey = serverName+"@"+pathEntry;
			    		String messageBody = Ad.parse(metadataString).toJSON();
			    		Exchange.BindOk bindStatus = consumer_channel.exchangeBind("destination", XCHG_NAME, routingKey);
			    		producer_channel.basicPublish(XCHG_NAME, routingKey, true,
			    				new AMQP.BasicProperties.Builder()
			    				.contentType("text/plain")
			    				.deliveryMode(2)
			    				//.priority(1)
			    				//.userId("bob")
			    				.build(), 
			    				messageBody.getBytes("UTF-8"));
			        	JSONObject jsonobj = new JSONObject(messageBody);
			    		System.out.println("[Upload to Server Queue] " + "MDTM: "  + jsonobj.getString("mdtm") + "; " + routingKey);
    				} catch (Exception ex) {
    					ex.printStackTrace();
    				}
    			} else {
    				synchronized (upload_queue) {
    					if (upload_queue.isEmpty()) {
    						System.out.println("upload_queue waiting to be populated");
	    					try {
	    						upload_waiton = true;
								upload_queue.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
							} finally {
								upload_waiton = false;
							}
    					}
    				}
    			}
    		}
    		try {
    			producer_channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
    	}		
	}public void upload(String serverName, String pathEntry, String metadataString) {
		UploadData data = new UploadData(serverName, pathEntry, metadataString);		
		upload_queue.offer(data);
		synchronized (upload_queue) {
			if (!upload_queue.isEmpty() && upload_waiton) {
				upload_queue.notify();
			}
		}
	}

	public static void main(String[] args) {
	}
}