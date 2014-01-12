package stork.dls.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.GuardedBy;

import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Reply;

import stork.dls.client.DLSClient;
import stork.dls.config.DLSConfig;
import stork.dls.io.network.MetaChannel;
import stork.dls.stream.DLSIOAdapter.FETCH_PREFETCH;
import stork.dls.stream.DLSIOAdapter.StreamInfo;
import stork.dls.stream.DLSStream.FAILURE_STATUS;

/**
 * 2D Pool
 * @author bing
 */
public class DLSStreamPool {
	public static boolean DEBUG_PRINT;//false;//true
	static final boolean DEBUG_PRINT_REINCARNATION = false;//false;//true
	static final boolean BALANCER_DEBUG_PRINT = false;//false;//true
	public static final int ONE_PIPE_CAPACITY = DLSStream.ONE_PIPE_CAPACITY;// configurable;
	public static final int NUM_CONCURRENCY_STREAM = DLSConfig.DLS_CONCURRENCY_STREAM;// configurable
	private volatile int value = 0;
	private volatile int waitCount = 0;
	private volatile int notifyCount = 0;	
	public final int MAX_NUM_OPENStream;
	
	@GuardedBy("sync on this")
	boolean activeStreamPool_status[] = null;
	private final BlockingQueue<FetchingTask> fetchingThreadQueue;
	
	public int STRIDE = 1;
	public ArrayList<DLSStream> streamList = null;
	//for debug
	public String StreamDebugKey = null;
	
	private synchronized void valueSanityScan(){
		int i = 0;
		for(int j = 0; j < MAX_NUM_OPENStream; j++){
			if(true == activeStreamPool_status[j]){
				i++;
			}
		}
		this.value = i;
	}
	
	public DLSStreamPool(boolean TwoD) {
		if(TwoD){
			STRIDE = ONE_PIPE_CAPACITY;
		}else{
			STRIDE = 1;
		}
		MAX_NUM_OPENStream = STRIDE*NUM_CONCURRENCY_STREAM;//configurable & negotiable.
		activeStreamPool_status = new boolean[MAX_NUM_OPENStream];
		streamList = new ArrayList<DLSStream>(MAX_NUM_OPENStream);
		value = MAX_NUM_OPENStream;
		fetchingThreadQueue = new LinkedBlockingQueue<FetchingTask>();//unlimited capacity
	}
	
	public int get_MAX_NUM_OPENStream(){
		return MAX_NUM_OPENStream;
	}
	public String get_realprotocol(){
		return streamList.get(0).realprotocol;
	}
	public void setName(String debugInfo) {
		StreamDebugKey = debugInfo;
	}
	
	public void getStreamInfo(StreamInfo senInfo){
		senInfo.available = this.value;
		senInfo.waited = this.waitCount;
	}
	
	private synchronized int balancer(String debuginfo){
		int activeIndx = -1;
		int i = 0;
		int max = 0;
		int group = -1;
		for(i = 0; i < NUM_CONCURRENCY_STREAM; i ++){
			int offset = i*STRIDE;
			int tmp = 0;
			for(int j = 0; j < STRIDE; j ++){
				if(true == activeStreamPool_status[offset+j]){
					tmp ++;
				}
			}
			if(BALANCER_DEBUG_PRINT){
				System.out.print("Group " + i + " has available streams "+ tmp + ";\t");
			}
			if(0 == tmp || tmp == max){
				continue;
			}
			max = Math.max(max, tmp);
			if(max == tmp){
				group = i;
			}
			if(max == STRIDE){
				if(BALANCER_DEBUG_PRINT){
					System.out.println("\nGroup " + i + " has max available streams "+ tmp + " so simply break the loop");
				}
				break;
			}
		}
		if(BALANCER_DEBUG_PRINT){
			System.out.println("we pick the group " + group);
		}
		if(0 <= group){
			int offset = group*STRIDE;
			for(int j = 0; j < STRIDE; j ++){
				if(true == activeStreamPool_status[offset+j]){
					activeIndx = offset+j;
					break;
				}
			}					
		}
		if(BALANCER_DEBUG_PRINT){
			System.out.println("we pick index "+ activeIndx%STRIDE + " from the group " + group +"~!\n\n");
		}
		if(-1 == activeIndx){
			String threadname = Thread.currentThread().getName();
			System.out.println("activeIndx is -1: " +threadname + " ~ " + debuginfo + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
		}
		return activeIndx;
	}
	
	private class FetchingTask extends Thread{
		final String assignedThreadName;
		final String path;
		final Thread supert;
		FetchingTask(Thread supert, String assignedThreadName, String path){
			this.supert = supert;
			this.assignedThreadName = assignedThreadName;
			this.path = path;
		}
		
		//soft interrupt
		public void interrupt() {
			System.out.println("notify{interrupt} fetching task: "+ assignedThreadName + " " + path);
			supert.interrupt();
		}
		/*
		//some1 thread waits on this.
		synchronized void waitOn() throws IOException{
			try {
				System.out.println("fetching wait~!");
				wait();
			} catch (InterruptedException e) {
				System.out.println("FetchingTask got interrupted~!");
				//implictely remove this fetching task from fetchingQueue,
				//since the thread waiting on this object already has been interrputed.
				synchronized(outStreamPool){
					fetchingQueue.remove(this);
				}
				throw new IOException("FetchingTask got interrupted~!");
			}
		}
		*/
		//some1 thread passes through this.
		//needs to explictely remove this fetching task from fetchingQueue
		/*
		synchronized void wakeUp(){
			System.out.println("notify fetching task");
			notify();
		}
		*/
	}
	
    public synchronized DLSStream getAvailableStream(String assignedThreadName, String path, FETCH_PREFETCH doFetching) throws IOException {
   		DLSStream activeStream= null;
   		valueSanityScan();
   		if (value <= waitCount) {
   			FetchingTask fetchtsk = null;
   			waitCount++;
			try {
				do {
					if(DEBUG_PRINT){
						//assignedThreadName = null;
						System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \t suspended~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
					}
					//wait();
					if(FETCH_PREFETCH.FETCH == doFetching){
						fetchtsk = new FetchingTask(Thread.currentThread(), assignedThreadName, path);
						fetchingThreadQueue.offer(fetchtsk);
						System.out.println(assignedThreadName + " fetching " + path);
					}
					wait(); 
				} while (notifyCount == 0);
			} catch (InterruptedException e) {
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \t got interrupted ~!" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
				if(FETCH_PREFETCH.FETCH == doFetching){
					
				}else{
					notify();
				}
			} finally {
				waitCount--;
			}
				notifyCount--;
				int activeIndx = balancer(path);
				activeStream = streamList.get(activeIndx);
				activeStream.activeStreamIndx = activeIndx;
				activeStreamPool_status[activeIndx] = false;
				if(DEBUG_PRINT){
				//assignedThreadName = null;
					System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \tacquire available Stream: " + activeStream.activeStreamIndx + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
				}
				value--;
				if(FETCH_PREFETCH.FETCH == doFetching){
					System.out.println("fetching task got stream and removed~!");
					//fetchingQueue.remove(fetchtsk);
					fetchingThreadQueue.remove();
				}
				//System.out.println("streams: " + activeStream + " index: " + activeStream.activeStreamIndx);
				return activeStream;		    	
			}else{
				int activeIndx = balancer(path);
				activeStream = streamList.get(activeIndx);
				activeStream.activeStreamIndx = activeIndx;
				activeStreamPool_status[activeIndx] = false;
				if(DEBUG_PRINT){
					//assignedThreadName = null;
					System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \t acquire available Stream: " + activeIndx + "; stream = " +activeStream + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
					System.out.println("activeStreamPool_status[" + activeIndx+"]" + " = "+activeStreamPool_status[activeIndx]);
				}
				value--;
				/*
				if(FETCH_PREFETCH.FETCH == doFetching){
					System.out.println("fetching task got stream and removed~!");
					//fetchingQueue.remove(fetchtsk);
					fetchingThreadQueue.remove();
				}*/
				//System.out.println("streams: " + activeStream + " index: " + activeStream.activeStreamIndx);
				return activeStream;	
			}
    }
	    
    private class StreamReincarnation extends Thread{
    	private int itry = 1;
    	private DLSStream StreamReborn; 
        private int activeIndx;
        private final int rebornResource;
        private DLSStreamPool StreamPool = null;
        private String assignedThreadName = null;
        public StreamReincarnation(String assignedThreadName, DLSStream newStream, DLSStreamPool poolSnapshot, int activeIndx, int rebornResource){
        	StreamPool = poolSnapshot;
        	StreamReborn = newStream;
        	this.assignedThreadName = assignedThreadName; 
        	this.activeIndx = activeIndx;
        	this.rebornResource = rebornResource;
        }

        public void run() {
            processCommand();
        }

        private void processCommand() {
        	while(true){
	            try {
	            	// sleep function should be exponential?
	                Thread.sleep(2000*itry);
	                try {
	                	StreamReborn.Authenticate( StreamDebugKey + " Stream "+Integer.toString(activeIndx), null, StreamReborn.token);
	                } catch (Exception e) {
	                	itry ++;
	                	if(DEBUG_PRINT_REINCARNATION){
	                		System.out.println("Stream "+activeIndx+ "trying to reborn but failed, sleep " +2000*itry+" s then retry~!");
	                	}
	                	continue;
	                }
	                /*
	                if(DEBUG_PRINT_REINCARNATION){
	                	System.out.println("StreamReincarnation: "+ StreamReborn + " reincarnation successful~!");
	                }
	                */
            		final int GROUP = this.activeIndx/STRIDE;
            		final int OFFSET = GROUP*STRIDE;
            		//set new dls client
               		{
               			DLSClient client = StreamReborn.dlsclient;
               			for(int j = 0; j < STRIDE; j ++){
            				int indx = OFFSET+j;
            				if(indx == activeIndx){
            					continue;
            				}
            				DLSStream oDls = streamList.get(indx);
            				oDls.setClient(client);
            			}
               		}
            		releaseAvailableStreams(StreamReborn, this.assignedThreadName, null, OFFSET, -1);
	                break;
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
        	}
        }
    }
    
    private class SimpleStreamReincarnation extends Thread{
    	private int itry = 1;
    	private DLSStream StreamReborn; 
        private int activeIndx;
        private int rebornResource;
        private DLSStreamPool StreamPool = null;
        private String assignedThreadName = null;
        public SimpleStreamReincarnation(String assignedThreadName, DLSStream newStream, DLSStreamPool poolSnapshot, int activeIndx, int rebornResource){
        	StreamPool = poolSnapshot;
        	StreamReborn = newStream;
        	this.assignedThreadName = assignedThreadName; 
        	this.activeIndx = activeIndx;
        	this.rebornResource = rebornResource;
        }

        public void run() {
            processCommand();
        }

        private void processCommand() {
        	while(true){
	            try {
	            	// sleep function should be exponential?
	                Thread.sleep(2000*itry);
	                try {
	                	StreamReborn.Authenticate( StreamDebugKey + " Stream "+Integer.toString(activeIndx), null, StreamReborn.token);
	                } catch (Exception e) {
	                	itry ++;
	                	if(DEBUG_PRINT_REINCARNATION){
	                		System.out.println("Stream "+activeIndx+ "trying to reborn but failed, sleep " +2000*itry+" s then retry~!");
	                	}
	                	continue;
	                }
	                /*
	                if(DEBUG_PRINT_REINCARNATION){
	                	System.out.println("StreamReincarnation: "+ StreamReborn + " reincarnation successful~!");
	                }
	                */
            		int group = this.activeIndx/STRIDE;
            		int offset = group*STRIDE;
            		synchronized(this.StreamPool){
            			/*
	            		for(int j = 0; j < STRIDE; j ++){
	            			activeStreamPool_status[offset+j] = true;
	           				//activeStreamPool_status.set(offset+j, DLSStream.AVAILABLE);	
	            		}*/
	            		releaseAvailableStreams(StreamReborn, this.assignedThreadName, null, offset, -1);
	            		StreamReborn.isAvailable = true;
		            	//streamList.set(activeIndx, StreamReborn);
		            	//releaseAvailableStream(null, null, activeIndx);
            		}
	                break;
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
        	}
        }
    }
    
    private class SingleStreamRevive extends Thread{
    	private int itry = 1;
    	private DLSStream StreamReborn; 
        private int activeIndx;
        private DLSStreamPool StreamPool = null;
        private String assignedThreadName = null;
        public SingleStreamRevive(String assignedThreadName, DLSStream newStream, DLSStreamPool poolSnapshot, int activeIndx){
        	StreamPool = poolSnapshot;
        	StreamReborn = newStream;
        	this.assignedThreadName = assignedThreadName; 
        	this.activeIndx = activeIndx;
        }

        public void run() {
        	while(true){
	            try {
	            	// sleep function should be exponential?
	                Thread.sleep(2000*itry);
	                try {
	                	StreamReborn.Authenticate( StreamDebugKey + " Stream "+Integer.toString(activeIndx), null, StreamReborn.token);
	                } catch (Exception e) {
	                	itry ++;
	                	if(DEBUG_PRINT_REINCARNATION){
	                		System.out.println("Stream "+activeIndx+ "trying to reborn but failed, sleep " +2000*itry+" s then retry~!");
	                	}
	                	continue;
	                }

	                if(DEBUG_PRINT_REINCARNATION){
	                	System.out.println("StreamReincarnation: "+ StreamReborn + " reincarnation successful~!");
	                }
	              
            		synchronized(this.StreamPool){
            			releaseThisStream(StreamReborn, assignedThreadName, null, this.activeIndx);
	            		StreamReborn.isAvailable = true;
            		}
	                break;
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
        	}
        }

    }
    
    public DLSStream MigrationStream(String assignedThreadName, String path, DLSStream newStream, int activeIndx){
   		synchronized(newStream){
   			if(FAILURE_STATUS.FAILURE_RETRANSMIT == newStream.stream_failstatus){
				if(DEBUG_PRINT_REINCARNATION){
					System.out.println("FAILURE_RETRANSMIT: RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " newStreamID = "+activeIndx + " path = "+ path + " ! Migration stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
				}
				releaseThisStream(newStream, assignedThreadName, path, activeIndx);
				return null;
   			}
   		}
    	if(1 != STRIDE){
    		return ComplexRecoveryStream(assignedThreadName, path, newStream, activeIndx);
    	}else{
    		return SimpleRecoveryStream(assignedThreadName, path, newStream, activeIndx);
    	}
    }
   		
    private DLSStream SimpleRecoveryStream(String assignedThreadName, String path, DLSStream newStream, int activeIndx){
    	try {
    		Thread.sleep(2000);
			newStream.Authenticate(assignedThreadName + " Stream "+Integer.toString(activeIndx), path, newStream.token);
		} catch (Exception e) {
			if(DEBUG_PRINT){
				//assignedThreadName = null;
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \tStream " + activeIndx + " is dead, wait to get reincarnation~!");
			}
			Thread t = new SingleStreamRevive(assignedThreadName, newStream, this, activeIndx);
			t.start();
			//e.printStackTrace();
			if(DEBUG_PRINT_REINCARNATION){
            	//System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStream = "+newStream + " ! Migrate this path request to another available stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
				System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStreamID = "+activeIndx + " ! Migration stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
			}
			return null;
		}
    	return newStream;
    }
    
    private DLSStream ComplexRecoveryStream(String assignedThreadName, String path, DLSStream newStream, int activeIndx){
    	final int GROUP = activeIndx/STRIDE;
    	final int OFFSET = GROUP*STRIDE;
    	int counter = 0;
   		synchronized(this){
    		if(true == newStream.dlsclient.isAvailable){
    			newStream.dlsclient.isAvailable = false;
                if(DEBUG_PRINT_REINCARNATION){
                	String info = " is the 1st to find Stream's failure ";
                	System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + info + " newStream = "+newStream +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
                }
    			//System.out.println(newStream + "path ("+path+")"+" entered synchronized(this) in recoveryStream~! before for(). this.value is: " + "this is " + this+ this.value+ " activeStreamPool_status[activeIndx] is: "+ activeStreamPool_status[activeIndx]);
	    		for(int j = 0; j < STRIDE; j ++){
	    			if(true == activeStreamPool_status[OFFSET+j]){
	    				activeStreamPool_status[OFFSET+j] = false;//sync happens here!
	    				counter ++;//cause problem?
	    			}
	    		}
	    		int oldvalue = this.value;
	    		//System.out.println(newStream +" entered synchronized(this) in recoveryStream~! after for(). this.value is: " + "this is " + this+ this.value + " activeStreamPool_status[activeIndx] is: "+ activeStreamPool_status[activeIndx]);
	    		valueSanityScan();
	    		//System.out.println(newStream +" entered synchronized(this) in recoveryStream~! after valueSanityScan(). this.value is: " + "this is " + this+ this.value+ "activeStreamPool_status[activeIndx] is: "+ activeStreamPool_status[activeIndx]);
                if(DEBUG_PRINT_REINCARNATION){
                	System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStream = "+newStream + " reduce (" + oldvalue + " to " + this.value + " )"+" waitCount = " + waitCount + " notifyCount = " + notifyCount + " counter = " + counter);
                }
    		}else{
    			if(DEBUG_PRINT_REINCARNATION){
                	System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStream = "+newStream + " !forward this path request to another available stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount + " counter = " + counter);
                }
    			return null;
    		}
    	}

    	try {
    		Thread.sleep(2000);
			newStream.Authenticate(assignedThreadName + " Stream "+Integer.toString(activeIndx), path, newStream.token);
		} catch (Exception e) {
			if(DEBUG_PRINT){
				//assignedThreadName = null;
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \tStream " + activeIndx + " is dead, wait to get reincarnation~!");
			}
			Thread t = new StreamReincarnation(assignedThreadName, newStream, this, activeIndx, STRIDE);
			t.start();
			//e.printStackTrace();
			if(DEBUG_PRINT_REINCARNATION){
            	System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStream = "+newStream + " !forward this path request to another available stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount + " counter = " + counter);
            }
			return null;
		}
   		//set new dls client
   		{
   			DLSClient client = newStream.dlsclient;
   			for(int j = 0; j < STRIDE; j ++){
				int indx = OFFSET+j;
				if(indx == activeIndx){
					continue;
				}
				DLSStream oDls = streamList.get(indx);
				oDls.setClient(client);
			}
   		}
   		
   		releaseAvailableStreams(newStream, assignedThreadName, path, OFFSET, activeIndx);
    	return newStream;
    }

    private synchronized void releaseAvailableStreams(DLSStream thisStream, String assignedThreadName, String path, int offset, final int activeIndx){
    	if(DEBUG_PRINT_REINCARNATION){
    		System.out.println("1.) releaseAvailableStreams(): "+ assignedThreadName + " path: " + path + "; value: " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);	
    	}
    	valueSanityScan();
    	if(DEBUG_PRINT_REINCARNATION){
    		System.out.println("2.) releaseAvailableStreams(): "+ assignedThreadName + " path: " + path + "; value: " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);	
    	}
    	boolean willNotify = false;
    	int willNotifyAll = 0;
    	for(int j = 0; j < STRIDE; j ++){
    		int indx = offset+j;
    		if(activeIndx == indx){
    			activeStreamPool_status[indx] = false;
    			continue;
    		}
    		activeStreamPool_status[offset+j] = true;
    		value++;
    		willNotify = true;
        	if (waitCount > notifyCount) {
    		    notifyCount++;			  
    		    willNotifyAll++;
        	}
    	}
    	if(DEBUG_PRINT_REINCARNATION){
    		System.out.println("3.) releaseAvailableStreams(): "+ assignedThreadName + " path: " + path + "; value: " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);	
    	}
    	if(willNotify){
    		if(willNotifyAll >= 2){
    			notifyAll();
    		}else if (willNotifyAll == 1){
    			notify();
    		}
    	}
    	thisStream.isAvailable = true;
    }
    
    public synchronized void releaseThisStream(DLSStream thisStream, String assignedThreadName, String path, int activeIndx) {
    	/*
		int old = value;
		valueSanityScan();
		if(value != old){
			System.out.println("releaseThisStream cause the problem: old: " + old + " value: " + value);
		}*/
		value++;
		if(true == activeStreamPool_status[activeIndx]){
			System.out.println("why previous is true? activeStreamPool_status[" + activeIndx+"]" + " = "+activeStreamPool_status[activeIndx]);
		}
		activeStreamPool_status[activeIndx] = true;
		if(waitCount > notifyCount) {
			notifyCount++;
			if(DEBUG_PRINT){
				if(assignedThreadName == null && null == path){
					System.out.println("Stream " +activeIndx+ " established a new gsiftp Stream~!");
				System.out.println("debug : Reincarnation Stream [[[notify]]]~" + activeIndx + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
				}else{
					//assignedThreadName = null;
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \trelease Stream [[[notify]]]~" + activeIndx + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
				}
			}
			//notify();
			FetchingTask fetchingtsk = this.fetchingThreadQueue.peek();
			if(null != fetchingtsk){
				fetchingtsk.interrupt();
			}else{
				notify();
			}
		  }else{
			  if(DEBUG_PRINT){
			    	if(assignedThreadName == null && null == path){
			    		System.out.println(assignedThreadName + " established a new gsiftp Stream~!");
		    		System.out.println("debug : Reincarnation Stream [[[notify]]]~" + activeIndx + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
		    	}else{
		    		//assignedThreadName = null;
		    		System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \trelease Stream without notify~" + activeIndx + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
			    	}
			  }
		  }//end else
		int endold = value;
		/*
		valueSanityScan();
		if(value != endold){
			System.out.println("releaseThisStream cause the problem: endold: " + endold + " value: " + value);
		}*/
		thisStream.isAvailable = true;
	}
}	
