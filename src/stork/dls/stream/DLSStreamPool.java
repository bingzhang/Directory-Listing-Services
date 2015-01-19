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
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSIOAdapter.FETCH_PREFETCH;
import stork.dls.stream.DLSIOAdapter.StreamInfo;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
import stork.dls.stream.DLSStream.REINCARNATION_STATUS;

/**
 * 
 * @author bing
 */
public class DLSStreamPool {
	public static boolean DEBUG_PRINT;//false;//true
	public static boolean FETECHING_DEBUG_PRINT = true;//false;//true
	static final boolean DEBUG_PRINT_REINCARNATION = true;//false;//true
	static final boolean BALANCER_DEBUG_PRINT = false;//false;//true
	public static final int ONE_PIPE_CAPACITY = DLSStream.ONE_PIPE_CAPACITY;// configurable;
	public static final int NUM_CONCURRENCY_STREAM = DLSConfig.DLS_CONCURRENCY_STREAM;// configurable
	private volatile int value = 0;
	private volatile int waitCount = 0;
	private volatile int notifyCount = 0;	
	public final int MAX_NUM_OPENStream;
	@GuardedBy("DLSStreamPool")
	private final BlockingQueue<FetchingTask> fetchingThreadQueue;
	
	public ArrayList<DLSStream> streamList = null;
	//for debug
	public String StreamDebugKey = null;
	
	
	public DLSStreamPool() {
		MAX_NUM_OPENStream = NUM_CONCURRENCY_STREAM;//configurable & negotiable.
		streamList = new ArrayList<DLSStream>(MAX_NUM_OPENStream);
		//value = MAX_NUM_OPENStream*ONE_PIPE_CAPACITY;
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
	/**
	 * 
	 * @return active stream index
	 */
	private synchronized int updateValue(){
		int i;
    	int activeIndx = -1;
		int min = 0;
		DLSStream stream = null;
		int capacity = 0;
		value = 0;
    	for(i = 0; i < NUM_CONCURRENCY_STREAM; i ++){
    	    stream = streamList.get(i);
    	    capacity = stream.getValue();
			if(min < capacity){
				activeIndx = i;
				min = capacity;
			}
			value += capacity;
    	}//end for
    	if(value > NUM_CONCURRENCY_STREAM*ONE_PIPE_CAPACITY){
    	    System.out.println("lower layer capacity is wrong!!");
    	}
    	if(-1 == activeIndx){
    		value = 0;// should be the same as "value = available"
    	}
    	return activeIndx;
	}
	
	private synchronized int getActiveStreamIndx(String debuginfo){
	   	/** check available capacity of streams
    	 *  if there is no available stream, then wait here.
    	 */
		DLSStream activeStream = null;
    	int activeIndx = updateValue();
    	if(-1 != activeIndx){
    		activeStream = streamList.get(activeIndx);
    		boolean success = activeStream.reserveValue();
    		if(success){
    		    return activeIndx;
    		}
    	}
		return -1;
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
			System.out.println("notify{interrupt} fetching task: "+ path);
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
    	updateValue();
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
						if(FETECHING_DEBUG_PRINT){
						    System.out.println("queue in: " + path);
						}
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
			int activeIndx = getActiveStreamIndx(path);
			/**
			 * all though notified, still there could be no available stream due to the stream broken.
			 */
			if(-1 == activeIndx){
                //notify();// need to notify?
                return null;
			}
			activeStream = streamList.get(activeIndx);
			if(DEBUG_PRINT){
				//assignedThreadName = null;
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \tacquire available Stream: " + activeStream.streamID + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
			}
			/*//move this part into releaseStream, make sure releaseStream is reentrant
			if(FETCH_PREFETCH.FETCH == doFetching){
			    if(FETECHING_DEBUG_PRINT){
			        System.out.println("queue pop: " + path);
			    }
				//fetchingQueue.remove(fetchtsk);
				fetchingThreadQueue.remove();
			}*/
			//System.out.println("streams: " + activeStream + " index: " + activeStream.activeStreamIndx);
			return activeStream;		    	
    	}else{
			int activeIndx = getActiveStreamIndx(path);
			if(-1 == activeIndx){
			    return null;
			}
			activeStream = streamList.get(activeIndx);
			if(DEBUG_PRINT){
				//assignedThreadName = null;
				System.out.println("debug : thread( " + assignedThreadName +" ); path ("+path+ " )"+" \t acquire available Stream: " + activeIndx + "; stream = " +activeStream + "~" + " value = " + value + " waitCount = " + waitCount + " notifyCount = " + notifyCount);
			}
			//System.out.println("streams: " + activeStream + " index: " + activeStream.activeStreamIndx);
			return activeStream;	
		}
    }
       
    public DLSStream MigrationStream(DLSListingTask listingtask, String assignedThreadName, String path, DLSStream newStream, int activeIndx){
   		DLSClient proxyclient = listingtask.getClient();
		if(CHANNEL_STATE.DC_RETRANSMIT == proxyclient.channelstate){
			if(DEBUG_PRINT_REINCARNATION){
				System.out.println("RecoveryStream: DC_RETRANSMIT: " + " assignedThreadName = " 
			    + assignedThreadName + " newStreamID = "+activeIndx + " path = "+ path);
			}
		    DLSStream stream = proxyclient.getStream();
		    if(null != stream){
    		    synchronized(this){
    		        stream.increaseValue();
    		    }
		    }
		    
		    //releaseThisStream(newStream, assignedThreadName, path, activeIndx);
			return null;
		}
		if(CHANNEL_STATE.CC_REVIVE == proxyclient.channelstate){
            if(DEBUG_PRINT_REINCARNATION){
                System.out.println("RecoveryStream: CC_REVIVE: " + " assignedThreadName = " 
                + assignedThreadName + " newStreamID = "+activeIndx + " path = "+ path);
            }
		//if(false == newStream.isAvailable && REINCARNATION_STATUS.NOT_PROCESSING == newStream.isReincarnation){
		    //newStream.isReincarnation = REINCARNATION_STATUS.PROCESSING;
		    return SimpleRecoveryStream(listingtask, assignedThreadName, path, newStream, activeIndx);
		}else{
            if(CHANNEL_STATE.DC_IGNORE ==proxyclient.channelstate){
                DLSStream stream = proxyclient.getStream();
                System.out.println("available streams: " + stream.value);
            }
		    return null;
		}
   		
   		
    }

    public synchronized void releaseThisStream(DLSStream thisStream, String assignedThreadName, String path, int activeIndx) {
        thisStream.increaseValue();
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
                if(FETECHING_DEBUG_PRINT){
                    System.out.println(path + " notify " + fetchingtsk.path);
                }
			    if(FETECHING_DEBUG_PRINT){
			        System.out.println("queue pop: " + path);
			    }
				fetchingThreadQueue.remove();
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
    }    
    
    private synchronized void helpRelease(){
        if(waitCount > notifyCount) {
            notifyCount++;
            int i = 0;
            for(i = 0; i < DLSStreamPool.ONE_PIPE_CAPACITY; i++){
                FetchingTask fetchingtsk = this.fetchingThreadQueue.peek();
                if(null != fetchingtsk){
                    fetchingtsk.interrupt();
                }else{
                    notify();
                }                
            }
        }
    }
    
    private DLSStream SimpleRecoveryStream(final DLSListingTask listingtask, String assignedThreadName, String path, DLSStream newStream, int activeIndx){
    	try {
    		Thread.sleep(2000);
			newStream.Authenticate(listingtask, assignedThreadName + " Stream "+Integer.toString(activeIndx), path, newStream.token);
			helpRelease();
		} catch (Exception e) {
			if(DEBUG_PRINT_REINCARNATION){
				//assignedThreadName = null;
				System.out.println("SimpleRecoveryStream: thread( " + assignedThreadName +" ); path ("+path+ " )"+" \tStream " + activeIndx + " is dead, wait to get reincarnation~!");
			}
			Thread t = new SingleStreamRevive(listingtask, assignedThreadName, newStream, this, activeIndx);
			t.start();
			//e.printStackTrace();
			if(DEBUG_PRINT_REINCARNATION){
            	//System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStream = "+newStream + " ! Migrate this path request to another available stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
				System.out.println("RecoveryStream : " + " assignedThreadName = " + assignedThreadName + " path = "+ path + " newStreamID = "+activeIndx + " ! Migration stream! " +" waitCount = " + waitCount + " notifyCount = " + notifyCount);
			}
			return null;
		}
        if(DEBUG_PRINT_REINCARNATION){
            System.out.println("OK: stream "+ newStream.streamID + " reincarnation successful~! " + activeIndx);
        }
    	return newStream;
    }
    
    private class SingleStreamRevive extends Thread{
        private int itry = 1;
        private DLSStream StreamReborn; 
        private int activeIndx;
        private DLSStreamPool StreamPool = null;
        private String assignedThreadName = null;
        private DLSListingTask listingtask = null;
        public SingleStreamRevive(final DLSListingTask listingtask, 
                String assignedThreadName, DLSStream newStream, DLSStreamPool poolSnapshot, int activeIndx){
            this.listingtask = listingtask;
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
                        StreamReborn.Authenticate(listingtask, StreamDebugKey + " Stream "+Integer.toString(activeIndx), null, StreamReborn.token);
                    } catch (Exception e) {
                        itry ++;
                        if(DEBUG_PRINT_REINCARNATION){
                            System.out.println("Stream "+activeIndx+ "trying to reborn but failed, sleep " +2000*itry+" s then retry~!");
                        }
                        continue;
                    }
                    //releaseThisStream(StreamReborn, assignedThreadName, null, this.activeIndx);
                    helpRelease();
                    if(DEBUG_PRINT_REINCARNATION){
                        System.out.println("OK: stream "+ StreamReborn.streamID + " reincarnation successful~! " + activeIndx);
                    }
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }    
}	
