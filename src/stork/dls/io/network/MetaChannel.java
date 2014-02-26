package stork.dls.io.network;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.globus.ftp.Buffer;
import org.globus.ftp.DataSink;
import org.globus.ftp.HostPort;
import org.globus.ftp.dc.DataChannelFactory;
import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.SimpleDataChannelFactory;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.BasicServerControlChannel;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;

import stork.dls.client.DLSClient;
import stork.dls.config.DLSConfig;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
import stork.dls.util.DLSLog;
import stork.dls.util.DLSLogTime;
/**
 * <b>what is the MetaChannel?</b><br>
 * MetaChannel acts as a storage of dls network stream (like the buffer for network metadata cmd transferring operations) in queue FIFO order
 * 
 * <br><br>
 * MetaChannel is DLS network I/O scheduler/monitor for one pipestream.<br>
 * MetaChannel never die Except explictlly invoking its close() method.
 * <br><br>
 * MetaChannel use 1 extra thread to read from local meta-channel port
 * <br><br>
 * MetaChannel takes ProtocolControlChannel object as input parameter, 
 * and when there is exception over the network I/O, MetaChannel will simply terminate 
 * that protocolControlChannel object, re-create a new one, continue its work.
 * <br><br>
 * Inside MetaChannel, there is scheduler/monitor thread.
 * @see
 * 		DLSFTPControlChannel
 * @author bing
 *
 */
public class MetaChannel {
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(MetaChannel.class.getName());
	protected HostPort hp = null;
	protected SocketBox socketBox;
	protected final DataChannelFactory dataChannelFactory;
	protected final ReadWriteLock spinlock;
	protected final BufferedReader ftpIn;
	protected final OutputStream ftpOut;
    final private AtomicInteger capacity = new AtomicInteger(0);
	/**
	 * wake-up queue <b>WorkerThread</b>
	 * The queue used for holding tasks and
	 * notify the wait WorkerThread
	 */
	/**
	 * Each submitted list cmd will create one (pipeTask) object in wakupQueue in the sequence of submission.
	 * <br><b>Infinite capacity</b>: because if there is failure,
	 * we need to re-send the whoe pipeTask objects in wakeupQueue.
	 * Or, let each to throw exception.
	 */
	protected MetaChannelMonitor monitor;
	protected final LinkedBlockingQueue<DLSMetaCmdTask> wakupQueue;

	protected MetaChannel(){
	    this.ftpIn = null;
	    this.ftpOut = null;
	    this.monitor = null;
	    this.spinlock = null;
	    this.wakupQueue = null;
	    this.dataChannelFactory = null;
	}
	
	protected void bindMonitor() throws IOException, ServerException{
	    if(null == this.monitor){
	        monitor = new MetaChannelMonitor_Default(socketBox, null);
	    }
	}
	
	public MetaChannel(DLSStream stream, HostPort hp, SocketBox socketBox, 
			BufferedReader ftpIn, OutputStream ftpOut)  throws IOException, ServerException{
        
	    logger.debug("hostport: " + hp.getHost() + " " + hp.getPort());
        spinlock = stream.getspinlock();
	    this.hp = hp;
	    this.socketBox = socketBox;
	    this.ftpIn = ftpIn;
	    this.ftpOut = ftpOut;
		this.dataChannelFactory = new SimpleDataChannelFactory();
		wakupQueue = new LinkedBlockingQueue<DLSMetaCmdTask>();
		bindMonitor();
	}
	
    private void write(Command cmd)
            throws IOException, IllegalArgumentException {
            if (cmd == null) {
                throw new IllegalArgumentException("null argument: cmd");
            }
            writeCmd(cmd.toString());
        }
    
    private void writeCmd(String msg) throws IOException {
        ftpOut.write(msg.getBytes());
        ftpOut.flush();
    }
    
	private synchronized void writeCmdGreedy(DLSMetaCmdTask metaTask) 
			throws 
				ServerException, IOException{
        wakupQueue.offer(metaTask);
        {
            //System.out.println("Synchronized write cmds: " + metaTask);
        }
		List<Command> cmds = metaTask.getCmds();
		Iterator<Command> it = cmds.iterator();
		while(it.hasNext()) {
			Command cmd = it.next();
			write(cmd);
			//DLSLogTime curtime = new DLSLogTime();
			//System.out.println("time: " + curtime.toString() + " SEND "+ cmd.toString());
			//logger.debug(metaTask.getAssignedThreadName() + " SEND "+ cmd.toString());
			//logger.debug("time: " + curtime.toString() + " SEND "+ cmd.toString());
		}
	}

	protected List<Reply> handleReply(DLSMetaCmdTask metaTask)
            throws
                ServerException,
                IOException{	
	       Reply reply = null;
	       List<Reply> replies = new ArrayList<Reply>();
	       Iterator<ReplyParser> replyParser = metaTask.getReplayParserChain().iterator();
	       while(null != (reply = metaTask.waitOn())){
	           if(this.monitor.isTermintate() || metaTask.isException){
	               String exceptionSubject = metaTask.toString();
	               exceptionSubject = exceptionSubject + reply;
	               throw new IOException(exceptionSubject);
	           }
	           boolean doublecheck = replyParser.hasNext();
	           if(false == doublecheck){
	               System.out.println("how n why?");
	               throw new IOException("how n why?");
	           }
	           ReplyParser oneReplyParser = replyParser.next();
	           oneReplyParser.replyParser(reply);
	           logger.debug(oneReplyParser.getCurrentCmd() + " Recv: {\n"+ reply.toString());
	           replies.add(reply);
	       }	    
	    return replies;
	}
	
	//asynchronous method
	final public List<Reply> sendrecv(DLSListingTask listingtask, final String assignedThreadName, List<Command> cmds, List<ReplyParser> replyParserChain, List<LocalReply> writeBacks) 
			throws
				ServerException,
			    IOException{
	   DLSMetaCmdTask metaTask = null; 
	   /**
	    * 1st, the reason why I use spinlock.readlock as the wrapper synchronization
	    * is that in streampool, getAvailableStream, I also use spinlock.readlock, so I want both operations to have
	    * concurrency performance.
	    * 2nd, While this same spinlock, I use writelock here in cc broken and Authentication. Because these two places should be
	    * strict sequential. And can not interleave.
	    */
	   while(true){
	       if(spinlock.readLock().tryLock()){
	           try{
	               if(true != this.monitor.isTermintate()){
    	               metaTask = new DLSMetaCmdTask(listingtask, assignedThreadName, cmds, replyParserChain, writeBacks);
    	               //synchronized write into the shared queue.
    	               writeCmdGreedy(metaTask);
	               }else{
	                   /**
	                    * already observed CC broken
	                    */
	                   DLSClient dummyclient = listingtask.getClient();
	                   dummyclient.channelstate = CHANNEL_STATE.DC_RETRANSMIT;
	                   throw new IOException();
	               }
	           }finally{
	               spinlock.readLock().unlock();
	           }
	           break;
	       }
	   }
  
	   return Collections.unmodifiableList(handleReply(metaTask));
	}

    private class ByteArrayDataSink implements DataSink {

        private ByteArrayOutputStream received;

        public ByteArrayDataSink() {
            this.received = new ByteArrayOutputStream(1000);
        }
        
        public void write(Buffer buffer) throws IOException {
            this.received.write(buffer.getBuffer(), 0, buffer.getLength());
            //logger.debug("received "+ buffer.getLength() + " bytes from server echo: "  + buffer);
        }

        public void close() throws IOException {
        }
        
        public ByteArrayOutputStream getData() {
            return this.received;
        }
    }
    
	//TransferSinkThread
	protected class MetaChannelMonitor_Default implements MetaChannelMonitor{
	    private volatile boolean monitoringTerminate;
	    private final WorkerThread wt;
	    private final String threadGroupID;
	    
		private final static String NOSUCHEXIST = "No such file or directory";
		private final static String PERMISSIONDENY = "Permission denied";
	    protected MetaChannelMonitor_Default(){
	        this.wt = null;
	        this.threadGroupID = null;
	    }
	    public MetaChannelMonitor_Default(SocketBox socketBox,
				      BasicServerControlChannel localControlChannel)  throws IOException, ServerException {
	        this.monitoringTerminate = false;
	    	this.threadGroupID = "MetaChannelMonitor" + "@" + socketBox;
	    	this.wt = DLSThreadsManager.Resources.reserveSingle(threadGroupID, this);
			this.wt.setPriority(Thread.MIN_PRIORITY);
			this.wt.start();
	    }
	    
	    public void run() {
	        monitoringTerminate = false;
	        Reply reply = null;
	        boolean isActiveCmds = false;
	        DLSMetaCmdTask activeCmdTask;
	        try{
		        while(!monitoringTerminate) {
		        	isActiveCmds = false;
		        	activeCmdTask = null;
		        	do{
						try {
							reply = read();
						} catch (ServerException e) {
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
                                    }finally{
                                        spinlock.writeLock().unlock();
                                    }
                                    break;
                                }
                            }
						    break;
							//throw new Exception(e.toString());
						} catch (FTPReplyParseException e) {
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
                                    }finally{
                                        spinlock.writeLock().unlock();
                                    }
                                    break;
                                }
                            }						    
						    break;
							//throw new Exception(e.toString());
						} catch (IOException e) {
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
                                    }finally{
                                        spinlock.writeLock().unlock();
                                    }
                                    break;
                                }
                            }						    
						    break;
							//throw new Exception(e.toString());
						}
			            if (reply == null) {
			            	break;//how why?
			            }
			            /**
			             * skip 226 message
			             */
			            if(Reply.isPositiveCompletion(reply)){
			            	if(226 == reply.getCode()){
			            		//System.out.println(" !!!!  skip "+ reply);
			            		isActiveCmds = true;
			            		continue;
			            	}
			            }
			            /**
			             * skip intermediate reply
			             */
			            if(Reply.isPositiveIntermediate(reply)){
			            	if(150 == reply.getCode()){
			            		//System.out.println(" !!!!  skip "+ reply);
			            	    isActiveCmds = true;
			            		continue;
			            	}
			            }
			            
			            if(Reply.isTransientNegativeCompletion(reply)){
			            	//421 Idle Timeout: closing control connection.
			            	if(421 == reply.getCode()){
			            	    synchronized(wakupQueue){
			            	        monitoringTerminate = true;
			            	    }
			            		break;
			            	}
			            }

			            if(Reply.isPermanentNegativeCompletion(reply)){
			    			int existed = reply.toString().indexOf(PERMISSIONDENY);
			    			if(-1 == existed){
			    				existed = reply.toString().indexOf(NOSUCHEXIST);
			    			}
			    			//fatal problem
			    			if(-1 == existed){
			    			    /**
			    			     * fatal problem happened,
			    			     * CC is broken. The victim thread will revive this cc  
			    			     */
			    			    isActiveCmds = false;
			    				channelTerminate(reply);
			    			}else{
			    			    /**
			    			     * this case could be: permission deny, wrong listing path
			    			     * So, set metaTask.isException is true.
			    			     */
			    				activeCmdTask = wakupQueue.peek();
					            isActiveCmds = activeCmdTask.wakeUp(reply, true);
					            activeCmdTask.execute();
					            activeCmdTask.getListingTask().getClient().channelstate = CHANNEL_STATE.DC_IGNORE;
			    			}
			            }else{
				            activeCmdTask = wakupQueue.peek();
				            isActiveCmds = activeCmdTask.wakeUp(reply, false);
				            activeCmdTask.execute();
			            }
			            
		        	}while(isActiveCmds);
		        	if(true == monitoringTerminate){
		        	    /**
		        	     * channel is broken
		        	     * loop wakupQueue, set exception flag to each task
		        	     */
		        	    channelTerminate(reply);
		        	}else{
		        		if(wakupQueue.size() > 0){
			        		activeCmdTask = wakupQueue.poll();
				        	if(activeCmdTask.isComplete){
				        		logger.debug("<<-----CMDS: " + activeCmdTask +" removed~! ----->>CMD END\n");	
				        	}else if(activeCmdTask.isException){
				        		logger.debug("<<-----CMDS: " + activeCmdTask +" got exception, need retransmit~! ----->>CMD END\n");
				        	}
		        		}
		        	}	        	
		        }
	        }catch (Exception ex){
	        	ex.printStackTrace();
	        }
	    }
	    
	    /**
	     * 1st, must sync with "getAvailableStream", to info this stream is unavailable
	     * 2nd, tell sendrecv monitoringTerminate is true.
	     * 3rd, poll the current metatask, and give it revive exception
	     * 4th, poll all the rest of queuing metatasks, and give every1 retransmit exception
	     * monitor thread will naturally exit.
	     * @param reply
	     */
	   public void channelTerminate(Reply reply) {
            while(true){
                if(spinlock.writeLock().tryLock()){
                	if(true == monitoringTerminate){
                	    spinlock.writeLock().unlock();
                	    return;
                	}
                    //2nd.
                	monitoringTerminate = true;
                    try{
                        DLSMetaCmdTask task = wakupQueue.peek();
                        DLSClient dummyclient = task.getListingTask().getClient();
                        //1st.
                        DLSStream stream = dummyclient.getStream(); 
                        stream.isAvailable = false;
                        stream.value = 0;
                        //3rd.
                        dummyclient.channelstate = CHANNEL_STATE.CC_REVIVE;
                        System.out.println("Task: " + task + " caused cc: " + dummyclient.getStream().streamID + " broken to revive!" + " cc stat: " + dummyclient.channelstate + " client: " + dummyclient);
                        task.wakeUp(reply, true);
                        wakupQueue.poll();
                        //report # stream's control channel is broken.
                        //4th.
                        boolean keepLooping = false;
                        while(null != (task = wakupQueue.peek()) ){
                            do{
                                task.getListingTask().getClient().channelstate = CHANNEL_STATE.DC_RETRANSMIT;
                                System.out.println("Task: " + task + " caused cc: " + dummyclient.getStream().streamID + " broken to migrate!" + " cc stat: " + dummyclient.channelstate + " client: " + dummyclient);
                                keepLooping = task.wakeUp(reply, true);
                            }while(keepLooping);
                            wakupQueue.poll();
                        }
                    }finally{
                        spinlock.writeLock().unlock();
                    }
                    break;
                }
             }
        }
	    
	    public Reply read()
	            throws ServerException, IOException, FTPReplyParseException {
	            Reply reply = new Reply(ftpIn);
	            //DLSLogTime curtime = new DLSLogTime();
	            //System.out.println("time: "+ curtime.toString() + " . Control channel read reply: " + reply + " from ftpIn: " + ftpIn);
	            //logger.debug("Control channel read reply: " + reply + " from ftpIn: " + ftpIn);
	            return reply;
	    }

        public boolean isTermintate() {
            return this.monitoringTerminate;
        }
	}

	public int getCapacity() {
		return capacity.get();
	}

	public int reserveCapacity() {
		int i = capacity.getAndIncrement();
		return i;
	}
	
	public void releaseCapacity(){
	    capacity.decrementAndGet();
	}
}
