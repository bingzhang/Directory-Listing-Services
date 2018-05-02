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
import stork.dls.stream.DLSIOAdapter;
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
	protected DLSStream attachedStream = null;
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
	 * we need to re-send the whole pipeTask objects in wakeupQueue.
	 * Or, let each to throw exception.
	 */
	protected MetaChannelMonitor monitor;
	protected final LinkedBlockingQueue<DLSMetaCmdTask> wakupQueue;
	//protected final ConcurrentHashMap<DLSMetaCmdTask, Boolean> zombietask;

	public void pollWakupQueue(){
        if(wakupQueue.size() > 0){
            DLSMetaCmdTask activeCmdTask = wakupQueue.poll();
            //if(activeCmdTask.isComplete){
            logger.debug("<<-----CMDS: " + activeCmdTask +" complete, removed~! ----->>CMD END\n"); 
            //}
        }
	}
	
	protected MetaChannel(){
	    this.ftpIn = null;
	    this.ftpOut = null;
	    this.monitor = null;
	    this.spinlock = null;
	    this.wakupQueue = null;
	    //this.zombietask = null;
	    this.dataChannelFactory = null;
	}
	
	protected void bindMonitor() throws IOException, ServerException{
	    if(null == this.monitor){
	        monitor = new MetaChannelMonitor_Default(socketBox, null);
	    }
	}
	
	public MetaChannel(DLSStream stream, HostPort hp, SocketBox socketBox, 
			BufferedReader ftpIn, OutputStream ftpOut)  throws IOException, ServerException{
	    //logger.debug("hostport: " + hp.getHost() + " " + hp.getPort());
	    this.attachedStream = stream;
        spinlock = stream.getspinlock();
	    this.hp = hp;
	    this.socketBox = socketBox;
	    this.ftpIn = ftpIn;
	    this.ftpOut = ftpOut;
		this.dataChannelFactory = new SimpleDataChannelFactory();
		wakupQueue = new LinkedBlockingQueue<DLSMetaCmdTask>();
		//this.zombietask = new ConcurrentHashMap<DLSMetaCmdTask, Boolean>();
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
        try{
            ftpOut.write(msg.getBytes());
            ftpOut.flush();
        }catch (Exception ex){
            ex.printStackTrace();
            throw new IOException(ex.toString());
        }
    }
    
	private synchronized void writeCmdGreedy(DLSMetaCmdTask metaTask) 
			throws 
				ServerException, IOException{
        wakupQueue.offer(metaTask);
        {
            logger.debug("Synchronized write cmds: " + metaTask);
        }
		List<Command> cmds = metaTask.getCmds();
		Iterator<Command> it = cmds.iterator();
		while(it.hasNext()) {
		    Command cmd = it.next();
			write(cmd);
			DLSLogTime curtime = new DLSLogTime();
			//System.out.println("time: " + curtime.toString() + " SEND "+ cmd.toString());
			logger.debug(metaTask.getAssignedThreadName() + "time: " + curtime.toString() + " SEND "+ cmd.toString());
		}
		logger.debug("Synchronized write cmds: completed");
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
	           if(DEBUG_PRINT){
    	           String shortreply = reply.toString();
    	           if(shortreply.length() > 40){
    	               shortreply = shortreply.substring(0, 40);
    	           }
    	           logger.debug("["+ oneReplyParser.getCurrentCmd()+ "->" + shortreply+ "]");
	           }
	           oneReplyParser.replyParser(reply);
	           replies.add(reply);
	       }
	       //this.zombietask.remove(metaTask);
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
	       //logger.debug(spinlock + " going to do readLock().tryLock()");
	       if(spinlock.readLock().tryLock()){
	           //logger.debug(spinlock + " finished to do readLock().tryLock()");
	    	   //Thread activeT = Thread.currentThread();
	    	   //listingtask.setAssignedThread(activeT);
	           try{
	               if(true != this.monitor.isTermintate()){
    	               metaTask = new DLSMetaCmdTask(listingtask, assignedThreadName, cmds, replyParserChain, writeBacks, this);
    	               //synchronized write into the shared queue.
    	               Boolean yes = new Boolean(true);
    	               //this.zombietask.put(metaTask, yes);
    	               writeCmdGreedy(metaTask);
	               }else{
	                   if(this.monitor.isTobeRevived(false)){
	                       //flipping TobeRevived
	                       System.out.println("1st to observe CC broken");
	                       DLSClient dummyclient = listingtask.getClient();
	                       dummyclient.channelstate = CHANNEL_STATE.CC_REVIVE;
	                       throw new IOException();	                       
	                   }
	                   /**
	                    * already observed CC broken
	                    */
	                   System.out.println("already observed CC broken");
	                   DLSClient dummyclient = listingtask.getClient();
	                   dummyclient.channelstate = CHANNEL_STATE.DC_RETRANSMIT;
	                   throw new IOException();
	               }
	           }finally{
	               //logger.debug(spinlock + " going to do readLock().unlock()");
	               spinlock.readLock().unlock();
	               //logger.debug(spinlock + " finished to do readLock().unlock()");
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
	    //@Guardby{spinlock}
	    public volatile boolean cc_need_revive = false;
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
						    DLSLogTime time = new DLSLogTime();
						    logger.debug("MetaChannel exception: ServerException. " +time + reply);
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        //monitoringTerminate = true;
                                        isActiveCmds = false;
                                        channelTerminate(reply, e);
                                    }finally{
                                        spinlock.writeLock().unlock();
                                    }
                                    break;
                                }
                            }
						    break;
							//throw new Exception(e.toString());
						} catch (FTPReplyParseException e) {
						    DLSLogTime time = new DLSLogTime();
						    System.out.println("MetaChannel exception: FTPReplyParseException. " +time+ reply);
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        //monitoringTerminate = true;
                                        isActiveCmds = false;
                                        channelTerminate(reply, e);
                                    }finally{
                                        spinlock.writeLock().unlock();
                                    }
                                    break;
                                }
                            }						    
						    break;
							//throw new Exception(e.toString());
						} catch (IOException e) {
							e.printStackTrace();
							DLSLogTime time = new DLSLogTime();
						    logger.debug("MetaChannel exception: IOException" + time+reply);
						    //same as the timeout exception.
						    // server close this transferring after some period of time,
						    //may be longer or short time period.
                            while(true){
                                logger.debug(spinlock + " going to do writeLock().tryLock()");
                                if(spinlock.writeLock().tryLock()){
                                    logger.debug(spinlock + " finished to do writeLock().tryLock()");
                                    try{
                                        //monitoringTerminate = true;
                                        isActiveCmds = false;
                                        channelTerminate(reply, e);
                                    }finally{
                                        logger.debug(spinlock + " going to do writeLock().unlock()");
                                        spinlock.writeLock().unlock();
                                        logger.debug(spinlock + " finished to do writeLock().unlock()");
                                    }
                                    break;
                                }
                            }						    
						    break;
							//throw new Exception(e.toString());
						}
						//----------end of all catches-------------------//
                        //----------end of all catches-------------------//
			            if (reply == null) {
			                System.out.println("MetaChannel exception: reply is null");
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
			            	    System.out.println("MetaChannel exception: 421 == reply.getCode()");
                                isActiveCmds = false;
                                channelTerminate(reply, new Exception("isTransientNegativeCompletion"));
			            	    /*
			            	    synchronized(wakupQueue){
			            	        monitoringTerminate = true;
			            	    }*/
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
			    				channelTerminate(reply, new Exception("isPermanentNegativeCompletion"));
			    			}else{
			    			    /**
			    			     * this case could be: permission deny, wrong listing path
			    			     * So, set metaTask.isException is true.
			    			     */
			    			    System.out.println("MetaChannel exception:" + reply);
			    				activeCmdTask = wakupQueue.peek();
			    				if(null != activeCmdTask){
				    				activeCmdTask.getListingTask().getClient().channelstate = CHANNEL_STATE.DC_IGNORE;
						            isActiveCmds = activeCmdTask.wakeUp(reply, true);
						            //activeCmdTask.execute();
			    				}
			    			}
			            }else{
				            activeCmdTask = wakupQueue.peek();
				            if(null == activeCmdTask) break;
				            isActiveCmds = activeCmdTask.wakeUp(reply, false);
				            boolean nottrue = activeCmdTask.execute();
				            if(isActiveCmds == true){
				                isActiveCmds = !nottrue;
				            }
			            }
		        	}while(isActiveCmds);
		        	
		        	if(true == monitoringTerminate){
		        	    /**
		        	     * channel is broken
		        	     * loop wakupQueue, set exception flag to each task
		        	     */
		        	    channelTerminate(reply, new Exception("may duplicate Ex"));
		        	}
                    /*
		        	else{
		        		if(wakupQueue.size() > 0){
			        		activeCmdTask = wakupQueue.poll();
				        	if(activeCmdTask.isComplete){
				        		logger.debug("<<-----CMDS: " + activeCmdTask +" complete, removed~! ----->>CMD END\n");	
				        	}else if(activeCmdTask.isException){
				        		logger.debug("<<-----CMDS: " + activeCmdTask +" got exception, removed~! ----->>CMD END\n");
				        	}
		        		}
		        	}*/
		        	
		        }//while(!monitoringTerminate) {
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
	   public void channelTerminate(Reply reply, Exception ex) {
            while(true){
                if(spinlock.writeLock().tryLock()){
                    try{
                    	if(true == monitoringTerminate){
                    	    //spinlock.writeLock().unlock();
                    	    return;
                    	}
                    	DLSLogTime time = new DLSLogTime();
                    	int steamid = attachedStream.streamID;
                    	System.out.println("REPLY cause excep: " +ex +" ;" + time + " ;repl:"+reply + " ; steam id: "+steamid);
                        //2nd.
                        //extra check condition: timeout flag
                    	cc_need_revive = true;
                    	monitoringTerminate = true;
                        if(wakupQueue.size() > 1){
                            System.out.println("wakupQueue has problem");
                        }
                        DLSMetaCmdTask task = wakupQueue.peek();
                        /*self close, set timeout flag inside this stream, */
                        if(null == task){
                            //attachedStream.getStreamPool().MigrationStream(null, null, null, attachedStream, 0);
                            System.out.println("close(true)");
                            try {
                                attachedStream.getCC().close(true);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            //can not close cc socket, otherwise can not revive it again.
                            return;
                        }else{
	                        DLSClient dummyclient = task.getListingTask().getClient();
	                        //1st.
	                        DLSStream stream = dummyclient.getStream();
	                        stream.value = 0;
	                        stream.isAvailable = false;
	                        
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
                        }
                        /*
                        Iterator<Map.Entry<DLSMetaCmdTask, Boolean>> iterator = zombietask.entrySet().iterator();
                        while(iterator.hasNext()){
                        	Map.Entry<DLSMetaCmdTask, Boolean> entry = iterator.next();
                        	DLSMetaCmdTask zombie = entry.getKey();
                        	if(null != zombie){
                        		zombie.wakeUp(reply, true);
                        	}
                        	iterator.remove();
                        }*/
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
        public boolean isTobeRevived(boolean flip) {
            this.cc_need_revive = this.cc_need_revive^flip;
            return this.cc_need_revive;
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
