package stork.dls.io.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.globus.ftp.HostPort;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.BasicServerControlChannel;
import org.globus.ftp.vanilla.Reply;

import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.service.prefetch.WorkerThread;
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
public class MetaChannel_State extends MetaChannel{
	public static boolean DEBUG_PRINT = false;
	private static DLSLog logger = DLSLog.getLog(MetaChannel_State.class.getName());
	
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
	@Override
	protected void bindMonitor() throws IOException, ServerException{
        if(null == this.monitor){
            monitor = new MetaChannelMonitor_State(socketBox, null);
        }
    }
	
	public MetaChannel_State(DLSStream stream, HostPort hp, SocketBox socketBox, 
			BufferedReader ftpIn, OutputStream ftpOut)  throws IOException, ServerException{
	    super(stream, hp, socketBox, ftpIn, ftpOut);
	}
	@Override
	protected List<Reply> handleReply(DLSMetaCmdTask metaTask)
            throws
                ServerException,
                IOException{	
	       Reply reply = null;
	       List<Reply> replies = new ArrayList<Reply>();
	       Iterator<ReplyParser> replyParser = metaTask.getReplayParserChain().iterator();
	       ReplyParser oneReplyParser = null;
	       logger.debug("[handleReply]: " + metaTask.toString() + "before while waiton~");
	       while(null != (reply = metaTask.waitOn())){
	           if(this.monitor.isTermintate() || metaTask.isException){
	               String exceptionSubject = metaTask.toString();
	               exceptionSubject = exceptionSubject + reply;
	               throw new IOException(exceptionSubject);
	           }
	           
	           switch(reply.getCategory()){
	           case Reply.POSITIVE_PRELIMINARY:
	           {
	        	   if(null == oneReplyParser){
	        		   oneReplyParser = replyParser.next();	   
	        	   }else{}
	        	   break;
	           }
	           case Reply.POSITIVE_COMPLETION:
	           {
	        	   if(null == oneReplyParser){
	        		   oneReplyParser = replyParser.next();	   
	        	   }else{}
	        	   break;
	           }
	           default:
	        	   oneReplyParser = replyParser.next();
	           }
	           
	           if(DEBUG_PRINT){
    	           String shortreply = reply.toString();
    	           /*
    	           if(shortreply.length() > 40){
    	               shortreply = shortreply.substring(0, 40);
    	           }*/
    	           //logger.debug("["+ oneReplyParser.getCurrentCmd()+ "->" + shortreply+ "]");
	           }	           
	           /*
	           boolean doublecheck = replyParser.hasNext();
	           if(false == doublecheck){
	               System.out.println("how n why?");
	               throw new IOException("how n why?");
	           }
	           */
	           
	           oneReplyParser.replyParser(reply);
               DLSLogTime curtime = new DLSLogTime();
               logger.debug(oneReplyParser.getCurrentCmd() + " Recv: {\n"+ reply.toString());
               logger.debug(oneReplyParser.getCurrentCmd() + ". time: " +curtime.toString() +" Recved ");
	           replies.add(reply);
	       }
	       logger.debug("[handleReply]: " + metaTask.toString() + "after while waiton~");
	       //this.zombietask.remove(metaTask);
	       return replies;
	}
	
	//TransferSinkThread
	final protected class MetaChannelMonitor_State extends MetaChannelMonitor_Default{
	    private volatile boolean monitoringTerminate;
	    private final WorkerThread wt;
	    private final String threadGroupID;
	    
		private final static String NOSUCHEXIST = "No such file or directory";
		private final static String PERMISSIONDENY = "Permission denied";
	    
	    public MetaChannelMonitor_State(SocketBox socketBox,
				      BasicServerControlChannel localControlChannel)  throws IOException, ServerException {
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
							reply = null;
							reply = read();
						} catch (ServerException e) {
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
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
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
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
							//e.printStackTrace();
                            while(true){
                                if(spinlock.writeLock().tryLock()){
                                    try{
                                        monitoringTerminate = true;
                                        channelTerminate(reply, e);
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
			            
			            switch (reply.getCategory()){ 
			            case Reply.POSITIVE_INTERMEDIATE:
	                         {
	                            if(150 == reply.getCode()){
	                                System.out.println("cc segment transfer complete~!");
	                                isActiveCmds = true;
	                                continue;
	                            }
	                        }
			                break;
			            case Reply.POSITIVE_COMPLETION:
    			            {
    			                if(250 == reply.getCode()){
    			                    System.out.println("cc transfer complete~!");
    			                }
    			            }
			                break;		                    
			            }
			            
			            /**
			             * skip 226 message
			             */
			            if(Reply.isPositiveCompletion(reply)){
			                //code 226 is self-defined.
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
			            	        channelTerminate(reply, new Exception("isTransientNegativeCompletion"));
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
			    				channelTerminate(reply, new Exception("isPermanentNegativeCompletion"));
			    			}else{
			    			    /**
			    			     * this case could be: permission deny, wrong listing path
			    			     * So, set metaTask.isException is true.
			    			     */
			    				
			    				activeCmdTask = wakupQueue.peek();
			    				if(null != activeCmdTask){
						            isActiveCmds = activeCmdTask.wakeUp(reply, true);
						            activeCmdTask.execute();
						            activeCmdTask.getListingTask().getClient().channelstate = CHANNEL_STATE.DC_IGNORE;
						            System.out.println("MetaChannel exception:" + reply + " ;Set to DC_IGNORE:" + activeCmdTask.getCmds());
			    				}else{
			    					System.err.println("MetaChannel exception:" + reply + " ;But task is null");
			    				}
			    			}
			            } else if(Reply.isPositivePreliminary(reply)){
			                activeCmdTask = wakupQueue.peek();
			                System.out.println(reply);
			                isActiveCmds = activeCmdTask.wakeUpInplace(reply, false);
			            }
			            else{
				            activeCmdTask = wakupQueue.peek();
				            if(null == activeCmdTask) break;
				            isActiveCmds = activeCmdTask.wakeUp(reply, false);
				            activeCmdTask.execute();
			            }
			            
		        	}while(isActiveCmds);
		        	if(true == monitoringTerminate){
		        	    /**
		        	     * channel is broken
		        	     * loop wakupQueue, set exception flag to each task
		        	     */
		        	    channelTerminate(reply, new Exception("may duplicate Exception"));
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
	}
}
