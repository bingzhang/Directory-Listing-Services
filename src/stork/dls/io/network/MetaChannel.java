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
import org.globus.ftp.Buffer;
import org.globus.ftp.DataSink;
import org.globus.ftp.HostPort;
import org.globus.ftp.Session;
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
import stork.dls.util.DLSLog;

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
	private final Session session;
	
	private HostPort hp = null;
	private SocketBox socketBox = null;
	private final DataChannelFactory dataChannelFactory;
	
    private final BufferedReader ftpIn;
    private final OutputStream ftpOut;
	
	public final static int ONE_PIPE_CAPACITY = DLSConfig.DLS_PIPE_CAPACITY;//configurable
	
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
	private final MetaChannelMonitor monitor;
	private final LinkedBlockingQueue<DLSMetaCmdTask> wakupQueue;

	public MetaChannel(DLSClient tobeDestroied, HostPort hp, SocketBox socketBox, 
			BufferedReader ftpIn, OutputStream ftpOut)  throws IOException, ServerException{
	        logger.debug("hostport: " + hp.getHost() + " " + hp.getPort());
	    this.hp = hp;
	    this.socketBox = socketBox;
	    this.ftpIn = ftpIn;
	    this.ftpOut = ftpOut;
		this.session = new Session();
	    session.serverMode = Session.SERVER_ACTIVE;
		this.dataChannelFactory = new SimpleDataChannelFactory();
		wakupQueue = new LinkedBlockingQueue<DLSMetaCmdTask>();
		monitor = new MetaChannelMonitor(tobeDestroied, socketBox, session, null);
		monitor.monitoringTerminate = false;
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
		if(this.monitor.monitoringTerminate){
			String exceptionSubject = "needs to retransmit again~!";
			throw new IOException(exceptionSubject);
		}
		wakupQueue.offer(metaTask);
		List<Command> cmds = metaTask.getCmds();
		Iterator<Command> it = cmds.iterator();
		while(it.hasNext()) {
			Command cmd = it.next();
			write(cmd);
			logger.debug(metaTask.getAssignedThreadName() + " send "+ cmd.toString() + " to remote port 21");
		}
	}
	private void writeCmd(DLSMetaCmdTask metaTask){
	}
	//asynchronous method
	public List<Reply> sendrecv(final String assignedThreadName, List<Command> cmds, List<ReplyParser> replyParserChain, List<LocalReply> writeBacks) 
			throws
				ServerException,
			    IOException{
		if(this.monitor.monitoringTerminate){
			String exceptionSubject = "needs to retransmit again~!";
			throw new IOException(exceptionSubject);
		}
		DLSMetaCmdTask metaTask = new DLSMetaCmdTask(assignedThreadName, cmds, replyParserChain, writeBacks);
		writeCmdGreedy(metaTask);
		Reply reply = null;
		List<Reply> replies = new ArrayList<Reply>();
		Iterator<ReplyParser> replyParser = metaTask.getReplayParserChain().iterator();
		while(null != (reply = metaTask.waitOn())){
			if(this.monitor.monitoringTerminate || metaTask.isException){
				String exceptionSubject = metaTask.toString();
				exceptionSubject = exceptionSubject + " needs to retransmit again~!";
				throw new IOException(exceptionSubject);
			}
			boolean doublecheck = replyParser.hasNext();
			if(false == doublecheck){
				System.out.println("how n why?");
				throw new IOException("how n why?");
			}
			ReplyParser oneReplyParser = replyParser.next();
			oneReplyParser.replyParser(reply);
			logger.debug(oneReplyParser.getCurrentCmd() + " Recv: {\n"+ reply.toString() + "\nfrom remote port 21}\n");
			replies.add(reply);
		}
		
		return Collections.unmodifiableList(replies);
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
	private class MetaChannelMonitor implements Runnable{
		private final DLSClient destroy;
	    private final SocketBox socketBox;
	    private volatile boolean monitoringTerminate;
	    private final WorkerThread wt;
	    private final Session session;
	    private final String threadGroupID;
	    
	    public MetaChannelMonitor(DLSClient tobeDestroied, SocketBox socketBox, Session session,
				      BasicServerControlChannel localControlChannel)  throws IOException, ServerException {
	    	this.destroy = tobeDestroied;
	    	this.socketBox = socketBox;
	    	this.session = session;
	    	this.threadGroupID = "MetaChannelMonitor" + "@" + session;
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
							throw new Exception(e.toString());
						} catch (FTPReplyParseException e) {
							//e.printStackTrace();
							throw new Exception(e.toString());
						} catch (IOException e) {
							//e.printStackTrace();
							throw new Exception(e.toString());
						}
			            if (reply == null) {
			            	break;//how why?
			            }
			            /**
			             * skip 226 message
			             */
			            if(Reply.isPositiveCompletion(reply)){
			            	if(226 == reply.getCode()){
			            		//System.out.println("skip "+ reply);
			            		isActiveCmds = true;
			            		continue;
			            	}
			            }
			            /**
			             * skip intermediate reply
			             */
			            if(Reply.isPositiveIntermediate(reply)){
			            	if(150 == reply.getCode()){
			            		continue;
			            	}
			            }
			            
			            if(Reply.isTransientNegativeCompletion(reply)){
			            	//421 Idle Timeout: closing control connection.
			            	if(421 == reply.getCode()){
			            		monitoringTerminate = true;
			            		break;
			            	}
			            }
			            
			            if(Reply.isPermanentNegativeCompletion(reply)){
				            activeCmdTask = wakupQueue.peek();
				            isActiveCmds = activeCmdTask.wakeUp(reply, true);
			            }else{
				            activeCmdTask = wakupQueue.peek();
				            isActiveCmds = activeCmdTask.wakeUp(reply, false);
				            activeCmdTask.execute();
			            }
		        	}while(isActiveCmds);
		        	if(true == monitoringTerminate){
		        		//loop wakupQueue, set exception flag to each task
		        		synchronized(this.destroy){
		        			DLSMetaCmdTask cursor = null;
		        			boolean keepLooping = false;
		        			while(null != (cursor = wakupQueue.peek())){
		        				do{
		        					keepLooping = activeCmdTask.wakeUp(reply, true);
		        				}while(keepLooping);
		        				wakupQueue.poll();
		        			}
		        			//
		        			destroy.close();
		        		}
		        	}else{
			        	activeCmdTask = wakupQueue.poll();
			        	if(activeCmdTask.isComplete){
			        		logger.debug("<<-----CMDS: " + activeCmdTask +" finished~! ----->>CMD END\n");	
			        	}else if(activeCmdTask.isException){
			        		logger.debug("<<-----CMDS: " + activeCmdTask +" got exception, need retransmit~! ----->>CMD END\n");
			        	}
		        	}
		        	
		        }
	        }catch (Exception ex){
	        	ex.printStackTrace();
	        }
	    }
	    
	    private void checkNull(DLSMetaCmdTask metaTask){
            if(null == metaTask){
            	System.out.println();
            }
            return ;
	    }
	    
	    private Reply read()
	            throws ServerException, IOException, FTPReplyParseException {
	            Reply reply = new Reply(ftpIn);
	            //logger.debug("Control channel got reply: " + reply + " from ftpIn: " + ftpIn);
	            return reply;
	    }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
