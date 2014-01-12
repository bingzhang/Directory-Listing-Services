package stork.dls.io.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.globus.ftp.HostPort;
import org.globus.ftp.Session;
import org.globus.ftp.dc.DataChannelFactory;
import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.SimpleDataChannelFactory;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import stork.dls.util.DLSLog;

/**
 * One meta channel is owned by one stream, which can and only can be accessed at most 
 * by one WorkerThread at the same time.
 * Write cmds in pipeline style.</b><br>
 * 
 * <b>what is the MetaChannel_Simple?</b><br>
 * MetaChannel_Simple acts as a storage of dls network stream (like the buffer for network metadata cmd transferring operations) 
 * in queue FIFO order
 * 
 * <br><br>
 * MetaChannel_Simple is DLS network I/O scheduler/monitor for one pipestream.<br>
 * MetaChannel_Simple never die Except explictlly invoking its close() method.
 * <br><br>
 * MetaChannel use no extra thread to read from remote port
 * <br><br>
 * MetaChannel_Simple takes ProtocolControlChannel object as input parameter, 
 * and when there is exception over the network I/O, MetaChannel_Simple will simply terminate 
 * that protocolControlChannel object, re-create a new one, continue its work.
 * <br><br>
 * Inside MetaChannel_Simple, there is scheduler/monitor thread.
 * @author bing
 * 
 * @see
 * 		DLSMetaCmdTask
 */
public class MetaChannel_Simple {
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(MetaChannel_Simple.class.getName());
	private final Session session;
	private HostPort hp = null;
	private SocketBox socketBox = null;
	private final DataChannelFactory dataChannelFactory;
    private final BufferedReader ftpIn;
    private final OutputStream ftpOut;
	private boolean isException = true;
	private boolean isComplete = false;
	public final static int ONE_PIPE_CAPACITY = 1;
	
	private DLSMetaCmdTask wakupQueue = null;
	/**
	 * wake-up queue <b>WorkerThread</b>
	 * The queue used for holding tasks and
	 * notify the wait WorkerThread
	 */
	/**
	 * Each submitted list cmd will create one (pipeTask) object in wakupQueue in the sequence of submission.
	 * <br><b>Infinite capacity</b>: because if there is failure,
	 * we need to re-send the whoe pipeTask objects in wakeupQueue.
	 * Or, let each to throw exception.<br><b>
	 * 
	 * Now, we just throw exception back to the upper component.
	 * 
	 */
	public MetaChannel_Simple(HostPort hp, SocketBox socketBox, 
			BufferedReader ftpIn, OutputStream ftpOut)  throws IOException, ServerException{
	        logger.debug("hostport: " + hp.getHost() + " " + hp.getPort());
	    this.hp = hp;
	    this.socketBox = socketBox;
	    this.ftpIn = ftpIn;
	    this.ftpOut = ftpOut;
		this.session = new Session();
	    session.serverMode = Session.SERVER_ACTIVE;
		this.dataChannelFactory = new SimpleDataChannelFactory();
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
    
    /**
     * write all the cmds into meta channel continuously 1 by 1.
     * @param metaTask
     * @throws ServerException
     * @throws IOException
     */
	private void writeCmdGreedy(DLSMetaCmdTask metaTask) 
			throws 
				ServerException, IOException{
		wakupQueue = metaTask;
		List<Command> cmds = metaTask.getCmds();
		Iterator<Command> it = cmds.iterator();
		while(it.hasNext()) {
			Command cmd = it.next();
			write(cmd);
			logger.debug(metaTask.getAssignedThreadName() + " send "+ cmd.toString() + " to remote port 21");
		}
	}
	
	/**
	 * read 1 meaningful message from meta channel<br>
	 * skip 150, 226 code
	 * @return
	 * @throws ServerException
	 * @throws IOException
	 * @throws FTPReplyParseException
	 */
    private Reply readReply()
            throws ServerException, IOException, FTPReplyParseException {
	    Reply reply = null;
	    do{
	        reply = new Reply(ftpIn);
	        if (null == reply) {
	        	throw new IOException("server refuse");
	        }
	        /**
	         * skip 226 message
	         */
	        if(Reply.isPositiveCompletion(reply)){
	        	if(226 == reply.getCode()){
	        		continue;
	        	}
	        	//logger.debug("Control channel got reply: " + reply + " from ftpIn: " + ftpIn);
	        	return reply;
	        }
	        /**
	         * skip intermediate reply
	         */
	        if(Reply.isPositiveIntermediate(reply)){
	        	System.out.println(reply.getCode());
	        	System.out.println(reply);
	        	if(150 == reply.getCode()){
	        		continue;
	        	}
	        	return reply;
	        }
	        
	        if(Reply.isPermanentNegativeCompletion(reply)){
	        	this.isException = true;
	        	this.isComplete  = false;
	        	throw new IOException("server refuse");
	        }
	    }while(true);
    }

	public synchronized List<Reply> sendrecv(final String assignedThreadName, List<Command> cmds, List<ReplyParser> replyParserChain, List<LocalReply> writeBacks) 
			throws
				ServerException,
			    IOException{
		DLSMetaCmdTask metaTask = new DLSMetaCmdTask(assignedThreadName, cmds, replyParserChain, writeBacks);
		writeCmdGreedy(metaTask);
		Reply reply = null;
		List<Reply> replies = new ArrayList<Reply>();
		Iterator<ReplyParser> replyParser = metaTask.getReplayParserChain().iterator();
		Iterator<LocalReply> localReply_itr = null;
		if(null != writeBacks){
			localReply_itr = writeBacks.iterator();
		}
		try {
			Reply remotereply = null;
			LocalReply localreply = null;
			while(replyParser.hasNext()){
				if(null == localreply){
					remotereply = readReply();
					//System.out.println(assignedThreadName + " " + metaTask +" got reply: " + remotereply);
					reply = remotereply;
				}else{
					reply = localreply;
				}
				ReplyParser oneReplyParser = null;
				try{
					oneReplyParser = replyParser.next();
					oneReplyParser.replyParser(reply);
					logger.debug(oneReplyParser.getCurrentCmd() + " Recv: {\n"+ reply.toString() + "\nfrom remote port 21}\n");
				}catch (Exception ex){
					System.out.println(oneReplyParser.getCurrentCmd() + " " + metaTask +" parsing cmd throw exception");
					throw ex;
				}
				replies.add(reply);
				if(null != writeBacks){
					localreply = localReply_itr.next(); 
				}
			}
			isComplete = true;
			isException = false;
		}catch(Exception ex){
			throw new IOException(ex.toString());
		}finally{
        	if(isException){
        		logger.debug("<<-----CMDS: " + metaTask +" got exception, need retransmit~! ----->>CMD END\n");
        	}
        	else if(isComplete){
        		logger.debug("<<-----CMDS: " + metaTask +" finished~! ----->>CMD END\n");	
        	}
		}
		return Collections.unmodifiableList(replies);
	}
	
	public void close() throws IOException{
    	Socket socket = socketBox.getSocket();
        if (ftpIn != null)
            ftpIn.close();
        if (ftpOut != null)
            ftpOut.close();
        if (socket != null)
            socket.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
