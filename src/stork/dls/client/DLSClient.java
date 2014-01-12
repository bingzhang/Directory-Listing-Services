package stork.dls.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.globus.ftp.FileInfo;
import org.globus.ftp.HostPort;
import org.globus.ftp.HostPort6;
import org.globus.ftp.Session;
import org.globus.ftp.dc.AbstractDataChannel;
import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.SimpleDataChannel;
import org.globus.ftp.dc.SimpleTransferContext;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.dc.TransferContext;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import org.ietf.jgss.GSSCredential;

import stork.dls.ad.Ad;
import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.io.network.DLSSimpleTransferReader;
import stork.dls.io.network.ReplyParser;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStream.FAILURE_STATUS;
import stork.dls.util.DLSLog;

/**
 * the base class of FTP client
 * @see
 * 		DLSFTPMetaChannel
 */
public class DLSClient implements FTPMetaCmdFun{
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSClient.class.getName());
	public DLSStream.FAILURE_STATUS FailureStatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	public volatile boolean isAvailable = false;
	protected String clientID = null; 
	protected int port;
	protected String host;
	public final static int ONE_PIPE_CAPACITY = DLSFTPMetaChannel.ONE_PIPE_CAPACITY;

	protected DLSFTPMetaChannel localmetachannel;
	
	public DLSClient(){
	}
	
	public void printHP(){
		System.out.println("host = " + this.host + "; port = " + this.port);
	}
	
	public void setHP(String host, int port){
		this.host = host;
		this.port = port;
	}
	public DLSClient(String host, int port){
		this.host = host;
		this.port = port;
	}
	/**
	 * authentication with proxy
	 * @param threadAssignedName
	 * @param credential
	 * @param username
	 * @throws Exception
	 */
    public void authenticate(final String threadAssignedName, final GSSCredential credential,
            String username) throws Exception {
    }
	/**
	 * authentication without proxy
	 * @param threadAssignedName
	 * @param username
	 * @param password
	 * @throws Exception
	 */
	public void authenticate(final String threadAssignedName, String username, String password) throws Exception {
		localmetachannel = new DLSFTPMetaChannel(this, host, port);
		localmetachannel.open();
		login(threadAssignedName, username, password);
	}
	
	protected void login(final String threadAssignedName, String username, String password) throws IOException, ServerException{
		final String subject = threadAssignedName;
		final Command userCmd = new Command("USER", username);
		final Command passCmd = new Command("PASS", password);
		List<Command> cmds = new ArrayList<Command>();
		cmds.add(userCmd);cmds.add(passCmd);
		List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
		
		ReplyParser firstReplyParser = new ReplyParser(){
			public void replyParser(Reply userReply) throws IOException,
					ServerException {
				logger.debug("received USER cmd reply: " + userReply);
				if (Reply.isPositiveIntermediate(userReply)) {
				}else if (Reply.isPositiveCompletion(userReply)){
				}else{
					throw ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(userReply),
                            "Bad user.");
				}
			}

			public String getCurrentCmd() {
				return subject+ " "+userCmd.toString().trim();
			}
		};
		replyParserChain.add(firstReplyParser);
		ReplyParser secondReplyParser = new ReplyParser(){
			public void replyParser(Reply passReply) throws IOException,
					ServerException {
				logger.debug("received PASS cmd reply: " + passReply);
				if (!Reply.isPositiveCompletion(passReply)) {
	                throw ServerException.embedUnexpectedReplyCodeException(
	                                   new UnexpectedReplyCodeException(passReply),
	                                   "Bad password.");
	            }
			}

			public String getCurrentCmd() {
				return subject+ " "+passCmd.toString().trim();
			}
		};
		replyParserChain.add(secondReplyParser);
		localmetachannel.exchange(threadAssignedName, cmds, replyParserChain, null);
	}
	
    private Vector<FileInfo> readDataChannel(HostPort datahp, String pasvFailure, String listingFullPath) throws IOException{
		final SocketBox socketBox;
		Vector<FileInfo> result = null;
		if(null == datahp){
			throw new IOException(pasvFailure);
		}
		int port      = datahp.getPort();
		String data_host = datahp.getHost();
	
		logger.debug(listingFullPath + " open data channel: " + data_host + " port = " + port);
		try {
			socketBox = DLSFTPMetaChannel.openSocket(datahp);
		} catch (Exception ex) {
			//ex.printStackTrace();
			logger.debug(listingFullPath + " open data channel: " + data_host + " port = " + port + " failed~!");
			throw new IOException(ex);
		}
		Session session = new Session();
		TransferContext context = SimpleTransferContext.getDefault();
	
		try {
			AbstractDataChannel datachannel = new SimpleDataChannel(session, socketBox);
			DLSSimpleTransferReader transferReader = new DLSSimpleTransferReader(datachannel, socketBox, context);
			result = transferReader.read(); 
		} catch (Exception ex) {
			//ex.printStackTrace();
			logger.debug(listingFullPath + " read data channel: " + data_host + " port = " + port + " failed~!");
			throw new IOException(ex);
		}
		closesocket(socketBox);
		logger.debug(listingFullPath + " close socket = " + socketBox.toString());
		return result;
	}
	
    public Vector<FileInfo> list(final String threadAssignedName, final String listingFullPath) throws ServerException, ClientException, IOException {
    	final String subject = threadAssignedName;
    	final Vector<FileInfo> result[] = new Vector[1];
    	final HostPort datahp[] = new HostPort[1];
    	final Command pasvCmd = Command.PASV;
    	final Command lsitcmd = new Command("LIST", listingFullPath);
    	List<Command> cmds = new ArrayList<Command>();
    	cmds.add(pasvCmd); cmds.add(lsitcmd);
    	final String[] pasvFailure = new String[1]; 
    	List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
    	ReplyParser firstReplyParser = new ReplyParser(){
	    	public void replyParser(Reply pasvReply) 
	    			throws IOException, ServerException {
			    	logger.debug(listingFullPath + " received PASV cmd reply: " + pasvReply);
			    	HostPort hp = null;
			    	if(Reply.isPermanentNegativeCompletion(pasvReply)){
				    	pasvFailure[0] = pasvReply.toString();
			    	}else{
			    	       String pasvReplyMsg = null;
			    	       pasvReplyMsg = pasvReply.getMessage();
			    	       int openBracket = pasvReplyMsg.indexOf("(");
			    	       int closeBracket = pasvReplyMsg.indexOf(")", openBracket);
			    	       String bracketContent =
			    	           pasvReplyMsg.substring(openBracket + 1, closeBracket);
			    	       if (localmetachannel.isIPv6()) {
			    	           hp = new HostPort6(bracketContent);
			    	           // since host information might be null. fill it it
			    	           if (hp.getHost() == null) {
			    	               ((HostPort6)hp).setVersion(HostPort6.IPv6);
			    	               ((HostPort6)hp).setHost(localmetachannel.host);
			    	           }
			    	       } else {
			    	           hp = new HostPort(bracketContent);
			    	       }
			    	}
			    	datahp[0] = hp;
		    	}

			public String getCurrentCmd() {
				return subject+ " "+pasvCmd.toString().trim();
			}
    	};
    	replyParserChain.add(firstReplyParser);
    	ReplyParser secondReplyParser = new ReplyParser(){
    	public void replyParser(Reply listReply) 
    			throws IOException, ServerException {
		    	logger.debug(listingFullPath + " received LIST cmd reply: " + listReply);
		    	result[0] = readDataChannel(datahp[0], pasvFailure[0], listingFullPath);
		    }

		public String getCurrentCmd() {
			return subject+ " "+lsitcmd.toString().trim();
		}
    	};
    	replyParserChain.add(secondReplyParser);
    	List<LocalReply> writeBacks = new ArrayList<LocalReply>();
    	LocalReply writeback = new LocalReply(226, "LIST cmd received~!");
    	writeBacks.add(writeback);
    	writeback = null;
    	writeBacks.add(writeback);
    	localmetachannel.exchange(threadAssignedName, cmds, replyParserChain, writeBacks);
    	return result[0];
    }
    
    // close data channel
    private void closesocket(SocketBox socketBox){
    	try {
			socketBox.getSocket().getInputStream().close();
		} catch (IOException e) {
			e.printStackTrace();
		}/*
    	try {
			socketBox.getSocket().getOutputStream().close();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
    	try {
			socketBox.getSocket().close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	/**
	 * close this client
	 */
	public void close() {
		try {
			if(null == localmetachannel) return;
			localmetachannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		localmetachannel = null;
		this.FailureStatus = FAILURE_STATUS.FAILURE_RECOVERABLE;
	}
	
	/**
	 * FTP meta-cmd implementations
	 * all are asynchronous functions
	 */
	
	public void close(boolean ignoreQuitReply) throws IOException,
	ServerException {
	}

	public void abort() throws IOException, ServerException {
	}
	
	public long getSize(String filename) throws IOException, ServerException {
	return 0;
	}
	
	public Date getLastModified(String filename) throws IOException,
		ServerException {
	return null;
	}
	
	public boolean exists(String filename) throws IOException, ServerException {
	return false;
	}
	
	public void changeDir(String dir) throws IOException, ServerException {
	}
	
	public void deleteDir(String dir) throws IOException, ServerException {
	}
	
	public void deleteFile(String filename) throws IOException, ServerException {
	}
	
	public void makeDir(String dir) throws IOException, ServerException {
	}
	
	public void rename(String oldName, String newName) throws IOException,
		ServerException {
	}
	
	public String getCurrentDir() throws IOException, ServerException {
	return null;
	}
	
	public void goUpDir() throws IOException, ServerException {
	}
	
	public void login() throws IOException, ServerException {
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
