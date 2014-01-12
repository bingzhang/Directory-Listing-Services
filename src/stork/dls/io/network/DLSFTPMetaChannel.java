package stork.dls.io.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

import org.globus.ftp.HostPort;
import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.SimpleSocketBox;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import org.globus.net.SocketFactory;

import stork.dls.client.DLSClient;
import stork.dls.util.DLSLog;

/**
 * meta-commands channel connecting to remote server
 * @author bing
 * @see MetaChannel
 */
public class DLSFTPMetaChannel {
	public static boolean DEBUG_PRINT;
    private static DLSLog logger =
    		DLSLog.getLog(DLSFTPMetaChannel.class.getName());
	private MetaChannel localmetachannel = null;
    public final static int ONE_PIPE_CAPACITY = MetaChannel.ONE_PIPE_CAPACITY;
	protected HostPort remoteServerAddress;
	protected SocketBox socketBox;
    protected Socket socket;
    protected BufferedReader ftpIn;
    protected InputStream rawFtpIn;
    protected OutputStream ftpOut;
    public String host;
    public final int port;
    protected boolean hasBeenOpened = false;
    private boolean ipv6 = false;
    private final DLSClient destroy;
    
    
    public DLSFTPMetaChannel(DLSClient tobeDestroied, String host, int port) {
    	this.destroy = tobeDestroied;
        this.host = host;
        this.port = port;
        this.ipv6 = (this.host.indexOf(':') != -1);
    }
    
    public BufferedReader get_ftpIn(){
    	return this.ftpIn;
    }
    
    public Socket getSocket(){
    	return this.socket;
    }
    
    public OutputStream get_ftpOut(){
    	return this.ftpOut;
    }
    
    public InputStream get_rawFtpIn(){
    	return this.rawFtpIn;
    }
    
    public boolean isIPv6() {return this.ipv6;}
    
    protected boolean haveStreams() {
        return (ftpIn != null && ftpOut != null);
    }
    
    /**
     * opens the connection and returns after it is ready for communication.
     * Before returning, it intercepts the initial server reply(-ies),
     * and not positive, throws UnexpectedReplyCodeException.
     * After returning, there should be no more queued replies on the line.
     *
     * Here's the sequence for connection establishment (rfc959):
     * <PRE>
     *     120
     *         220
     *     220
     *     421
     *</PRE>
     * @throws Exception 
     **/
    public void open() throws Exception {
        if (hasBeenOpened) {
            throw new IOException("Attempt to open an already opened connection");
        }
        InetAddress                     allIPs[];
        //depending on constructor used, we may already have streams
        if (!haveStreams()) {
            int                         timeout = 30000;
            int                         i = 0;
            boolean                     firstPass = true;
            
            String toS = System.getProperty("org.globus.ftp.openTO");
            if(toS != null){
                try{
                    timeout = Integer.parseInt(toS);
                }
                catch(NumberFormatException ex){
                    throw new NumberFormatException("Invalid value for property "
                        + "org.globus.ftp.openTO (" + toS + "). Must be numeric.");
                }
            }
            else{
                timeout = 0;
                firstPass = false;
            }
            allIPs = InetAddress.getAllByName(host);
            while(true){
                try{
                    logger.debug("opening control channel to " 
                        + allIPs[i] + " : " + port);
                    InetSocketAddress isa = new InetSocketAddress(allIPs[i], port);
                    this.socket = new Socket();
                    this.socket.setSoTimeout(0);
                    this.socket.connect(isa, timeout);
                    break;
                }
                catch(IOException ioEx){
                    logger.debug("failed connecting to  " 
                        + allIPs[i] + " : " + port +":"+ioEx);
                    i++;
                    if(i == allIPs.length){
                        if(firstPass){
                            firstPass = false;
                            i = 0;
                            timeout = 0; // next time let system time it out
                        }
                        else{
                            throw ioEx;
                        }
                    }
                }
            }
            String pv = System.getProperty("org.globus.ftp.IPNAME");
            if(pv != null){
                host = socket.getInetAddress().getHostAddress();
            }
            else{
                host = socket.getInetAddress().getCanonicalHostName();
            }
            setInputStream(socket.getInputStream());
            setOutputStream(socket.getOutputStream());
        }
        readInitialReplies();
        hasBeenOpened = true;
        this.remoteServerAddress = new HostPort(socket.getInetAddress(), port);
        try {
			socketBox = DLSFTPMetaChannel.openSocket(this.remoteServerAddress);
		} catch (Exception e) {
			e.printStackTrace();
			socketBox = null;
			throw e;
		}
    }
	
    public void setInputStream(InputStream in) {
        rawFtpIn = in;
        ftpIn = new BufferedReader(new InputStreamReader(rawFtpIn));
    }

    public void setOutputStream(OutputStream out) {
        ftpOut = out;
    }
    
    public static SocketBox openSocket(HostPort hp) throws Exception {
    	if(null == hp) return null;
        SocketBox sBox = new SimpleSocketBox();
        SocketFactory factory = SocketFactory.getDefault();
        Socket mySocket = factory.createSocket(hp.getHost(), hp.getPort());
        sBox.setSocket(mySocket);
        return sBox;
    }
    
    /**
     * intercepts the initial replies 
     * that the server sends after opening control ch.
     * @throws IOException
     * @throws ServerException
     */
    protected void readInitialReplies() throws IOException, ServerException {
        Reply reply = null;
        try {
            reply = new Reply(ftpIn);
            logger.debug("got reply: " + reply.toString() + " from ftpIn: " + ftpIn);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                rpe,
                                "Received faulty initial reply");
        }

        if (Reply.isPositivePreliminary(reply)) {
            try {
                reply = new Reply(ftpIn);
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(
                                        rpe,
                                        "Received faulty second reply");
            }
        }

        if (!Reply.isPositiveCompletion(reply)) {
            close();
            throw ServerException.embedUnexpectedReplyCodeException(
                                new UnexpectedReplyCodeException(reply),
                                "Server refused connection.");
        }
    }
    
    /**
     * Closes the control channel connected to remote 21 port
     */
    public void close() throws IOException {
        logger.debug("ftp socket closed");
        if (ftpIn != null)
            ftpIn.close();
        if (ftpOut != null)
            ftpOut.close();
        if (socket != null)
            socket.close();
        hasBeenOpened = false;
        localmetachannel = null;
    }
    
    /**
     * Sends the command over the control channel.
     * wait for reply.
     * @param cmd FTP command
     * @throws Exception 
     */
    public List<Reply> exchange(final String assignedThread, List<Command> cmds, List<ReplyParser> replyParserChain, List<LocalReply> writeBacks)
    		 throws IOException, ServerException{
    	List<Reply> replies = null;
        if(null == localmetachannel){
        	localmetachannel = new MetaChannel(this.destroy, this.remoteServerAddress, socketBox, ftpIn, ftpOut);
        	//localmetachannel = new MetaChannel_Simple(this.remoteServerAddress, socketBox, ftpIn, ftpOut);
        }
    	replies = localmetachannel.sendrecv(assignedThread, cmds, replyParserChain, writeBacks);
        return Collections.unmodifiableList(replies);
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
