package stork.dls.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import org.globus.common.ChainedIOException;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.FileInfo;
import org.globus.ftp.GridFTPSession;
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
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.extended.GridFTPInputStream;
import org.globus.ftp.extended.GridFTPOutputStream;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.util.Base64;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import stork.dls.config.DLSConfig;
import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.io.network.DLSSimpleTransferReader;
import stork.dls.io.network.ReplyParser;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.util.DLSLog;

/**
 * gsi authentication FTP client
 * 
 * @author bing
 * @see
 * 		DLSClient
 */
public class DLSGSIFTPClient extends DLSClient{
	public static boolean DEBUG_PRINT;
	public static boolean CONTROLCHANNEL_LISTING = DLSConfig.CC_LISTING;//false
	
	private static DLSLog logger = DLSLog.getLog(DLSGSIFTPClient.class.getName());

	protected String username = null;
    protected GSSCredential credentials = null;
    protected Authorization authorization = HostAuthorization.getInstance();
    protected int protection = GridFTPSession.PROTECTION_PRIVATE;
	
	private Socket socket;
    protected InputStream rawFtpIn;
    protected OutputStream ftpOut;
    protected BufferedReader ftpIn;
    public String DCAU;

	public DLSGSIFTPClient(String host, int port, DLSStream input){
		super(host, port, input);
	}
	
    private String getHandshakeToken(byte [] token) 
	throws IOException {
    	final byte[] header = "ADAT ".getBytes();
    	final byte[] body = Base64.encode(token);
    	final byte[] CRLF = "\r\n".getBytes();
    	String s1 = new String(header);
    	String s2 = new String(body);
    	String s3 = new String(CRLF);    	
    	String result = s1+s2+s3;
    	return result;
    }
	
	private Command generateTokenCmd(GSSCredential credential, GSSContext context, byte [] inToken)
			throws IOException,	ServerException{
		Command tokenCmd = null;
        byte [] outToken = null;
        try {
        	outToken = context.initSecContext(inToken, 0, inToken.length);
            String tokenstr = getHandshakeToken(outToken);
            tokenCmd = new Command(tokenstr);
        }catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        }
		
		return tokenCmd;
	}
	
	private GSSName getExpectedName(DLSFTPMetaChannel localCC, 
	        GSSCredential credential)  throws Exception {
		GSSName expectedName = null;
		try {
        	socket = localCC.getSocket();
            String host = this.socket.getInetAddress().getHostAddress();

            if (this.authorization instanceof GSSAuthorization) {
                GSSAuthorization auth = (GSSAuthorization)this.authorization;
                expectedName = auth.getExpectedName(credential, host);
            }
		}catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        }
		return expectedName;
		
	}
	
	private GSSContext getContext(GSSName expectedName, GSSCredential credential) throws Exception{
		GSSContext context = null;
		GSSManager manager = ExtendedGSSManager.getInstance();
		try {
		context = manager.createContext(expectedName,
                GSSConstants.MECH_OID,
                credential,
                GSSContext.DEFAULT_LIFETIME);
		context.requestCredDeleg(true);
		context.requestConf(this.protection == 
		    GridFTPSession.PROTECTION_PRIVATE);
		}catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        }
		return context;
	}
	
	private void sendTokenCmd(final GSSCredential credential, final GSSContext context, byte [] inToken) 
			throws IOException,	ServerException{
		Command tokenCmd = generateTokenCmd(credential, context, inToken);
		List<Command> cmds = new ArrayList<Command>();
		cmds.add(tokenCmd);
    	List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
		ReplyParser firstReplyParser = new ReplyParser(){
			public void replyParser(Reply adatReply) throws IOException,
					ServerException {
				logger.debug("received TOKEN cmd reply: " + adatReply);
				if( !context.isEstablished() ) {
					byte[] intoken = adatReply.toString().getBytes();
					sendTokenCmd(credential, context, intoken);
				}
			}

			public String getCurrentCmd() {
				return "Token CMD";
			}
		};
		replyParserChain.add(firstReplyParser);
	}
	
    /**
     * Performs authentication with specified user credentials and
     * a specific username (assuming the user dn maps to the passed username).
     * with data channel authentication OFF
     * 
     * @param credential user credentials to use.
     * @param username specific username to authenticate as.
     * @throws IOException on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
	@Override
    public DLSFTPMetaChannel authenticate(
            final DLSListingTask listingtask, 
            final String threadAssignedName, 
            final GSSCredential credential,
            String username)
        throws Exception {
	    final DLSFTPMetaChannel localCC = new DLSFTPMetaChannel(this.stream, host, port);
    	localCC.open();
    	socket   = localCC.getSocket();
    	ftpIn    = localCC.get_ftpIn();
    	ftpOut   = localCC.get_ftpOut();
    	rawFtpIn = localCC.get_rawFtpIn();
    	
        setCredentials( credential );
        write(new Command("AUTH", "GSSAPI"));
        Reply reply0 = null;
        try {
            reply0 = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                      rpe,
                                      "Received faulty reply to AUTH GSSAPI");
        }
        if (! Reply.isPositiveIntermediate(reply0)) {
           stream.closeCC();
           throw ServerException.embedUnexpectedReplyCodeException(
                                  new UnexpectedReplyCodeException(reply0),   
                                  "Server refused GSSAPI authentication.");
        }
        GSSContext context = null;
        GridFTPOutputStream gssout = null;
        GridFTPInputStream gssin = null;

        try {
            GSSName expectedName = getExpectedName(localCC, credential);
            context = getContext(expectedName, credential);
            gssout = new GridFTPOutputStream(ftpOut, context);
            gssin = new GridFTPInputStream(rawFtpIn, context);
            byte [] inToken = new byte[0];
            byte [] outToken = null;
            while( !context.isEstablished() ) {
                outToken = context.initSecContext(inToken, 0, inToken.length);
                if (outToken != null) {
                    gssout.writeHandshakeToken(outToken);
                }
                if (!context.isEstablished()) {
                    inToken = gssin.readHandshakeToken();
                }
            }

        } catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        }

        if (this.authorization != null) {
            try {
                this.authorization.authorize(context, host);
            } catch (AuthorizationException e) {

                throw new ChainedIOException("Authorization failed", e);
            }
        }

        // this should be authentication success msg (plain)
        // 234 (ok, no further data required)
        Reply reply1 = null;
        try {
            reply1 = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                      rpe,
                                      "Received faulty reply to authentication");

        }
        
        if ( ! Reply.isPositiveCompletion(reply1)) {
            stream.closeCC();
            throw ServerException.embedUnexpectedReplyCodeException(
                                    new UnexpectedReplyCodeException(reply1),
                                    "GSSAPI authentication failed.");
        }
        logger.debug("DLSGSIFTP authentication successful~!");
        // enter secure mode - send MIC commands
        localCC.setInputStream(gssin);
        localCC.setOutputStream(gssout);
        //from now on, the commands and replies
        //are protected and pass through gsi wrapped socket
        login(listingtask, localCC, threadAssignedName, username, null);
        return localCC;
    }
	/**
	 * DCAU is NONE
	 */
	protected void login(DLSListingTask listingtask, final DLSFTPMetaChannel localCC,final String threadAssignedName,
	        String username, String password) throws IOException, ServerException{
    	final String subject = threadAssignedName;
    	final Command userCmd = new Command("USER", (username == null) ? ":globus-mapping:" : username);
		final Command passCmd = new Command("PASS", "dummy");
		DataChannelAuthentication type = DataChannelAuthentication.NONE;
		final Command dcauCmd = new Command("DCAU", (type).toFtpCmdArgument());
		List<Command> cmds = new ArrayList<Command>();
		cmds.add(userCmd);cmds.add(passCmd);cmds.add(dcauCmd);
		List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
		
		ReplyParser firstReplyParser = new ReplyParser(){
			public void replyParser(Reply userReply) throws IOException,
					ServerException {
				logger.debug("received USER cmd reply: " + userReply);
				if (Reply.isPositiveIntermediate(userReply)) {
				}else if (Reply.isPositiveCompletion(userReply)){
				}else{
					stream.closeCC();
			           throw ServerException.embedUnexpectedReplyCodeException(
                               new UnexpectedReplyCodeException(userReply),
                               "User authorization failed.");
				}
			}

			public String getCurrentCmd() {
				return subject+ " "+userCmd.toString().trim();
			}
		};
		replyParserChain.add(firstReplyParser);
		final String[] realUserName = new String[2]; 
		ReplyParser secondReplyParser = new ReplyParser(){
			public void replyParser(Reply passReply) throws IOException,
					ServerException {
				logger.debug("received PASS cmd reply: " + passReply);
				if (!Reply.isPositiveCompletion(passReply)) {
				    stream.closeCC();
	                throw ServerException.embedUnexpectedReplyCodeException(
	                                   new UnexpectedReplyCodeException(passReply),
	                                   "Bad password.");
	            }
				String str = passReply.toString();
				Pattern p = Pattern.compile("[ ]+");
				String[] result = p.split(str);
				realUserName[0] = result[2];
			}

			public String getCurrentCmd() {
				return subject+ " "+passCmd.toString().trim();
			}
		};
		replyParserChain.add(secondReplyParser);
		ReplyParser thirdReplyParser = new ReplyParser(){
			public void replyParser(Reply dcauReply) throws IOException,
					ServerException {
				logger.debug("received DCAU N cmd reply: " + dcauReply);
				if (!Reply.isPositiveCompletion(dcauReply)) {
				    stream.closeCC();
	                throw ServerException.embedUnexpectedReplyCodeException(
	                                   new UnexpectedReplyCodeException(dcauReply),
	                                   "DCAU N failed.");
	            }
				DCAU = "none";
			}

			public String getCurrentCmd() {
				return subject+ " "+dcauCmd.toString().trim();
			}
		};
		replyParserChain.add(thirdReplyParser);
		
		if(DLSGSIFTPClient.CONTROLCHANNEL_LISTING){
		    //final Command optsCmd = new Command("OPTS", "MLST Type*;Size*;Modify*;UNIX.mode*;UNIX.uid*;UNIX.gid*");
		    final Command optsCmd = new Command("OPTS", "MLST Type*;Size*;Modify*;UNIX.mode*");
		    cmds.add(optsCmd);
		    ReplyParser fourthReplyParser =new ReplyParser() {
	            public String getCurrentCmd() { return "OPTS"; }
	            public void replyParser(Reply optsReply) throws IOException,
	                    ServerException { 
	                logger.debug("received OPTS cmd reply: " + optsReply);
	            }
	        };
	        replyParserChain.add(fourthReplyParser);
		}
		
		localCC.exchange(listingtask, threadAssignedName, cmds, replyParserChain, null);
        logger.debug("DLSGSIFTP user: "+realUserName[0]+ " login successful~!");
	}
    
    private Reply read()
            throws ServerException, IOException, FTPReplyParseException {
            Reply reply = new Reply(ftpIn);
            logger.debug("Control channel received: " + reply);
            return reply;
        }
    
    private void write(Command cmd) throws IOException {
    	String msg = cmd.toString();
        ftpOut.write(msg.getBytes());
        ftpOut.flush();
    }
    
    protected void setCredentials( GSSCredential credentials ) {
        this.credentials = credentials;
    }
    
    protected GSSCredential getCredentials() {
        return credentials;
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}