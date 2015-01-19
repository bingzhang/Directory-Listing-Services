package stork.dls.client;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.globus.ftp.FTPClient;
import org.globus.ftp.FileInfo;
import org.globus.ftp.HostPort;
import org.globus.ftp.HostPort6;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.Session;
import org.globus.ftp.dc.AbstractDataChannel;
import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.SimpleDataChannel;
import org.globus.ftp.dc.SimpleTransferContext;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.dc.TransferContext;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import org.ietf.jgss.GSSCredential;
import org.irods.jargon.core.pub.CollectionAndDataObjectListAndSearchAO;

import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.io.network.DLSSimpleTransferReader;
import stork.dls.io.network.ReplyParser;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
import stork.dls.util.DLSLog;

/**
 * the base class of FTP client each transferring through this client retains
 * the channel state
 * 
 * @see DLSFTPMetaChannel
 */
public class DLSClient extends FTPClient implements FTPMetaCmdFun{
    public static boolean DEBUG_PRINT;
    public static boolean DEBUG_PIPELINE = false;
    private static DLSLog logger = DLSLog.getLog(DLSClient.class.getName());
    public DLSStream.CHANNEL_STATE channelstate = CHANNEL_STATE.CC_BENIGN;
    protected String clientID = null;
    protected int port;
    protected String host;
    private SocketBox socketBox;
    public final static int ONE_PIPE_CAPACITY = DLSFTPMetaChannel.ONE_PIPE_CAPACITY;
    protected final DLSStream stream;

    public DLSClient(String host, int port, DLSStream input) {
        this.stream = input;
        this.host = host;
        this.port = port;
    }

    /**
     * for the pipe capacity constructor
     * 
     * @param localmetachannel
     * @param input
     */
    public DLSClient(DLSStream input) {
        this.stream = input;
    }

    public DLSStream getStream() {
        return stream;
    }

    public SocketBox getDataChannelSocket() {
        return this.socketBox;
    }

    public void printHP() {
        System.out.println("host = " + this.host + "; port = " + this.port);
    }

    public void setHP(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * authentication with proxy
     * 
     * @param threadAssignedName
     * @param credential
     * @param username
     * @throws Exception
     */
    public DLSFTPMetaChannel authenticate(final DLSListingTask listingtask, final String threadAssignedName,
            final GSSCredential credential, String username) throws Exception {
        return null;
    }

    public CollectionAndDataObjectListAndSearchAO authenticate(final String host, final int port,
			final String userName, final String password,
			final String homeDirectory, final String userZone,
			final String defaultStorageResource) throws Exception {
        return null;
    }
    
    /**
     * authentication without proxy
     * 
     * @param threadAssignedName
     * @param username
     * @param password
     * @throws Exception
     */
    public DLSFTPMetaChannel authenticate(
            final DLSListingTask listingtask, 
            final String threadAssignedName, 
            String username,
            String password) throws Exception {
        DLSFTPMetaChannel localCC = new DLSFTPMetaChannel(this.stream, host, port);
        localCC.open();
        login(listingtask, localCC, threadAssignedName, username, password);
        return localCC;
    }

    protected void login(final DLSListingTask listingtask, 
            final DLSFTPMetaChannel localCC, 
            final String threadAssignedName, 
            String username, String password) throws IOException, ServerException {
        final String subject = "login";//threadAssignedName;
        final Command userCmd = new Command("USER", username);
        final Command passCmd = new Command("PASS", password);
        List<Command> cmds = new ArrayList<Command>();
        cmds.add(userCmd);
        cmds.add(passCmd);
        List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();

        ReplyParser firstReplyParser = new ReplyParser() {
            public void replyParser(Reply userReply) throws IOException,
                    ServerException {
                logger.debug("received USER cmd reply: " + userReply);
                if (Reply.isPositiveIntermediate(userReply)) {
                } else if (Reply.isPositiveCompletion(userReply)) {
                } else {
                    throw ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(userReply),
                            "Bad user.");
                }
            }

            public String getCurrentCmd() {
                return subject + ": " + userCmd.toString().trim();
            }
        };
        replyParserChain.add(firstReplyParser);
        ReplyParser secondReplyParser = new ReplyParser() {
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
                return subject + ": " + passCmd.toString().trim();
            }
        };
        replyParserChain.add(secondReplyParser);
        localCC.exchange(listingtask, threadAssignedName, cmds, replyParserChain,
                null);
        
        
        
    }

    private Vector<FileInfo> readDataChannel(final DLSListingTask listingtask, HostPort datahp,
            String pasvFailure, String listingFullPath) throws IOException {
        // final SocketBox socketBox;
    	//System.out.println("readDataChannel" + Thread.currentThread());
        SocketBox socketBox;
        Vector<FileInfo> result = null;
        if (null == datahp) {
            if(CHANNEL_STATE.CC_BENIGN == listingtask.getClient().channelstate){
                listingtask.getClient().channelstate = CHANNEL_STATE.DC_RETRANSMIT;
            }
            this.closeDC();
            throw new IOException(pasvFailure);
        }
        int port = datahp.getPort();
        String data_host = datahp.getHost();

        try {
            socketBox = DLSFTPMetaChannel.openSocket(datahp);
            this.socketBox = socketBox;
        } catch (Exception ex) {
            // ex.printStackTrace();
            logger.debug(listingFullPath + " open data channel: " + data_host
                    + " port = " + port + " failed~!");
            if(CHANNEL_STATE.CC_BENIGN == listingtask.getClient().channelstate){
                listingtask.getClient().channelstate = CHANNEL_STATE.DC_RETRANSMIT;
            }
            this.closeDC();
            throw new IOException(ex);
        }
        logger.debug(listingFullPath + " open data channel: " + data_host
                + " port = " + port);
        Session session = new Session();
        TransferContext context = SimpleTransferContext.getDefault();

        try {
            AbstractDataChannel datachannel = new SimpleDataChannel(session,
                    socketBox);
            DLSSimpleTransferReader transferReader = new DLSSimpleTransferReader(
                    datachannel, socketBox, context);
            //System.out.println("listingFullPath begins to read!");
            socketBox.getSocket().setSoTimeout(10*1000); //10 sec
            result = transferReader.read();
        }catch (SocketTimeoutException ex){ 
            // ex.printStackTrace();
            logger.debug(listingFullPath + " read data channel: " + data_host
                    + " port = " + port + " " + ex);
            System.out.println(listingFullPath + " read data channel: " + data_host
                    + " port = " + port + " " + ex + " cc stat: " + listingtask.getClient().channelstate);
            listingtask.getClient().channelstate = CHANNEL_STATE.DC_IGNORE;
            this.closeDC();
            throw new IOException(ex);
        }
        catch (Exception ex) {
            // ex.printStackTrace();
            logger.debug(listingFullPath + " read data channel: " + data_host
                    + " port = " + port + " " + ex);
            System.out.println(listingFullPath + " read data channel: " + data_host
                    + " port = " + port + " " + ex + " cc stat: " + listingtask.getClient().channelstate);
            if(CHANNEL_STATE.CC_BENIGN == listingtask.getClient().channelstate){
                listingtask.getClient().channelstate = CHANNEL_STATE.DC_RETRANSMIT;
            }
            this.closeDC();
            throw new IOException(ex);
        }
        this.closeDC();
        logger.debug(listingFullPath + " close socket = "
                + socketBox.toString());
        return result;
    }
	public Vector<FileInfo> list(
            final DLSListingTask listingtask, 
            final CollectionAndDataObjectListAndSearchAO actualCollection, 
            final String threadAssignedName,
            final String targetIrodsCollection) throws Exception{ 
		return null;
	}
    public Vector<FileInfo> list(
            final DLSListingTask listingtask, 
            final DLSFTPMetaChannel localCC, 
            final String threadAssignedName,
            final String listingFullPath) throws Exception{ 
            //ServerException, ClientException, IOException {
        if(DLSGSIFTPClient.CONTROLCHANNEL_LISTING){
            return cclist(listingtask, localCC, threadAssignedName, listingFullPath);
        }else {
            return dclist(listingtask, localCC, threadAssignedName, listingFullPath);
        }
    }
    
    public Vector<FileInfo> cclist(
            final DLSListingTask listingtask, 
            final DLSFTPMetaChannel localCC, 
            final String threadAssignedName,
            final String listingFullPath) throws Exception{ 
            //ServerException, ClientException, IOException {
        final String protocol = listingtask.getUri().getScheme();
        final String subject = threadAssignedName;
        final Vector<FileInfo> result[] = new Vector[1];
        final HostPort datahp[] = new HostPort[1];
        /*
         * MLSC
         * STAT
         * MLST
         * LIST
         * NLST
         */
        
        Command mlsccmd = null;
        //final Command mlsccmd = new Command("MLSC", listingFullPath);
        //final Command mlsccmd = new Command("LIST", listingFullPath);
        List<Command> cmds = new ArrayList<Command>();
        List<LocalReply> writeBacks = null;

        final String[] pasvFailure = new String[1];
        result[0] = new Vector<FileInfo>();
        List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
        // Reply parser for control channel listing.
        if(protocol.toLowerCase().equals("ftpdls")){
        	mlsccmd = new Command("STAT", listingFullPath);
            ReplyParser firstReplyParser = new ReplyParser() {
                StringBuffer sb = new StringBuffer();
                public void replyParser(Reply reply) throws IOException, ServerException {
                	if(Reply.isPermanentNegativeCompletion(reply)){
                        // Listing failed.
                    } else if (Reply.isPositivePreliminary(reply)) {
                        sb.append(reply.getMessage());
                    } else {
                        // Listing succeeded.
                        sb.append(reply.getMessage());
                        for (String line : sb.toString().split("\n")) {
                        	FileInfo fileInfo = null;
                            try {
                            	line = line.trim();
                                //if (line.startsWith("status") || line.startsWith("total"))
                            	if (line.startsWith("d") || line.startsWith("-")){
	                                try {
	                                    fileInfo = new FileInfo(line);
	                                } catch (FTPException e) {
	                                    /*
	                                    int existed = e.toString().indexOf(INVALID_TOKENNUM);
	                                    if(-1 != existed){
	                                        //continue;
	                                    }*/
	                                    ClientException ce =
	                                        new ClientException(
	                                                            ClientException.UNSPECIFIED,
	                                                            "Could not create FileInfo");
	                                    ce.setRootCause(e);
	                                    throw ce;
	                                }
	                                result[0].add(fileInfo);
                            	}
                            } catch (FTPException e) {
                                e.printStackTrace();
                            }
                        }
                        if(0 == result[0].size()){
                        	DLSClient proxyclient = listingtask.getClient();
                    		proxyclient.channelstate = CHANNEL_STATE.DC_IGNORE;                		
                    		throw new IOException("No such file or directory");
                        }
                    }
                }

                public String getCurrentCmd() {
                    return subject + ": " + "list more";
                }
            };    
            replyParserChain.add(firstReplyParser);
        }else{// gridftp or gsiftp parser
        	mlsccmd = new Command("MLSC", listingFullPath);
	        ReplyParser firstReplyParser = new ReplyParser() {
	            StringBuffer sb = new StringBuffer();
	            public void replyParser(Reply reply) throws IOException, ServerException {
	                if(Reply.isPermanentNegativeCompletion(reply)){
	                    // Listing failed.
	                } else if (Reply.isPositivePreliminary(reply)) {
	                    sb.append(reply.getMessage());
	                } else {
	                    // Listing succeeded.
	                    sb.append(reply.getMessage());
	                    for (String line : sb.toString().split("\n")) {
	                        try {
	                            if (!line.startsWith(" "))
	                                continue;
	                            MlsxEntry mlsxentry = new MlsxEntry(line);
	                            FileInfo fileinfo = new FileInfo();
	                            String name = mlsxentry.getFileName();                          
	                            fileinfo.setName(name);
	                            
	                            String type = mlsxentry.get("type");
	                            if (type != null){
	                            	if(type.endsWith("dir")){
	                            		fileinfo.setFileType(FileInfo.DIRECTORY_TYPE);
	                            	}else if(type.endsWith("file")){
	                            		fileinfo.setFileType(FileInfo.FILE_TYPE);
	                            	}
	                                String factname = mlsxentry.SIZE;
	                                fileinfo.setSize(Integer.parseInt(mlsxentry.get(factname)));
	                                //final SimpleDateFormat dformat = new SimpleDateFormat("yyyyMMddHHmmss");
	                                factname = mlsxentry.MODIFY;
	                                String dateString = mlsxentry.get(factname);
	                                fileinfo.setDate(dateString);
	                                fileinfo.setTime(null);
	                                /*
	                                Date date = null;
									try {
										date = dformat.parse(dateString);
										fileinfo.setDate(date.toString());
									} catch (ParseException e) {
										e.printStackTrace();
									}*/
	                                factname = mlsxentry.PERM;
	                                String perm = mlsxentry.get(factname);
	                                if(null == perm ){
	                                	factname = mlsxentry.UnixMode;
	                                	String mode = mlsxentry.get(factname);
		                                int permission = Integer.parseInt(mode);
		                                fileinfo.setMode(permission);
		                                //System.out.println(name + " " + permission);
	                                }else{
		                                int permission = Integer.parseInt(perm);
		                                fileinfo.setMode(permission);
		                                //System.out.println(name + " " + permission);
	                                }
	                            }
	                            result[0].add(fileinfo);
	                        } catch (FTPException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                }
	            }
	            public String getCurrentCmd() {
	                return subject + ": " + "list more";
	            }
	        };
        	replyParserChain.add(firstReplyParser);
        }
        

        writeBacks = new ArrayList<LocalReply>();
        LocalReply writeback = new LocalReply(226, "finish transfer~!");
        writeBacks.add(writeback);
        cmds.add(mlsccmd);
        try{
            localCC.exchange(listingtask, threadAssignedName, cmds, replyParserChain, writeBacks);
        }catch (Exception ex){
            DLSClient proxyclient = listingtask.getClient();
            DLSStream stream = proxyclient.stream; 
            System.out.println("in list stream " + stream.streamID + " " + threadAssignedName + listingFullPath + " cc stat: " + proxyclient.channelstate + " client: " + proxyclient);
            throw ex;
        }
        return result[0];
    }    
    
    public Vector<FileInfo> dclist(
            final DLSListingTask listingtask, 
            final DLSFTPMetaChannel localCC, 
            final String threadAssignedName,
            final String listingFullPath) throws Exception{ 
            //ServerException, ClientException, IOException {
        
        final String subject = listingFullPath+";dclist";//threadAssignedName;
        final Vector<FileInfo> result[] = new Vector[1];
        final HostPort datahp[] = new HostPort[1];
        final Command pasvCmd = Command.PASV;
        final Command lsitcmd = new Command("LIST", listingFullPath);
        List<Command> cmds = new ArrayList<Command>();
        cmds.add(pasvCmd);
        cmds.add(lsitcmd);
        List<LocalReply> writeBacks = null;

        final String[] pasvFailure = new String[1];
        List<ReplyParser> replyParserChain = new ArrayList<ReplyParser>();
        ReplyParser firstReplyParser = new ReplyParser() {
            public void replyParser(Reply pasvReply) throws IOException,
                    ServerException {
                logger.debug(listingFullPath + " received PASV cmd reply: "
                        + pasvReply);
                HostPort hp = null;
                if (Reply.isPermanentNegativeCompletion(pasvReply)) {
                    pasvFailure[0] = pasvReply.toString();
                } else {
                    String pasvReplyMsg = null;
                    pasvReplyMsg = pasvReply.getMessage();
                    int openBracket = pasvReplyMsg.indexOf("(");
                    int closeBracket = pasvReplyMsg.indexOf(")", openBracket);
                    String bracketContent = pasvReplyMsg.substring(
                            openBracket + 1, closeBracket);
                    if (localCC.isIPv6()) {
                        hp = new HostPort6(bracketContent);
                        // since host information might be null. fill it it
                        if (hp.getHost() == null) {
                            ((HostPort6) hp).setVersion(HostPort6.IPv6);
                            ((HostPort6) hp).setHost(localCC.host);
                        }
                    } else {
                        hp = new HostPort(bracketContent);
                    }
                }
                datahp[0] = hp;
                // result[0] = readDataChannel(datahp[0], pasvFailure[0],
                // listingFullPath);//debug
            }

            public String getCurrentCmd() {
                return subject + ": " + pasvCmd.toString().trim();
            }
        };
        replyParserChain.add(firstReplyParser);
        ReplyParser secondReplyParser = new ReplyParser() {
            public void replyParser(Reply listReply) throws IOException,
                    ServerException {
                logger.debug(listingFullPath + " received LIST cmd reply: "
                        + listReply);
                result[0] = readDataChannel(listingtask, datahp[0], pasvFailure[0],
                        listingFullPath);
            }

            public String getCurrentCmd() {
                return subject + ": " + lsitcmd.toString().trim();
            }
        };
        replyParserChain.add(secondReplyParser);
        writeBacks = new ArrayList<LocalReply>();
        LocalReply writeback = new LocalReply(226, "finish transfer~!");
        writeBacks.add(writeback);

        if (DEBUG_PIPELINE) {
            final Command pasvCmd2 = Command.PASV;
            final Command lsitcmd2 = new Command("LIST", listingFullPath);
            cmds.add(pasvCmd2);
            cmds.add(lsitcmd2);
            ReplyParser firstReplyParser2 = new ReplyParser() {
                public void replyParser(Reply pasvReply) throws IOException,
                        ServerException {
                    logger.debug(listingFullPath + " received PASV cmd reply: "
                            + pasvReply);
                    HostPort hp = null;
                    if (Reply.isPermanentNegativeCompletion(pasvReply)) {
                        pasvFailure[0] = pasvReply.toString();
                    } else {
                        String pasvReplyMsg = null;
                        pasvReplyMsg = pasvReply.getMessage();
                        int openBracket = pasvReplyMsg.indexOf("(");
                        int closeBracket = pasvReplyMsg.indexOf(")",
                                openBracket);
                        String bracketContent = pasvReplyMsg.substring(
                                openBracket + 1, closeBracket);
                        if (localCC.isIPv6()) {
                            hp = new HostPort6(bracketContent);
                            // since host information might be null. fill it it
                            if (hp.getHost() == null) {
                                ((HostPort6) hp).setVersion(HostPort6.IPv6);
                                ((HostPort6) hp).setHost(localCC.host);
                            }
                        } else {
                            hp = new HostPort(bracketContent);
                        }
                    }
                    datahp[0] = hp;
                    result[0] = readDataChannel(listingtask, datahp[0], pasvFailure[0],
                            listingFullPath);
                }

                public String getCurrentCmd() {
                    return subject + ": " + pasvCmd.toString().trim();
                }
            };
            replyParserChain.add(firstReplyParser2);
        }
        
        try{
            localCC.exchange(listingtask, threadAssignedName, cmds, replyParserChain, writeBacks);
        }catch (Exception ex){
            DLSClient proxyclient = listingtask.getClient();
            DLSStream stream = proxyclient.stream;
            if(CHANNEL_STATE.CC_BENIGN == proxyclient.channelstate){
                System.out.println("dclist got excep " + stream.streamID + " " + threadAssignedName + listingFullPath + " cc stat: " + proxyclient.channelstate + " client: " + proxyclient);
                proxyclient.channelstate = CHANNEL_STATE.DC_RETRANSMIT;
                ex.printStackTrace();                
            }
            throw ex;
        }
        return result[0];
    }

    // close data channel
    public void closeDC() {
        if (null == this.socketBox)
            return;
        try {
            socketBox.getSocket().getInputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }/*
          * try { socketBox.getSocket().getOutputStream().close(); } catch
          * (IOException e) { e.printStackTrace(); }
          */
        try {
            socketBox.getSocket().close();
            this.socketBox = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * FTP meta-cmd implementations all are asynchronous functions
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
}