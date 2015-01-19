package stork.dls.client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.globus.ftp.DataSink;
import org.globus.ftp.FTPClient;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.HostPort;
import org.globus.ftp.Session;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.FTPControlChannel;
import org.globus.ftp.vanilla.FTPServerFacade;
import org.globus.ftp.vanilla.Reply;

import stork.dls.stream.DLSStream;

/**
 * wrapper of globus FTP client
 * 
 * @author bing
 * @see
 * 		FTPClient
 */
public class FTPClientEx extends DLSClient{
	FTPControlChannelEx ftpcex;
	private static class FTPControlChannelEx extends FTPControlChannel {

		public FTPControlChannelEx(String host, int port) {
			super(host, port);
		}
		public Socket getSocket(){
			return super.socket;
		}
		 public Reply read() 
			throws ServerException, IOException, FTPReplyParseException{
			 Reply reply = super.read(); 
			 return reply; 
		 }
		 public void write(Command cmd)
			throws IOException, IllegalArgumentException {
			 super.write(cmd); 
		 }
	}

	public FTPClientEx(String host, int port, DLSStream input) throws ServerException, IOException{
	    super(host, port, input);
		session = new Session();
		controlChannel = ftpcex = new FTPControlChannelEx(host, port);
        controlChannel.open();
        localServer = new FTPServerFacade(controlChannel);
        localServer.authorize();
        session.waitDelay = 5;
	}
	
    public FTPClientEx(DLSStream ftpStream) {
        super(ftpStream);
    }

    public void setPassiveMode(boolean passiveMode)
            throws IOException, ClientException, ServerException {
            if (passiveMode) {
                setPassive();
                Socket socket = ftpcex.getSocket();
                session.serverAddress = new HostPort(socket.getInetAddress(), 
                		session.serverAddress.getPort());
                setLocalActive();
            } else {
                setLocalPassive();
                setActive();
            }
        }
    
    public void list(String filter, String modifier, DataSink sink)
            throws ServerException, ClientException, IOException {
        String arg = null;

        if (modifier != null) {
            arg = modifier;
        } 
        if (filter != null) {
            arg = (arg == null) ? filter : arg + " " + filter;
        }
        
        Command cmd = new Command("LIST", arg);

        performTransfer(cmd, sink);
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}