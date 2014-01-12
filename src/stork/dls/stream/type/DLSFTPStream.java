package stork.dls.stream.type;

import java.io.IOException;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;

import stork.dls.client.DLSClient;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;
import stork.dls.util.DLSLog;

/**
 * Wrapper of everything, like a tool class.<br>
 * Passive mode 
 * @author bing
 * @see DLSStream
 */
public class DLSFTPStream extends DLSStream{
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSFTPStream.class.getName());
	
	public static final int DEFAULT_FTP_PORT = 21;
	private boolean authorized = false;
	
	public DLSFTPStream(String streamKey) {
		super(true, streamKey);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}
	
	protected DLSFTPStream(boolean TwoD, String streamKey) {
		super(true, streamKey);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}

	@Override
	public void close() {
		super.dlsclient = null;
	}
	/*
	@Override
	protected void setInfo(String host, int port, String username, 
			String password, String protocol, String proxyCertContent, String token){
		dlsclient.setHP(host, port);
		super.setInfo(host, port, username, password, protocol, proxyCertContent, token);
	}*/
	
	@Override
	protected DLSClient createClient(){
		DLSClient client = new DLSClient(host, port);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
		return client;
	}
	
	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			int j = 0;
			for(;j <DLSStreamPool.ONE_PIPE_CAPACITY; j ++){
				DLSStream e = null;
				e = new DLSFTPStream(streamkey);
				dls_StreamPool.streamList.add(e);
			}
		}
	}

	@Override
	protected String DefaultPort() {
		port = DLSFTPStream.DEFAULT_FTP_PORT;
		realprotocol ="ftp";
		return realprotocol;
	}

	@Override
	protected synchronized void  Authenticate(String assignedThreadName, String path,
			String token) throws Exception {
		if(FAILURE_STATUS.FAILURE_RECOVERABLE == stream_failstatus){
			if(null != dlsclient){
				dlsclient.close();
			}
			dlsclient = new DLSClient(super.host, super.port);
		}
		
		if (null == username || username.isEmpty())
			username = "anonymous";
		if (null == password)
			password = "";
		
		dlsclient.authenticate(assignedThreadName, username, password);
        this.authorized = true;
        dlsclient.isAvailable = true;
		if(DEBUG_PRINT){
			System.out.println(assignedThreadName + " "+path +" establish a new dlsftp Stream~!");
		}
	}

	public void setPassiveMode(boolean passiveMode)
	        throws IOException, ClientException, ServerException {return;}

	@Override
	protected void finalize() throws Throwable {
		dlsclient.close();
		super.finalize();
		if(DEBUG_PRINT){
			logger.debug("StreamGFTP.finalize ~!");
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
