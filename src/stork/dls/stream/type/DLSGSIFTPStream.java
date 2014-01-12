package stork.dls.stream.type;

import java.io.IOException;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.ietf.jgss.GSSCredential;

import stork.dls.client.DLSClient;
import stork.dls.client.DLSGSIFTPClient;
import stork.dls.stream.DLSProxyCred;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;
import stork.dls.util.DLSLog;
/**
 * Wrapper of everything, like a tool class.<br>
 * Passive mode 
 * @author bing
 * @see DLSStream
 */
public class DLSGSIFTPStream extends DLSStream{
	protected String protocol;
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSGSIFTPStream.class.getName());
	GSSCredential cred = null;
	public static final int DEFAULT_FTP_PORT = 2811;
	private boolean authorized = false;
	
	public DLSGSIFTPStream(String streamkey, boolean flag){
		super(true, streamkey);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}
	
	public DLSGSIFTPStream(String streamkey){
		super(true, streamkey, true);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}
	protected DLSGSIFTPStream(boolean TwoD, String streamkey) {
		super(true, streamkey);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}

	@Override
	public void close() {
		super.dlsclient = null;
	}
	@Override
	protected DLSClient createClient(){
		DLSClient client = new DLSGSIFTPClient(host, port);
		stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
		return client;
	}
	@Override
	public void fillStreamPool(String streamkey) {
		for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			int j = 0;
			DLSStream e = new DLSGSIFTPStream(streamkey);
			for(;j <DLSStreamPool.ONE_PIPE_CAPACITY; j ++){
				dls_StreamPool.streamList.add(e);
			}
		}
	}

	@Override
	protected String DefaultPort() {
		port = DLSGSIFTPStream.DEFAULT_FTP_PORT;
		realprotocol = "gsiftpdls";
		//protocol = "gsiftpdls";
		return realprotocol;
	}

	@Override
	protected synchronized void Authenticate(String assignedThreadName, String path,
			String token) throws Exception {
		token = null;
		if(FAILURE_STATUS.FAILURE_RECOVERABLE == stream_failstatus){
			if(null != dlsclient){
				dlsclient.close();
			}
			dlsclient = new DLSGSIFTPClient(super.host, super.port);
		}
		try{
			if(null == this.cred){
				this.cred = DLSProxyCred.getCred(this);				
			}
			dlsclient.authenticate(assignedThreadName, this.cred, username);
			this.authorized = true;
	        dlsclient.isAvailable = true;
		}catch (Exception ex){
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		if(DEBUG_PRINT){
			System.out.println(assignedThreadName + " "+path +" establish a new dlsgsiftp stream~!");
		}
	}
	public void setPassiveMode(boolean passiveMode)
	        throws IOException, ClientException, ServerException {return;}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}