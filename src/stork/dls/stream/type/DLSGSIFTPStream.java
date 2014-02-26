package stork.dls.stream.type;

import java.io.IOException;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.ietf.jgss.GSSCredential;

import stork.dls.client.DLSClient;
import stork.dls.client.DLSGSIFTPClient;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSListingTask;
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
	
	public DLSGSIFTPStream(String streamkey){
		super(streamkey);
		//stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}public DLSGSIFTPStream(int id, String streamkey){
		super(id, streamkey);
		//stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}protected DLSGSIFTPStream(boolean TwoD, String streamkey) {
		super(streamkey);
		//stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
	}

	@Override
	public void fillStreamPool(String streamkey) {
		for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = new DLSGSIFTPStream(i, streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}

	@Override
	protected String DefaultPort() {
		port = DLSGSIFTPStream.DEFAULT_FTP_PORT;
		realprotocol = "gsiftpdls";
		//protocol = "gsiftpdls";
		return realprotocol;
	}
	/**
	 * when do Authenticate, everything related to this stream should stop
	 * So, synchronized on this stream is safely OK.
	 */
	@Override
	protected void Authenticate(DLSListingTask listingtask, String assignedThreadName, String path,
			String token) throws Exception {
		token = null;
		this.isAvailable = false;
		DLSClient authclient = new DLSGSIFTPClient(host, port, this);

		while(true){
		    if(spinlock.writeLock().tryLock()){
		        try{
	                try{
	                    if(null == this.cred){
	                        this.cred = DLSProxyCred.getCred(this);             
	                    }
	                    this.localCC = authclient.authenticate(listingtask, assignedThreadName, this.cred, username);
	                    this.authorized = true;
	                    this.isAvailable = true;
	                    this.value = ONE_PIPE_CAPACITY;
	                }catch (Exception ex){
	                    ex.printStackTrace();
	                    if(null != authclient){
	                        authclient.closeDC();
	                    }
	                    throw new Exception(ex);
	                }
	                if(DEBUG_PRINT){
	                    System.out.println(assignedThreadName + " "+path +" establish a new dlsgsiftp stream~!");
	                }
		        }finally{
		            spinlock.writeLock().unlock();
		        }
		        break;
		    }		        
		}
	}
	
	public void setPassiveMode(boolean passiveMode)
	        throws IOException, ClientException, ServerException {return;}

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
}