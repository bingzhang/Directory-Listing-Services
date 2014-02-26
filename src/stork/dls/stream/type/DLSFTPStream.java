package stork.dls.stream.type;

import java.io.IOException;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;

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
public class DLSFTPStream extends DLSStream{
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSFTPStream.class.getName());
	
	public static final int DEFAULT_FTP_PORT = 21;
	private boolean authorized = false;
	
	public DLSFTPStream(String streamKey) {
		super(streamKey);
	}
	
	protected DLSFTPStream(int id, String streamKey) {
		super(id, streamKey);
	}

	/*
	@Override
	protected void setInfo(String host, int port, String username, 
			String password, String protocol, String proxyCertContent, String token){
		proxyclient.setHP(host, port);
		super.setInfo(host, port, username, password, protocol, proxyCertContent, token);
	}*/
	
	
	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new DLSFTPStream(i, streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}

	@Override
	protected String DefaultPort() {
		port = DLSFTPStream.DEFAULT_FTP_PORT;
		realprotocol ="ftp";
		return realprotocol;
	}

	@Override
	protected synchronized void  Authenticate(DLSListingTask listingtask, String assignedThreadName, String path,
			String token) throws Exception {
        DLSClient authclient = new DLSClient(host, port, this);
	    
		while(true){
		    if(spinlock.writeLock().tryLock()){
		        try{
	                try{
	            		if (null == username || username.isEmpty())
	            			username = "anonymous";
	            		if (null == password)
	            			password = "";
	                    this.localCC = authclient.authenticate(listingtask, assignedThreadName, username, password);
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
	protected void finalize() throws Throwable {
		closeCC();
		super.finalize();
		if(DEBUG_PRINT){
			logger.debug("StreamGFTP.finalize ~!");
		}
	}

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
}
