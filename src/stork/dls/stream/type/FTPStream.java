package stork.dls.stream.type;

import java.io.IOException;
import java.util.Vector;
import org.globus.ftp.FileInfo;
import org.globus.ftp.exception.FTPReplyParseException;

import stork.dls.client.DLSClient;
import stork.dls.client.FTPClientEx;
import stork.dls.client.iRODSJargonClient;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;
import stork.dls.util.XMLString;
/**
 * Wrapper of everything, like a tool class.<br>
 * Passive mode 
 * @author bing
 * @see DLSStream
 */
public class FTPStream extends DLSStream{
	public static final int DEFAULT_FTP_PORT = 21;
	private FTPClientEx authclient = null;	
	
	public FTPStream(String streamkey) {
		super(streamkey);
	}public FTPStream(int id, String streamKey) {
	    super(id, streamKey);
    }
	
    final protected String DefaultPort(){
		port = DEFAULT_FTP_PORT;
		realprotocol = "ftp";
		return realprotocol;
	}
	
   @Override
    protected DLSClient createClient(){
       return this.authclient;
        //return new FTPClientEx(this);
    }
   
	final protected synchronized void Authenticate(final DLSListingTask listingytask, 
	        String assignedThreadName, String path, String token) throws Exception {
	    //FTPClientEx authclient = null; 
		if (null == username || username.isEmpty())
			username = "anonymous";
		if (null == password)
			password = "";
		authclient = new FTPClientEx(host, port, this);
		
		/*
		try{
		    authclient.authorize(username, password);
			this.value = ONE_PIPE_CAPACITY;
		}catch (Exception ex){
			//ex.printStackTrace();
			throw new Exception(ex);
		}*/
		
	      while(true){
	            if(spinlock.writeLock().tryLock()){
	                try{
	                    try{
	                        if (null == username || username.isEmpty())
	                            username = "anonymous";
	                        if (null == password)
	                            password = "";
	                        authclient.authorize(username, password);
	                        this.isAvailable = true;
	                        this.value = ONE_PIPE_CAPACITY;
	                    }catch (Exception ex){
	                        ex.printStackTrace();
	                        if(null != authclient){
	                            //authclient.closeDC();
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
	
	
	//final protected Vector<FileInfo> listCMD (String assignedThreadName, String root_path)throws Exception
	final protected Vector<FileInfo> listCMD (DLSListingTask listingtask, String assignedThreadName, String listingFullPath)throws Exception{
		Vector<FileInfo> fileList = null;
		boolean exceptionFlag = false;
		DLSClient proxyclient = listingtask.getClient();
		try{
			synchronized (proxyclient) {
			    proxyclient.setPassiveMode(true);
				fileList = (Vector<FileInfo>)proxyclient.list(listingFullPath, null);
			}
		}catch (IOException ie){
			exceptionFlag = true;
			fileList = null;
			throw ie;
		}catch (Exception e) {
			exceptionFlag = true;
			fileList = null;
			throw e;
		}finally{

		}
		/*
		XMLString xml_str = new XMLString();
		String ret = null;
		ret = xml_str.getXMLString(fileList, root_path);
		
		if(null != fileList){
			fileList.clear();
			fileList = null;
		}
		return ret;
		*/
		return fileList;
	}
	@Override
	public void close(){
		try {
			finalize();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	/*
	@Override
	protected void finalize() throws Throwable {
	    authclient.close();
	    authclient = null;
		super.finalize();
	}*/

	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new FTPStream(i, streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}

}
