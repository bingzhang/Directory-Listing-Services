package stork.dls.stream.type;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.globus.ftp.FileInfo;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.query.CollectionAndDataObjectListingEntry;

import stork.dls.client.DLSClient;
import stork.dls.client.iRODSJargonClient;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;

public class DLSIRODSStream  extends DLSStream{
	public DLSIRODSStream(String streamkey) {
		super(streamkey);
	}protected DLSIRODSStream(int id, String streamKey) {
		super(id, streamKey);
	}

	@Override
	public void close() {
	}

	@Override
	public void fillStreamPool(String streamKey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new DLSIRODSStream(i, streamKey);
			dls_StreamPool.streamList.add(e);
		}
	}

	@Override
	protected String DefaultPort() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected DLSClient createClient(){
	    return new iRODSJargonClient(this);
	}
	@Override
	protected Vector<FileInfo> listCMD(DLSListingTask listingtask, String assignedThreadName, String listingFullPath)
			throws Exception {
	    Vector<FileInfo> fileList = null;
	    DLSClient proxyclient = listingtask.getClient();
	    if(listingFullPath.endsWith("/")){
	    	listingFullPath = listingFullPath.substring(0, listingFullPath.length()-1);
	    }
	    fileList = proxyclient.list(listingtask, this.actualCollection, assignedThreadName, listingFullPath);
	    return fileList;
	}
	
	@Override
	protected void Authenticate(DLSListingTask listingtask,
			String assignedThreadName, String path, String token)
			throws Exception {
		URI uri = listingtask.getUri();
		String hostName = uri.getHost();
		int port = uri.getPort();
		String username = listingtask.getUserName();
		String password = listingtask.getPassword();
		String homeDirectory = listingtask.getiRodsHome();
		String resource = listingtask.getResource();
		String zone = listingtask.getZone();
		DLSClient authclient = new iRODSJargonClient(hostName, port, this);
		
		while(true){
		    if(spinlock.writeLock().tryLock()){
		        try{
	                try{
	                	this.actualCollection = authclient.authenticate(hostName, port, username, password, homeDirectory,
	            				zone, resource);
	                    this.isAvailable = true;
	                    this.value = ONE_PIPE_CAPACITY;
	                }catch (Exception ex){
	                    ex.printStackTrace();
	                    if(null != authclient){
	                        authclient.closeDC();
	                    }
	                    throw new Exception(ex);
	                }
		        }finally{
		            spinlock.writeLock().unlock();
		        }
		        break;
		    }		        
		}//while(true)
	}

	
}
