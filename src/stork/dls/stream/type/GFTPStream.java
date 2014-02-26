package stork.dls.stream.type;

import java.io.IOException;
import java.util.Vector;
import org.globus.ftp.*;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;

import stork.dls.client.DLSClient;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSProxyCred;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;


final public class GFTPStream extends DLSStream{
	public GFTPStream(String streamkey) {
		super(streamkey);
	}public GFTPStream(int id, String streamkey) {
	    super(id, streamkey);
	}
	public static final int DEFAULT_GFTP_PORT = 2811;
	private GridFTPClient gfc = null;
	GSSCredential cred = null;
	
	final protected String DefaultPort(){
		port = DEFAULT_GFTP_PORT;
		realprotocol = "gsiftp";
		return realprotocol;
	}
	
	private static class NodelayGFTPClient extends GridFTPClient {
		public NodelayGFTPClient(String host, int port) throws Exception {
			super(host, port);
			//super.session.waitDelay = 5;
		}
	}
	
	final protected synchronized void Authenticate(DLSListingTask listingtask, 
	        String assignedThreadName, String path, String token) throws Exception {
		token = null;
		cred = DLSProxyCred.getCred(this);
		gfc = new NodelayGFTPClient(host, port);
		try{
		gfc.authenticate(cred, username);
		}catch (Exception ex){
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		gfc.setDataChannelAuthentication(DataChannelAuthentication.NONE);
		if(DEBUG_PRINT){
			System.out.println(assignedThreadName + " "+path +" establish a new gsiftp Stream~!");
		}
	}

	final protected Vector<FileInfo> listCMD (String assignedThreadName, String root_path) throws Exception{
		boolean exceptionFlag = false;
		Vector<FileInfo> fileList = null;
		try{
			synchronized (gfc) {
				gfc.setPassiveMode(true);
				fileList = (Vector<FileInfo>) gfc.list(root_path, "-r");
			}
		}catch (ServerException se) {
			//maybe it is the wrong input path
			//Indicates that operation failed because of conditions on the server, independent from the client. 
			//For instance, the server did not understand command, or could not read file. 
			//Note that here "server" can mean either a remote server, or the local internal server (FTPServerFacade). 
			fileList = null;
			//System.out.println("threadID( "+assignedThreadName + " ) met with ServerException to list path: " +listingFullPath+"~!");
			exceptionFlag = true;
			throw se;
			//se.printStackTrace();
		}catch (ClientException ce){
			//Indicates a local client side problem that has not been caused by remote server nor the local data channel.
			fileList = null;
			//System.out.println("threadID( "+assignedThreadName + " ) met with ClientException to list path: " +listingFullPath+"~!");
			//ce.printStackTrace();
			exceptionFlag = true;
			throw ce;
		}catch (IOException ie){
			//Stream is broken/
			//Signals that an I/O exception of some sort has occurred. 
			//This class is the general class of exceptions produced by failed or interrupted I/O operations. 
			fileList = null;
			//System.out.println("threadID( "+assignedThreadName + " ) met with IOException to list path: " +listingFullPath+"~!");
			//ie.printStackTrace();
			exceptionFlag = true;
			throw ie;
		}catch (Exception e) {
			//e.printStackTrace();
			//xr.reportError(e.toString());
			exceptionFlag = true;
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
	@Override
	protected void finalize() throws Throwable {
		gfc.close();
		super.finalize();
		if(DEBUG_PRINT){
			System.out.println("StreamGFTP.finalize ~!");
		}
	}

	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new GFTPStream(i, streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}

}
