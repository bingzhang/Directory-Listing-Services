package stork.dls.stream.type;

import java.io.IOException;
import java.util.Vector;
import org.globus.ftp.FileInfo;
import org.globus.ftp.exception.FTPReplyParseException;

import stork.dls.client.FTPClientEx;
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
	private FTPClientEx ftpc = null;	
	
	public FTPStream(String streamkey) {
		super(false, streamkey);
	}
	final protected String DefaultPort(){
		port = DEFAULT_FTP_PORT;
		realprotocol = "ftp";
		return realprotocol;
	}
		
	final protected synchronized void Authenticate(String assignedThreadName, String path, String token) throws Exception {
		if (null == username || username.isEmpty())
			username = "anonymous";
		if (null == password)
			password = "";
		ftpc = new FTPClientEx(host, port);
		try{
			ftpc.authorize(username, password);
		}catch (Exception ex){
			//ex.printStackTrace();
			throw new Exception(ex);
		}
	}

	final protected Vector<FileInfo> listCMD (String assignedThreadName, String root_path)throws Exception{
		Vector<FileInfo> fileList = null;
		boolean exceptionFlag = false;
		try{
			synchronized (ftpc) {
				ftpc.setPassiveMode(true);
				fileList = (Vector<FileInfo>)ftpc.list(root_path, null);
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
	@Override
	protected void finalize() throws Throwable {
		ftpc.close();
		ftpc = null;
		super.finalize();
	}

	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new FTPStream(streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}
}
