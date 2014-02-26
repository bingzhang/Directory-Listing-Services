package stork.dls.stream.type;

import java.util.Vector;

import org.globus.ftp.FileInfo;

import stork.dls.client.DLSClient;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;
import stork.dls.util.XMLString;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;
/**
 * Wrapper of everything, like a tool class.<br>
 * Passive mode 
 * @author bing
 * @see DLSStream
 */
public class SFTPStream extends DLSStream{
	public SFTPStream(String streamkey) {
		super(streamkey);
	}public SFTPStream(int id, String streamkey) {
	    super(id, streamkey);
	}

	private static final int DEFAULT_SFTP_PORT = 22;
	
	private JSch jsch = null;
	private Session Stream;
	private Channel channel;
	private ChannelSftp sftp;

	final protected String DefaultPort(){
		port = DEFAULT_SFTP_PORT;
		return "sftp";
	}	
	
	final protected synchronized void Authenticate(final DLSListingTask listingtask, 
	        String assignedThreadName, String path, String token) throws Exception {
		jsch = new JSch();
		
		UserInfo ui = new MyUserInfo(password);

		Stream = jsch.getSession(username, host, port);
		Stream.setUserInfo(ui);
		Stream.connect();
		channel = Stream.openChannel("sftp");
		channel.connect();
		sftp = (ChannelSftp) channel;
		System.out.println(assignedThreadName + path +"establish a new sftp Stream~!");
	}

	final protected Vector<FileInfo> listCMD (String assignedThreadName, String root_path) throws Exception{
		Vector fileList = null;
		boolean exceptionFlag = false;
		try{
			synchronized (jsch) {
				fileList = sftp.ls(root_path);
			}
		} catch (SftpException e) {
			exceptionFlag = true;
			if (e.id == 3) {
				//e.printStackTrace();
				fileList = null;
				throw e;
				//xr.reportError(XMLReply.PermissionError);
			} else {
				//e.printStackTrace();
				throw e;
				//xr.reportError(XMLReply.ResourceError);
			}
		}catch (Exception e) {
			exceptionFlag = true;
			e.printStackTrace();
			throw e;
			//xr.reportError(e.toString());
		}finally{
			
		}
		if(fileList.isEmpty()){
			throw new Exception("FPT protocol: A system call failed: No such file or directory");
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
		sftp.disconnect();
		Stream.disconnect();
		super.finalize();
	}
	private static class MyUserInfo implements UserInfo {

		String password;

		public MyUserInfo(String string) {
			password = string;
		}

		public String getPassphrase() {
			return null;
		}

		public String getPassword() {
			return password;
		}


		public boolean promptPassphrase(String arg0) {
			return true;
		}

		public boolean promptPassword(String arg0) {
			return true;
		}

		public boolean promptYesNo(String arg0) {
			return true;
		}

		public void showMessage(String arg0) {

		}
	}
	
	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new SFTPStream(streamkey);
			dls_StreamPool.streamList.add(i, e);
		}
		
	}

}
