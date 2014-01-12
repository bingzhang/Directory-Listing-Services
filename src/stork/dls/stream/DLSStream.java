package stork.dls.stream;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;

import javax.annotation.concurrent.GuardedBy;

import org.globus.ftp.FileInfo;

import stork.dls.client.DLSClient;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.stream.DLSIOAdapter.FETCH_PREFETCH;
import stork.dls.util.DLSResult;

/**
 * @author bing
 * @see DLSStreamPool
 */
public abstract class DLSStream{
	public static boolean DEBUG_PRINT ;//= true;//false;//true
	static final boolean PRINT_TIME = false;//true;//false;
	public static final int ONE_PIPE_CAPACITY = DLSClient.ONE_PIPE_CAPACITY;

	private final String streamKeyID;
	DLSProxyInfo proxyinfo;
	
	protected int 	 port;
	protected String host;
	protected String username;
	protected String password;
	protected String proxyCertContent;
	protected String protocol;
	protected String realprotocol;
	protected String token = null;
	protected DLSClient dlsclient;
	
	@GuardedBy("DLSStream.dls_StreamPool")
	public volatile boolean isAvailable = true;
	public enum FAILURE_STATUS{
		FAILURE_RETRANSMIT,
		FAILURE_RECOVERABLE,
		FAILURE_IGNORABLE
	};
	protected FAILURE_STATUS stream_failstatus = FAILURE_STATUS.FAILURE_RECOVERABLE;
	public int activeStreamIndx = 0;
	
	public String getStreamKey(){
		return this.streamKeyID;
	}
	
	final static class logTime{
		private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
		protected Date date = new Date();
		public String toString(){
			return df.format(date).toString();
		}
	}
	
	protected final DLSStreamPool dls_StreamPool; 
	
	boolean persistent_multi_Streams = false;//true
	
	protected DLSStream(boolean TwoD, String streamKey){
		dls_StreamPool = new DLSStreamPool(TwoD);
		this.streamKeyID = streamKey;
	}
	
	protected DLSStream(boolean TwoD, String streamKey, boolean flag){
		dls_StreamPool = null;
		this.streamKeyID = streamKey;
	}
	public String get_realprotocol(){
		return dls_StreamPool.get_realprotocol();
	}
	public int get_MAX_NUM_OPENStream(){
		return dls_StreamPool.get_MAX_NUM_OPENStream();
	}
	
	abstract public void close();
	final public void getStreamInfo(stork.dls.stream.DLSIOAdapter.StreamInfo result){
		dls_StreamPool.getStreamInfo(result);
	}
	
	final public DLSStream getAvailableStream(String threadName, String path, FETCH_PREFETCH doFetching) throws Exception{
		DLSStream activeStream = dls_StreamPool.getAvailableStream(threadName, path, doFetching);
		return activeStream;
	}
	
	final public DLSStream MigrationStream(String assignedThreadName, String path, DLSStream deadStream, int activeIndx){
		DLSStream newStream = null;
		newStream = deadStream;
		if(null != newStream){
			return dls_StreamPool.MigrationStream(assignedThreadName, path, newStream, activeIndx);
		}
		return newStream;
	}
	
	final public void releaseThisStream(DLSStream thisStream, String assignedThreadName, String path, int activeIndx){
		dls_StreamPool.releaseThisStream(thisStream, assignedThreadName, path, activeIndx);
	}
	
	abstract public void fillStreamPool(String streamKey);
	protected DLSClient createClient(){
		return null;
	}
	public void setClient(DLSClient client) {
		if(null != client && null != dlsclient){
			dlsclient.close();
		}
		dlsclient = client;
	}
	final public void initStreamPool(final URI uri, final DLSProxyInfo dlsproxy, 
			final String proxyCertContent2, 
			final String token) throws Exception{
		if(persistent_multi_Streams){
			return;
		}else{
			final String host2 = uri.getHost();
			final int port2 = uri.getPort(); 
			final String username2;
			final String password2;
			String ui = uri.getUserInfo();
			if (ui != null && !ui.isEmpty()) {
				String sa[] = ui.split(":");
				username2 = sa[0];
				if (sa.length > 1){
					password2 = sa[1];
				}else{
					password2 = null;
				}
			}else{
				username2 = null;
				password2 = null;
			}
			final String protocol2 = uri.getScheme();
			this.token = token;
			dls_StreamPool.setName(this.streamKeyID);
			fillStreamPool(this.streamKeyID);
			String realProtocol = null;
			final int STRIDE = dls_StreamPool.STRIDE;
			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
				DLSStream oDls = dls_StreamPool.streamList.get(i*STRIDE);
				oDls.protocol = protocol2;
				int port = port2;
				if (port2 == -1){
					realProtocol = oDls.DefaultPort();
					port = oDls.port;
				}				
				oDls.setInfo(host2, port, username2, password2, realProtocol, proxyCertContent2, token, dlsproxy);
			}
			int indx = 0;
			String threadGroupID = "pre-Authenticate initStreamPool";
			ArrayList<Thread> threadList = new ArrayList<Thread>(DLSStreamPool.NUM_CONCURRENCY_STREAM);
			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
				indx = i*STRIDE;
				DLSStream oDls = dls_StreamPool.streamList.get(indx);
				Runnable r = new threaded_Authenticate(oDls, indx);
				Thread t = DLSThreadsManager.Resources.reserveSingle(threadGroupID, r);
				threadList.add(t);
				t.start();
			}
			for(Thread t : threadList){
				t.join();
			}
			DLSThreadsManager.Resources.finalAll(threadGroupID);
			int init_Streams = 0;
			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i++){
				int indx1 = i*STRIDE;
				if(dls_StreamPool.activeStreamPool_status[indx1] == true){
					init_Streams ++;
				}
			}
			int sanity_check = 0;
			for(int i = 0; i < dls_StreamPool.MAX_NUM_OPENStream; i++){
				if(dls_StreamPool.activeStreamPool_status[i] == true){
					sanity_check ++;
				}
			}
			if(sanity_check != dls_StreamPool.MAX_NUM_OPENStream){
				System.out.println("initStreamPool sanity check failed~!");
			}
			if(init_Streams == 0){
				System.out.println("\nnew Stream Resource "+this.streamKeyID +" failed~! to open #Streams: " + init_Streams +" of total preassigned "+DLSStreamPool.NUM_CONCURRENCY_STREAM/*MAX_NUM_OPENStream*/ + " "+ protocol2 +" protocols~!");
				throw new Exception("remote servers deny service~!\n");
			}else{
				System.out.println("\nnew Stream Resource "+this.streamKeyID +" finish to open #Streams: " + init_Streams +" of total preassigned "+DLSStreamPool.NUM_CONCURRENCY_STREAM/*MAX_NUM_OPENStream*/  + " "+ protocol2 +" protocols~!");
				persistent_multi_Streams = true;
			}
		}
		return;
	}
	
	class threaded_Authenticate implements Runnable{
		private DLSStream dls_obj = null;
		private int activeIndx;
		threaded_Authenticate(DLSStream obj, int indx){
			dls_obj = obj;
			activeIndx = indx;
		}
		public void run(){
			try {
				String assignedThreadName = dls_obj.username+"@"+dls_obj.host + " ";
				DLSClient client = dls_obj.createClient();
				dls_obj.setClient(client);
				dls_obj.Authenticate("new StreamKey: " + assignedThreadName, "caused to ", token);
				dls_StreamPool.activeStreamPool_status[activeIndx] = true;
				final int STRIDE = dls_StreamPool.STRIDE;
				for(int i = 1; i < STRIDE; i ++){
					int indx = activeIndx+i;
					DLSStream oDls = dls_StreamPool.streamList.get(indx);
					//oDls.setClient(client);
					dls_StreamPool.activeStreamPool_status[indx] = true;
				}
			} catch (Exception e) {
				dls_obj.setClient(null);
				dls_StreamPool.activeStreamPool_status[activeIndx] = false;
				final int STRIDE = dls_StreamPool.STRIDE;
				for(int i = 1; i < STRIDE; i ++){
					int indx = activeIndx+i;
					DLSStream oDls = dls_StreamPool.streamList.get(indx);
					oDls.setClient(null);
					dls_StreamPool.activeStreamPool_status[indx] = false;
				}
				e.printStackTrace();
			}
		}
	}
	
	final private void setInfo(String host, int port, String username, 
			String password, String protocol, String proxyCertContent, String token, DLSProxyInfo dlsproxy){
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.protocol = protocol;
		this.proxyCertContent = proxyCertContent;
		this.token = token;
		this.proxyinfo = dlsproxy;
	}
	abstract protected String DefaultPort();
	abstract protected void Authenticate(String assignedThreadName, String path, String token)throws Exception;

	protected Vector<FileInfo> listCMD(String assignedThreadName, String listingFullPath)
			throws Exception {
		Vector<FileInfo> fileList = null;
		try{
			long st = 0;
			if(PRINT_TIME){
				logTime logStart = new logTime();
				st = logStart.date.getTime();
			}
			fileList = dlsclient.list(assignedThreadName, listingFullPath);
			if(PRINT_TIME){
				logTime logEnd = new logTime();
				long en = logEnd.date.getTime();
				System.out.println(assignedThreadName + " list " + listingFullPath + " use " + (en - st) + " ms");
				System.out.println(assignedThreadName + " list " + listingFullPath + " finished at " + (en) + " ms");
			}
		}catch (Exception ex){
			//ex.printStackTrace();
			//System.out.println(ex.toString());
			if(FAILURE_STATUS.FAILURE_RECOVERABLE == dlsclient.FailureStatus){
				stream_failstatus = FAILURE_STATUS.FAILURE_RECOVERABLE;
			}else{
				stream_failstatus = FAILURE_STATUS.FAILURE_RETRANSMIT;
			}
			throw ex;
		}
		return fileList;		
	}
	
	
	protected String retrieveContents(String assignedThreadName, String path, DLSResult dlsresult) throws Exception{
		long st = 0;
		if (!path.endsWith("/"))
			path = path+"/";		
		if(PRINT_TIME){
			st = System.currentTimeMillis();
		}
		Vector<FileInfo> fileList = listCMD(assignedThreadName, path);
		String list_result = DLSResult.convertion(fileList, dlsresult);
		if(PRINT_TIME){
			long en = System.currentTimeMillis();
			System.out.print("listCMD takes: " + (en - st) + " ms.\t");
		}
		return list_result;		
	}
}
