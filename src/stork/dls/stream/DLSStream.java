package stork.dls.stream;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import org.globus.ftp.FileInfo;
import org.irods.jargon.core.pub.CollectionAndDataObjectListAndSearchAO;

import stork.dls.client.DLSClient;
import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSIOAdapter.FETCH_PREFETCH;
import stork.dls.util.DLSResult;

/**
 * stream proxy
 * @author bing
 * @see DLSStreamPool
 */
public abstract class DLSStream{
	public static boolean DEBUG_PRINT ;//= true;//false;//true
	static final boolean PRINT_TIME = false;//true;//false;
	public static final int ONE_PIPE_CAPACITY = DLSClient.ONE_PIPE_CAPACITY;

	DLSProxyInfo proxyinfo;
	private final String streamKeyID;
	public final int streamID;
	protected int 	 port;
	protected String host;
	protected String username;
	protected String password;
	protected String proxyCertContent;
	protected String protocol;
	protected String realprotocol;
	protected String token = null;
	
	/*replacable localCC*/
	protected DLSFTPMetaChannel localCC;
	protected CollectionAndDataObjectListAndSearchAO actualCollection;
	
	protected final ReadWriteLock spinlock = new ReentrantReadWriteLock();
	@GuardedBy("DLSStream.spinlock")
	public int value = 0;	

    public volatile boolean isAvailable = false;   
    public volatile REINCARNATION_STATUS isReincarnation = REINCARNATION_STATUS.NOT_PROCESSING;
	
    public enum REINCARNATION_STATUS{
        NOT_PROCESSING,
        PROCESSING
    }
    
    public enum CHANNEL_STATE{
        DC_IGNORE,//DC, permission, wrong path
        DC_RETRANSMIT,//DC,migration
        DC_SUCCESS, // successful
        CC_REVIVE,//CC recreate
        CC_BENIGN
    };
    
    public DLSFTPMetaChannel getCC(){
        return localCC;
    }
    
	public String getStreamKey(){
		return this.streamKeyID;
	}
	
	public ReadWriteLock getspinlock(){
	    return this.spinlock;
	}
	
	public boolean increaseValue(){
        if(false == isAvailable){
            return false;
        }
        if(spinlock.readLock().tryLock()){
            try{
                this.value++;
            }finally{
                spinlock.readLock().unlock();    
            }
            return true;
        }else{
            return false;
        }
	}
	
	/**
	 * reserveValue using spinlock.readlock
	 * since it is only invoked in uniquely synchronized streampool,
	 * so read concurrency can not happen on it.
	 * @return
	 */
	public boolean reserveValue(){
        if(false == isAvailable){
            return false;
        }
        if(spinlock.readLock().tryLock()){
            try{
                this.value--;
            }finally{
                spinlock.readLock().unlock();    
            }
            return true;
        }else{
            return false;
        }
	}
	
	/**
	 *  isAvailable is important. any time-consuming operation within CRITICALSECTION
	 *  must set isAvailable is false first.
	 *  
	 *  StreamPool getAvailableStream will keep on reading the status of volatile "isAvailable"
	 *  somewhere will write it.
	 *  
	 * @return
	 */
	public int getValue(){
	    
        if(false == isAvailable){
            return 0;
        }
        if(spinlock.readLock().tryLock()){
            int val;
            try{
                val = this.value;
            }finally{
                spinlock.readLock().unlock();    
            }
            return val;
        }else{
            return 0;
        }
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
	
	protected DLSStream(String streamKey){
	    this.streamID = -1;
	    this.streamKeyID = streamKey;
	    dls_StreamPool = new DLSStreamPool();
	}  /**
     * dummy stream
     * @param streamKey
     */protected DLSStream(int id, String streamKey){
	    this.streamID = id;
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
	
	final public DLSStream MigrationStream(DLSListingTask listingtask, String assignedThreadName, String path, DLSStream deadStream, int activeIndx){
		if(null != deadStream){
			return dls_StreamPool.MigrationStream(listingtask, assignedThreadName, path, deadStream, activeIndx);
		}else{
		    System.out.println("MigrationStream deadStream is null");
		    return null;
		}
	}
	
	final public void releaseThisStream(DLSStream thisStream, String assignedThreadName, String path, int activeIndx){
		dls_StreamPool.releaseThisStream(thisStream, assignedThreadName, path, activeIndx);
	}
	
	abstract public void fillStreamPool(String streamKey);
	protected DLSClient createClient(){
	    return new DLSClient(this);
	}
	
	final public void initStreamPool(final DLSListingTask listingtask,
			final String token) throws Exception{
		if(persistent_multi_Streams){
			return;
		}else{
		    final URI uri = listingtask.getUri();
		    final DLSProxyInfo dlsproxy = listingtask.getDlsproxy();
		    final String proxyCertContent2 = listingtask.getProxy();
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

			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
				DLSStream oDls = dls_StreamPool.streamList.get(i);
				oDls.protocol = protocol2;
				int port = port2;
				if (port2 == -1){
					realProtocol = oDls.DefaultPort();
					port = oDls.port;
				}				
				oDls.setInfo(host2, port, username2, password2, realProtocol, proxyCertContent2, token, dlsproxy);
			}
			String threadGroupID = "pre-Authenticate initStreamPool";
			ArrayList<Thread> threadList = new ArrayList<Thread>(DLSStreamPool.NUM_CONCURRENCY_STREAM);
			//concurrent authenticate streams
			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
				DLSStream stream = dls_StreamPool.streamList.get(i);
				Runnable r = new threaded_Authenticate(stream, i, listingtask);
				Thread t = DLSThreadsManager.Resources.reserveSingle(threadGroupID, r);
				threadList.add(t);
				t.start();
			}
			for(Thread t : threadList){
				t.join();
			}
			//release threads resource
			DLSThreadsManager.Resources.finalAll(threadGroupID);
			
			//sanity check
			int sanity_check = 0;
			for(int i = 0; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i++){
				DLSStream stream = dls_StreamPool.streamList.get(i);
				if(true == stream.isAvailable){
					sanity_check ++;
				}
			}

			if(sanity_check != dls_StreamPool.MAX_NUM_OPENStream){
				System.out.println("\nnew Stream Resource "+this.streamKeyID +" failed~! to open #Streams: " + sanity_check +" of total preassigned "+DLSStreamPool.NUM_CONCURRENCY_STREAM/*MAX_NUM_OPENStream*/ + " "+ protocol2 +" protocols~!");
				throw new Exception("remote servers deny service~!\n");
			}else{
				System.out.println("\nnew Stream Resource "+this.streamKeyID +" finish to open #cc:" + DLSStreamPool.NUM_CONCURRENCY_STREAM/*MAX_NUM_OPENStream*/ + " #pp:" + ONE_PIPE_CAPACITY + " of total preassigned " + " "+ protocol2 +" protocols~!");
				persistent_multi_Streams = true;
			}
		}
		return;
	}
	
	class threaded_Authenticate implements Runnable{
		private DLSStream stream = null;
		private DLSListingTask listingtask = null;
		threaded_Authenticate(DLSStream obj, int indx, DLSListingTask listingtask){
			stream = obj;
			this.listingtask = listingtask;
		}
		public void run(){
			try {
				String assignedThreadName = stream.username+"@"+stream.host + " ";
				//stream.createClient();
				stream.Authenticate(listingtask, "new StreamKey: " + assignedThreadName, "caused to ", token);
				stream.isAvailable = true;
			} catch (Exception e) {
				stream.isAvailable = false;
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
	abstract protected void Authenticate(DLSListingTask listingtask, String assignedThreadName, String path, String token)throws Exception;

	protected Vector<FileInfo> listCMD(DLSListingTask listingtask, String assignedThreadName, String listingFullPath)
			throws Exception {
	    Vector<FileInfo> fileList = null;
	    DLSClient proxyclient = listingtask.getClient();
		try{
			long st = 0;
			if(PRINT_TIME){
				logTime logStart = new logTime();
				st = logStart.date.getTime();
			}
			fileList = proxyclient.list(listingtask, this.localCC, assignedThreadName, listingFullPath);
			if(PRINT_TIME){
				logTime logEnd = new logTime();
				long en = logEnd.date.getTime();
				System.out.println(assignedThreadName + " list " + listingFullPath + " use " + (en - st) + " ms");
				System.out.println(assignedThreadName + " list " + listingFullPath + " finished at " + (en) + " ms");
			}
		}catch (Exception ex){
			//ex.printStackTrace();
			int existed = ex.toString().indexOf("500-500 Command failed");
			if(-1 != existed){
			    System.out.println(ex);
			    System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " cc stat: " + proxyclient.channelstate);
			}
			
		    if(proxyclient.channelstate == CHANNEL_STATE.CC_REVIVE){
		        /**
		         * set exception to all the threads which are still queuing.
		         * And let this thread which caused the CC_REVIVE to revive this cc.
		         */
		        System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " to do CC revive");
		        throw ex;
		    }else if(proxyclient.channelstate == CHANNEL_STATE.DC_RETRANSMIT){
		    	System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + "TTL: " + listingtask.TTL +" DC migrate TTL");
		    	if(listingtask.TTL <= 0){
		    		proxyclient.channelstate = CHANNEL_STATE.DC_IGNORE;
		    		System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC migrate to DC ignore");
		    		//throw ex;
		    	}else{
			    	listingtask.TTL--;
			        System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC migrate");
			        throw ex;
		    	}
		    }else if(proxyclient.channelstate == CHANNEL_STATE.DC_IGNORE){
		        fileList = null;
		        System.out.println("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC ignore");
		    }
		}
		return fileList;
	}
	
	
	protected String retrieveContents(DLSListingTask listingtask, 
	        String assignedThreadName, String path, DLSResult dlsresult) throws Exception{
		long st = 0;
		if (!path.endsWith("/"))
			path = path+"/";		
		if(PRINT_TIME){
			st = System.currentTimeMillis();
		}
		Vector<FileInfo> fileList = listCMD(listingtask, assignedThreadName, path);
		if(null == fileList && CHANNEL_STATE.CC_BENIGN == listingtask.getClient().channelstate){
			fileList = new Vector();
		}
		String list_result = DLSResult.convertion(fileList, dlsresult);
		if(PRINT_TIME){
			long en = System.currentTimeMillis();
			System.out.print("listCMD takes: " + (en - st) + " ms.\t");
		}
		return list_result;		
	}
	
	   /**
     * close channel
     */
    public void closeCC() {
        this.isAvailable = false;
        try {
            if(null == localCC) return;
            localCC.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        localCC = null;
    }
}
