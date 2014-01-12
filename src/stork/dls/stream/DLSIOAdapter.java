package stork.dls.stream;

import java.net.URI;
import javax.annotation.concurrent.GuardedBy;

import stork.dls.io.local.DBCache;
import stork.dls.util.DLSResult;

/**
 * adaptor of local IO and network IO
 * 
 * @author bingzhan@buffalo.edu (Bing Zhang)
 * 
 * @see	DBCache
 * @see	DLSStreamManagement
 * @see	DLSStream
 * */
public class DLSIOAdapter {
	static final boolean PRINT_TIME = false;//true
	public static enum FETCH_PREFETCH{
		FETCH,
		PREFETCH
	};
	private FETCH_PREFETCH doFetching = FETCH_PREFETCH.FETCH;
	private static DBCache db_cache;
	private boolean isNetwork = false;
	private static DLSStreamManagement dls_Stream_management;
	private final static String NOSUCHEXIST = "No such file or directory";
	private final static String PERMISSIONDENY = "Permission denied";
	
	public DLSIOAdapter(){
		doFetching = FETCH_PREFETCH.FETCH;
	}public DLSIOAdapter(FETCH_PREFETCH fetchOrpre){
		doFetching = fetchOrpre;
	}
	
	
	/**
	 * init/start the singleton out-Memory (DB) cache.
	 */
	public static void INIT() throws Exception{
		db_cache = new DBCache();
		dls_Stream_management = new DLSStreamManagement();
	}
	
	public static void reset() throws Exception {
		if (db_cache != null)
			db_cache.reset();
	}
	
	public boolean accesstype(){
		return isNetwork;
	}
	
	public boolean contains(String serverName, String path){
		if(null == db_cache){
			return false;
		}
		return db_cache.contains(serverName, path);
	}
	
	public class StreamInfo{
		public int available = 0;
		public int waited = 0;
	}
	
	public static DLSStream initStream(URI uri, DLSProxyInfo dlsproxy, String proxy) throws Exception{
		DLSStream StreamMgn = null;
		
		try{
			StreamMgn = dls_Stream_management.allocate_Stream(uri, dlsproxy, proxy, null);
		}catch (Exception ex){
			throw new Exception("DLSStream.allocate_Stream failed~\n");
		}
		return StreamMgn;
	}
	
	public StreamInfo StreamSnapshot(URI uri) throws Exception{
		DLSStream StreamMgn = null;
		StreamInfo ret = new StreamInfo();
		try{
			StreamMgn = dls_Stream_management.allocate_Stream(uri, null, null, null);
		}catch (Exception ex){
			throw new Exception("DLSStream.allocate_Stream failed~\n");
		}
		StreamMgn.getStreamInfo(ret);
		return ret;
	}
	
	/**
	 * read from local DB or remote server
	 * @param assignedThreadName
	 * @param path
	 * @param uri
	 * @param dlsresult
	 * @param forceRefresh
	 * @param proxyCertContent
	 * @param assignedStream
	 * @param activeStreamID
	 * @param token
	 * @return Json format string
	 * @throws Exception
	 */
	@GuardedBy("DLSIOAdapter.dls_Stream_management")
	//public String getDirectoryContents(String assignedThreadName, String path, URI uri, DLSProxyInfo dlsproxy, DLSResult dlsresult, boolean forceRefresh, String proxyCertContent, 
		//	DLSStream assignedStream, int activeStreamID, String token)
	public String getDirectoryContents(String assignedThreadName, DLSFetchingTask fetchingtask, DLSResult dlsresult, int activeStreamID, String token)
					throws Exception{
		//long st = System.currentTimeMillis();
		String result = "";
		DLSStream StreamMgn = null;
		final URI uri = fetchingtask.getUri();
		final String path = fetchingtask.getFethchingPath();
		
		if (!fetchingtask.isForceRefresh() /*|| TTL is not OK*/) {
			result = db_cache.Lookup(uri.getHost(), path);
			if(!result.equals(DBCache.NoEntry)) {
				DLSResult.preparePrefetchingList(result, dlsresult);
				return result;
			}
		}
		//read from network
		try{
			final DLSProxyInfo dlsproxy = fetchingtask.getDlsproxy();
			final String proxyCertContent = fetchingtask.getProxy();
			StreamMgn = dls_Stream_management.allocate_Stream(uri, dlsproxy, proxyCertContent, token);
		}catch (Exception ex){
			ex.printStackTrace();
			throw new Exception("DLSStream.createStreamPool failed~\n");
		}
		if(null == fetchingtask.assignedStream){
			fetchingtask.assignedStream = StreamMgn.getAvailableStream(/*extraInfo+*/assignedThreadName, path, doFetching);
			//System.out.println(assignedThreadName + " got getAvailableStream; path = "+path+"; with activeindex = " + activeIndxObj.activeindex);
		}
		
		try{
			result = fetchingtask.assignedStream.retrieveContents(/*extraInfo+*/assignedThreadName, path, dlsresult);
		}catch (Exception ex){
			ex.printStackTrace();
			int existed = ex.toString().indexOf(PERMISSIONDENY);
			if(-1 == existed){
				existed = ex.toString().indexOf(NOSUCHEXIST);
			}
			if(-1 == existed){
				//System.out.println(assignedThreadName + " got exception; path = "+path+"; with activeindex = " + activeIndxObj.activeindex);
				fetchingtask.assignedStream = StreamMgn.MigrationStream(/*extraInfo+*/assignedThreadName, path, fetchingtask.assignedStream, fetchingtask.assignedStream.activeStreamIndx);
				if(null == fetchingtask.assignedStream){
					//System.out.println("unrecoverable StreamMgn.recoveryStream~!");
					return getDirectoryContents(/*extraInfo+*/assignedThreadName, fetchingtask, dlsresult, -1, token);
				}else{
					return getDirectoryContents(/*extraInfo+*/assignedThreadName, fetchingtask, dlsresult, fetchingtask.assignedStream.activeStreamIndx, token);
				}						
			}else{
				result = null;
			}
		}
		StreamMgn.releaseThisStream(fetchingtask.assignedStream, /*extraInfo+*/assignedThreadName, path, fetchingtask.assignedStream.activeStreamIndx);
		isNetwork = true;
		if(null != result){
			String adstring = dlsresult.getAdString();
			db_cache.put(uri.getHost(), path, adstring);
		}else{
			//result = NOSUCHEXIST.toString();
			result = null;
		}
		return result;
	}
}