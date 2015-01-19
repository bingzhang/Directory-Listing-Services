package stork.dls.stream;

import java.net.URI;
import javax.annotation.concurrent.GuardedBy;

import stork.dls.client.DLSClient;
import stork.dls.io.local.DBCache;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
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
	public static DLSStreamManagement dls_Stream_management;
	
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
	
	public static DLSStream initStream(DLSListingTask listingtask, URI uri, DLSProxyInfo dlsproxy, String proxy) throws Exception{
		DLSStream StreamMgn = null;
		
		try{
			StreamMgn = dls_Stream_management.allocate_Stream(listingtask, dlsproxy, proxy, null);
		}catch (Exception ex){
			throw new Exception("DLSStream.allocate_Stream failed~\n");
		}
		return StreamMgn;
	}
	
	public StreamInfo StreamSnapshot(DLSListingTask listingtask) throws Exception{
		DLSStream StreamMgn = null;
		StreamInfo ret = new StreamInfo();
		try{
			StreamMgn = dls_Stream_management.allocate_Stream(listingtask, null, null, null);
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
	public String getDirectoryContents(String assignedThreadName, DLSListingTask listingtask, DLSResult dlsresult, int activeStreamID, String token)
					throws Exception{
		//long st = System.currentTimeMillis();
		String result = "";
		DLSStream StreamMgn = null;
		final URI uri = listingtask.getUri();
		final String path = listingtask.getFethchingPath();
		
		if (!listingtask.isForceRefresh() /*|| TTL is not OK*/) {
			result = db_cache.Lookup(uri.getHost(), path);
			if(!result.equals(DBCache.NoEntry)) {
				DLSResult.preparePrefetchingList(result, dlsresult);
				return result;
			}
		}
		//read from network
		try{
			final DLSProxyInfo dlsproxy = listingtask.getDlsproxy();
			final String proxyCertContent = listingtask.getProxy();
			StreamMgn = dls_Stream_management.allocate_Stream(listingtask, dlsproxy, proxyCertContent, token);
		}catch (Exception ex){
			ex.printStackTrace();
			throw new Exception("DLSStream.createStreamPool failed~\n");
		}

		do{
            while(null == listingtask.assignedStream){
                listingtask.assignedStream = StreamMgn.getAvailableStream(/*extraInfo+*/assignedThreadName, path, doFetching);
                DLSClient dummy = listingtask.assignedStream.createClient();
                listingtask.bindClient(dummy);
                /*
                if(null != listingtask.assignedStream){
                    System.out.println(assignedThreadName + " path = "+path+"; with activeindex = " + listingtask.assignedStream.streamID);
                }else{
                    System.out.println(assignedThreadName + " path = " +path + "trying getAvailableStream" );
                }*/
            }
            
            while(true){
    	        try{
    	            result = listingtask.assignedStream.retrieveContents(listingtask, assignedThreadName, path, dlsresult);
    	            break;
    	        }catch (Exception ex){
    	            //ex.printStackTrace();
    	            //System.out.println(assignedThreadName + " got exception; path = "+path+"; with activeindex = " + listingtask.assignedStream.activeStreamIndx);
    	            listingtask.assignedStream = StreamMgn.MigrationStream(listingtask, assignedThreadName, path, 
    	                    listingtask.assignedStream, listingtask.assignedStream.streamID);
    	            if(null == listingtask.assignedStream){
    	                break;
    	            }else{
    	                boolean reservable = listingtask.assignedStream.reserveValue();
    	                if(!reservable){
    	                    listingtask.assignedStream = null;
    	                    break;
    	                }
    	            }
    	        }
            }
            DLSClient proxyclient = listingtask.getClient();
            if(CHANNEL_STATE.DC_IGNORE ==proxyclient.channelstate){
                break;
            }
		}while(null == listingtask.assignedStream);
		StreamMgn.releaseThisStream(listingtask.assignedStream, /*extraInfo+*/assignedThreadName, path, listingtask.assignedStream.streamID);
		isNetwork = true;
		if(null != result){
			String adstring = dlsresult.getAdString();
			db_cache.put(uri.getHost(), path, adstring);
		}else{
			//result = NOSUCHEXIST.toString();
		    System.out.println(assignedThreadName + " finish; path = "+path+"; with activeindex = " + listingtask.assignedStream.streamID);
			result = null;
		}
		return result;
	}
}