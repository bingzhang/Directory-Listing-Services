package stork.dls.stream;

import java.net.URI;
import java.net.URISyntaxException;


public class DLSFetchingTask {
	protected final static boolean PIPEMYSELF = true;
	public  long inmemCacheElapseTime;
	public  DLSStream assignedStream;
	private final String cachekey;
	private final URI uri;
	private final String username;
	private final String serverName;
	private final String fethchingPath;
	private final String proxy;
	private final long threadID;
	private final DLSProxyInfo dlsproxy;
	private final boolean forceRefresh;
	private final boolean enablePrefetch;
	
	public DLSFetchingTask(long threadID, URI uri, DLSProxyInfo dlsproxy, boolean forceRefresh, boolean enablePrefetch, String proxy){
		uri = uri.normalize();
		String serverName = uri.getHost();
		String pathEntry = uri.getPath();
		if(!pathEntry.endsWith("/")){
			pathEntry = pathEntry +"/";
			try {
				uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), pathEntry, uri.getQuery(), uri.getFragment());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}

		if(null != proxy && proxy.isEmpty()){
			proxy = null;
		}
		String protocol = uri.getScheme();
		String dlsprotocol = protocol; 
		if(PIPEMYSELF){
			if(serverName.equals("osg-xsede.grid.iu.edu")){
				if(protocol.toLowerCase().equals("ftp")){
					dlsprotocol = protocol + "DLS";
				}else if(protocol.toLowerCase().equals("gsiftp")){
					dlsprotocol = protocol + "DLS";
				}
				URI newuri = null;
				try {
					newuri = new URI(dlsprotocol, uri.getUserInfo(), uri.getHost(), uri.getPort(), pathEntry, null, null);
					uri = newuri; 
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
			}
		}
		this.uri = uri;
		this.proxy = proxy;
		this.threadID = threadID;
		this.dlsproxy = dlsproxy;
		this.assignedStream = null;
		this.fethchingPath = pathEntry;
		this.serverName = uri.getHost();
		this.forceRefresh = forceRefresh;
		this.enablePrefetch = enablePrefetch;
		this.cachekey = serverName+"@"+pathEntry;
		String user = uri.getUserInfo();
		String ui = uri.getUserInfo();
		if (ui != null && !ui.isEmpty()) {
			String sa[] = ui.split(":");
			username = sa[0];
		}else{
			username = user;
		}
		this.inmemCacheElapseTime = 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public String getServiceID(){
		if(null == this.assignedStream){
			final String STREAMKEY = username+"@"+serverName;//?username?\
			return STREAMKEY;
		}else{
			return this.assignedStream.getStreamKey();
		}
	}
	
	public URI getUri() {
		return uri;
	}

	public String getProxy() {
		return proxy;
	}

	/**
	 * @return the threadID
	 */
	public long getThreadID() {
		return threadID;
	}

	/**
	 * @return the dlsproxy
	 */
	public DLSProxyInfo getDlsproxy() {
		return dlsproxy;
	}

	/**
	 * @return the forceRefresh
	 */
	public boolean isForceRefresh() {
		return forceRefresh;
	}

	/**
	 * @return the enablePrefetch
	 */
	public boolean isEnablePrefetch() {
		return enablePrefetch;
	}

	/**
	 * @return the fethchingPath
	 */
	public String getFethchingPath() {
		return fethchingPath;
	}

	/**
	 * @return the serverName
	 */
	public String getServerName() {
		return serverName;
	}

	/**
	 * @return the cachekey
	 */
	public String getCachekey() {
		return cachekey;
	}

}
