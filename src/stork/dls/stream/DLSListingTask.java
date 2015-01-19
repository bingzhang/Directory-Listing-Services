package stork.dls.stream;

import java.net.URI;
import java.net.URISyntaxException;

import stork.dls.client.DLSClient;


public class DLSListingTask {
	protected final static boolean PIPEMYSELF = true;
	public int TTL;
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
	
	private final String zone;
	private String password;
	private String irodsHomeDirectory;
	private final String defaultresource;
	private String irodshost;
	
    private DLSClient proxyclient = null;
    
    public DLSClient getClient(){
        return proxyclient;
    }
    
    public void bindClient(DLSClient proxyclient){
        this.proxyclient = proxyclient;
    }
	
    public DLSListingTask(URI uri){
    	TTL = 3;
        cachekey = null;
        this.uri = uri;
        username = null;
        serverName = null;
        fethchingPath = null;
        proxy = null;
        threadID = 0;
        dlsproxy = null;
        forceRefresh = false;
        enablePrefetch = false;
        zone = null;
        defaultresource = null;
    }
    
	public DLSListingTask(long threadID, URI uri, DLSProxyInfo dlsproxy, boolean forceRefresh, boolean enablePrefetch, String proxy, String zone, String resource){
		TTL = 3;
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
			//if(serverName.equals("osg-xsede.grid.iu.edu")){
				if(protocol.toLowerCase().equals("ftp")){
					dlsprotocol = protocol + "DLS";
					//dlsprotocol = protocol;
				}else if(protocol.toLowerCase().equals("gsiftp")){
					dlsprotocol = protocol + "DLS";
				}else if(protocol.toLowerCase().equals("irods")){
					dlsprotocol = protocol + "DLS";
				}
				URI newuri = null;
				try {
					newuri = new URI(dlsprotocol, uri.getUserInfo(), uri.getHost(), uri.getPort(), pathEntry, null, null);
					uri = newuri; 
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
			//}
		}
		this.zone = zone;
		this.defaultresource = resource;
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
			if (sa.length > 1) password = sa[1];
		}else{
			username = user;
		}
		
		this.inmemCacheElapseTime = 0;
	}
	
	public String getiRodsHome(){
		return this.irodsHomeDirectory;
	}
	public void setiRodsHome(String home){
		this.irodsHomeDirectory = home;
	}
	
	public String getiRodsHost(){
		return this.irodshost;
	}
	public void setiRodsHost(String host){
		this.irodshost = host;
	}

	public String getServiceID(){
		if(null == this.assignedStream){
			final String STREAMKEY = username+"@"+serverName;//?username?\
			return STREAMKEY;
		}else{
			return this.assignedStream.getStreamKey();
		}
	}
	
	public String getPassword(){
		return this.password;
	}
	
	public String getUserName(){
		return this.username;
	}
	
	public String getZone(){
		return this.zone;
	}

	public String getResource(){
		return this.defaultresource;
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
