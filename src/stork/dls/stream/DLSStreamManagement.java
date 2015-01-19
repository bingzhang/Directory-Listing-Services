package stork.dls.stream;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import stork.dls.stream.type.DLSFTPStream;
import stork.dls.stream.type.DLSGSIFTPStream;
import stork.dls.stream.type.DLSIRODSStream;
import stork.dls.stream.type.FTPStream;
import stork.dls.stream.type.GFTPStream;
import stork.dls.stream.type.SFTPStream;

/**
 * different type of streams
 * @author bing
 *
 */
public class DLSStreamManagement {
	public static boolean DEBUG_PRINT;// = false;//false;//true
	private final ReadWriteLock spinlock = new ReentrantReadWriteLock();
	private class DLSManager{
		HashMap<String, DLSStream> supportedProtocols = new HashMap<String, DLSStream>();
		private void create_management(String protocol, String streamkey) throws Exception{
			if(protocol.toLowerCase().equals("ftpdls")){
				final DLSStream dls_StreamMng = new DLSFTPStream(streamkey);
				supportedProtocols.put("ftpDLS", dls_StreamMng);
			}else if(protocol.toLowerCase().equals("irodsdls")){
				final DLSStream dls_StreamMng = new DLSIRODSStream(streamkey);
				supportedProtocols.put("irodsDLS", dls_StreamMng);
			}else if(protocol.toLowerCase().equals("gsiftpdls")){
				final DLSStream dls_StreamMng = new DLSGSIFTPStream(streamkey);
				supportedProtocols.put("gsiftpDLS", dls_StreamMng);
			}else if(protocol.toLowerCase().equals("ftp")){
				final DLSStream dls_StreamMng = new FTPStream(streamkey);
				supportedProtocols.put("ftp", dls_StreamMng);	
			}else if(protocol.toLowerCase().equals("gsiftp")){
				final DLSStream dls_StreamMng = new GFTPStream(streamkey);
				supportedProtocols.put("gsiftp", dls_StreamMng);	
			}else if(protocol.toLowerCase().equals("sftp")){
				final DLSStream dls_StreamMng = new SFTPStream(streamkey);
				supportedProtocols.put("sftp", dls_StreamMng);	
			}
		}
	}	
	private HashMap<String, DLSManager> inMemoryStreamsMap = new HashMap<String, DLSManager>();

	public DLSStream allocate_Stream(DLSListingTask listingtask, DLSProxyInfo dlsproxy, String proxyCertContent, String token) throws Exception{
	    URI uri = listingtask.getUri();
		int port = uri.getPort();
		String protocol = uri.getScheme();
		String serverName = uri.getHost();
		String username = uri.getUserInfo();
		String password = null;
		String ui = uri.getUserInfo();
		if(null == protocol){
			throw new Exception("URI protocol Error: URI is " + uri.toString());
		}
		if (ui != null && !ui.isEmpty()) {
			String sa[] = ui.split(":");
			username = sa[0];
			if (sa.length > 1) password = sa[1];
		}
		
		final String STREAMKEY = username+"@"+serverName;//?username?
		int states = 1;
		String protocolKey;
		if(protocol.toLowerCase().equals("ftp")){
			protocolKey = "ftp";
		}else if(protocol.toLowerCase().equals("gsiftp")){
			protocolKey = "gsiftp";
		}else if(protocol.toLowerCase().equals("sftp")){
			protocolKey = "sftp";
		} else if(protocol.toLowerCase().equals("ftppipe")){
			protocolKey = "ftpPipe";
		} else if(protocol.toLowerCase().equals("gsiftppipe")){
			protocolKey = "gsiftpPipe";
		} else if(protocol.toLowerCase().equals("irodsdls")){
			protocolKey = "irodsDLS";
		} else if(protocol.toLowerCase().equals("gsiftpdls")){
			protocolKey = "gsiftpDLS";
		} else if(protocol.toLowerCase().equals("ftpdls")){
			protocolKey = "ftpDLS";
		}else{
			throw new Exception(protocol + "setup connection failed!\n" + "Unsupported protocol: "+ protocol + "\n");
		}
		while(true){
			DLSManager dls_manager = null;
			if(spinlock.readLock().tryLock()){
				try{
					dls_manager = inMemoryStreamsMap.get(STREAMKEY);
					if(null == dls_manager){
						states = 1;
					}else if (null == dls_manager.supportedProtocols.get(protocolKey)){
						states = 1;
					}else{
						states = 2;
					}
				}finally{
					if(1 == states){
						spinlock.readLock().unlock();
						while(true){
							if(spinlock.writeLock().tryLock()){
								try{
									dls_manager = inMemoryStreamsMap.get(STREAMKEY);
									if(null == dls_manager){
										final DLSManager dls_manager_true = new DLSManager();
										dls_manager_true.create_management(protocol, STREAMKEY);
										inMemoryStreamsMap.put(STREAMKEY, dls_manager_true);
									}
								}finally{
									states = 2;
									spinlock.writeLock().unlock();
								}
								break;
							}
						}		
					}else{
						spinlock.readLock().unlock();					
					}
				}
				break;				
			}
		}
		DLSManager dls_manager = null;
		if(spinlock.readLock().tryLock()){
			try{
				dls_manager = inMemoryStreamsMap.get(STREAMKEY);
			}finally{
				spinlock.readLock().unlock();
			}
		}
		DLSStream curStreamMng = null;
		if(DEBUG_PRINT){
			System.out.println("dls_manager.supportedProtocols: "+ dls_manager.supportedProtocols);
		}
		synchronized(dls_manager.supportedProtocols){
			curStreamMng = dls_manager.supportedProtocols.get(protocolKey);
			if(null == curStreamMng){
				dls_manager.create_management(protocolKey, STREAMKEY);
				curStreamMng = dls_manager.supportedProtocols.get(protocolKey);
			}	
			
			try{
				if(null != dlsproxy && -1 != protocolKey.indexOf("gsi")){
					curStreamMng.proxyinfo = dlsproxy;
					new DLSProxyCred(curStreamMng.proxyinfo, curStreamMng.getStreamKey());
				}
				curStreamMng.initStreamPool(listingtask, token);
			}catch (Exception ex){
				dls_manager.supportedProtocols.remove(protocolKey);
				curStreamMng = null;
				throw new Exception(protocolKey, ex);
			}
		}
		return curStreamMng;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
