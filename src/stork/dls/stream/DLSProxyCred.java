package stork.dls.stream;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.ietf.jgss.GSSCredential;
import org.globus.myproxy.*;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import stork.dls.util.DLSLogTime;

/**
 * for gsi cred
 * can throw exception back if there is something wrong during creating proxy cred.
 * @param host : default "myproxy.teragrid.org" (without quotes)
 * @author bing
 * @see	DLSStream	
 */

class Sentinel{
	GSSCredential cred = null;
	long cred_renew_time = 0;
}
public class DLSProxyCred {
	private final static long ELAPSE_TIME = 21600000;//6 hours//"06:00:00:00";
	private final static ReadWriteLock spinlock = new ReentrantReadWriteLock();
    private final static Map<String, Sentinel> credMap = new LinkedHashMap<String, Sentinel>();

    public DLSProxyCred(DLSProxyInfo proxyinfo, String streamkey) throws Exception{
    	if(null != proxyinfo){
    		genCred(proxyinfo, streamkey);
    	}
    }
    
    public DLSProxyCred(DLSStream stream) throws Exception{
    	genCred(stream.proxyinfo, stream.getStreamKey());
    }
    
    public static GSSCredential getCred(DLSStream stream) throws Exception{
    	GSSCredential cred = null;
    	String proxyCertContent = stream.proxyCertContent;
    	if (proxyCertContent == null || proxyCertContent.isEmpty()){
    		cred = genCred(stream.proxyinfo, stream.getStreamKey());
    	}else{
			byte[] proxyCertContentArray = proxyCertContent.getBytes();
			ExtendedGSSManager gssm = (ExtendedGSSManager) ExtendedGSSManager
					.getInstance();
			cred = gssm.createCredential(proxyCertContentArray,
					ExtendedGSSCredential.IMPEXP_OPAQUE,
					GSSCredential.DEFAULT_LIFETIME, null,
					GSSCredential.INITIATE_AND_ACCEPT);
    	}
		return cred;
    }
  
	private static GSSCredential genCred(DLSProxyInfo proxyinfo, String streamkey) throws Exception{
		int states = 1;
		while(true){
			Sentinel sentinel = null;
			if(spinlock.readLock().tryLock()){
				try{
					sentinel = credMap.get(streamkey);
					if(null == sentinel){
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
								sentinel = credMap.get(streamkey);
								if(null == sentinel){
									final Sentinel sentinel_true = new Sentinel();
									DLSLogTime curThreadTime = new DLSLogTime();
									long curMilsec = curThreadTime.getTime();
									sentinel_true.cred_renew_time = curMilsec;
									credMap.put(streamkey, sentinel_true);
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
			}//finally
				break;
			}
		}//while(true)
		final Sentinel curSentinel = credMap.get(streamkey);
		synchronized(curSentinel){
			DLSLogTime curThreadTime = new DLSLogTime();
			long curMilsec = curThreadTime.getTime();
			long lastMilsec = curSentinel.cred_renew_time;
			if(null == curSentinel.cred || ((curMilsec - lastMilsec) > ELAPSE_TIME)){
				String host = proxyinfo.proxyserver;
				int port = proxyinfo.credport;
				String username = proxyinfo.account;
				String password = proxyinfo.passphrase;
				if (port < 0) port = MyProxy.DEFAULT_PORT;
				curSentinel.cred = null;
				MyProxy proxy = new MyProxy(host, port);
				curSentinel.cred = proxy.get(username, password, 3600);
			}
		}
		return curSentinel.cred;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
