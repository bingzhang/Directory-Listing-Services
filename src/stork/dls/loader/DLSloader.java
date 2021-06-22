package stork.dls.loader;

import java.net.URL;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import stork.dls.ad.Ad;
import stork.dls.config.DLSConfig;
import stork.dls.inmemcache.MemoryCache;
import stork.dls.io.local.DBCache;
import stork.dls.rabbitmq.CloudReceiver;
import stork.dls.rest.RestInterface;

public class DLSloader implements ServletContextListener{
	public void contextDestroyed(ServletContextEvent arg0) {
	}
	public void contextInitialized(ServletContextEvent arg0) {
		String name = "DLS.conf";
		try{
			URL url = this.getClass().getClassLoader().getResource(name);
			Ad ad = Ad.parse(url.openStream());
			DBCache.BDB_PATH = ad.get("this.BDB_PATH", DBCache.BDB_PATH);
			MemoryCache.inMemoryCache_capacity = ad.getInt("this.inMemoryCache_capacity", MemoryCache.inMemoryCache_capacity);
			RestInterface.ip = ad.get("ip", RestInterface.ip);
			RestInterface.hostname = ad.get("hostname", RestInterface.hostname);
			DLSConfig.CC_LISTING = ad.getBoolean("this.CC_LISTING", DLSConfig.CC_LISTING);
			DLSConfig.TCPNODELAY = ad.getBoolean("this.TCP_NODELAY", DLSConfig.TCPNODELAY);
			DLSConfig.DLS_CONCURRENCY_STREAM = ad.getInt("this.DLS_CONCURRENCY_STREAM", DLSConfig.DLS_CONCURRENCY_STREAM);
			DLSConfig.SINGLETON = ad.getBoolean("this.SINGLETON", DLSConfig.SINGLETON);
			DLSConfig.REPLICA_QUEUE_HOST = ad.get("replicaQueue.host", DLSConfig.REPLICA_QUEUE_HOST);
			DLSConfig.DLSEDGE = ad.getBoolean("this.DLSEDGE", DLSConfig.DLSEDGE);
			
			if(!DLSConfig.DLSEDGE) {
				CloudReceiver cloudreceiver = new CloudReceiver(DLSConfig.REPLICA_QUEUE_HOST);
			}
		}catch (Exception ex){
			ex.printStackTrace();
			System.out.println("fail to open DLS.conf ~!");
			return;
		}
		
		System.out.println("{{ DLS on: " + RestInterface.ip + " " + RestInterface.hostname );
		System.out.println("system listing on control channel: " + DLSConfig.CC_LISTING);
		System.out.println("DLSConfig.SINGLETON: " + DLSConfig.DLSEDGE);
		System.out.println("dls.conf.BDB_PATH: " + DBCache.BDB_PATH);
		System.out.println("MemoryCache.inMemoryCache_capacity: " + MemoryCache.inMemoryCache_capacity);
		System.out.println("DLSConfig.DLS_CONCURRENCY_STREAM: " + DLSConfig.DLS_CONCURRENCY_STREAM);
		System.out.println("DLSConfig.DLS_PIPE_CAPACITY: " + DLSConfig.DLS_PIPE_CAPACITY);
		System.out.println("DLSConfig.SINGLETON: " + DLSConfig.SINGLETON);
		System.out.println("init all the components }}");
		
		if (DLSConfig.SINGLETON) {
			System.out.println("[Connect to replica queue Host] " + DLSConfig.REPLICA_QUEUE_HOST);
		}
		
		try{
			stork.dls.config.DebugConfig.DebugPrint();
			stork.dls.service.prefetch.DLSThreadsManager.INIT();
			stork.dls.rest.RestInterface.INIT();
			stork.dls.stream.DLSIOAdapter.INIT();
			
		}catch (Exception e) {
			System.out.println("fail to init all the components~!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("contextInitialized done!~");
	}

}
