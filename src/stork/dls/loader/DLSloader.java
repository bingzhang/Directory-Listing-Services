package stork.dls.loader;

import java.net.URL;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import stork.dls.ad.Ad;
import stork.dls.config.DLSConfig;
import stork.dls.inmemcache.MemoryCache;
import stork.dls.io.local.DBCache;
import stork.dls.rest.RestInterface;

public class DLSloader implements ServletContextListener{

	public void contextDestroyed(ServletContextEvent arg0) {
	}
	public void contextInitialized(ServletContextEvent arg0) {
		String name = "DLS.conf";
		try{
			URL url = this.getClass().getClassLoader().getResource(name);
			Ad ad = Ad.parse(url.openStream());
			DBCache.BDB_PATH = ad.get("conf.BDB_PATH", DBCache.BDB_PATH);
			MemoryCache.inMemoryCache_capacity = ad.getInt("conf.inMemoryCache_capacity", MemoryCache.inMemoryCache_capacity);
			RestInterface.ip = ad.get("ip", RestInterface.ip);
			RestInterface.hostname = ad.get("hostname", RestInterface.hostname);
			DLSConfig.DLS_CONCURRENCY_STREAM = ad.getInt("conf.DLS_CONCURRENCY_STREAM", DLSConfig.DLS_CONCURRENCY_STREAM);
			DLSConfig.DLS_PIPE_CAPACITY = ad.getInt("conf.DLS_PIPE_CAPACITY", DLSConfig.DLS_PIPE_CAPACITY);
		}catch (Exception ex){
			ex.printStackTrace();
			System.out.println("fail to open DLS.conf ~!");
		}
		
		System.out.println("{{ DLS on: " + RestInterface.ip + " " + RestInterface.hostname );
		System.out.println("dls.conf.BDB_PATH: " + DBCache.BDB_PATH);
		System.out.println("MemoryCache.inMemoryCache_capacity: " + MemoryCache.inMemoryCache_capacity);
		System.out.println("DLSConfig.DLS_CONCURRENCY_STREAM: " + DLSConfig.DLS_CONCURRENCY_STREAM);
		System.out.println("DLSConfig.DLS_PIPE_CAPACITY: " + DLSConfig.DLS_PIPE_CAPACITY);
		System.out.println("init all the components }}");
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
	}

}
