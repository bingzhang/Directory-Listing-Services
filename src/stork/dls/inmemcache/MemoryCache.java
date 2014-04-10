package stork.dls.inmemcache;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.service.prefetch.PrefetchingServices;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSIOAdapter;
import stork.dls.util.DLSLogTime;
import stork.dls.util.DLSResult;

/**
 * Multi-threads concurrently synchronized/asynchronized read/write.<br>
 * If prefetch option is checked, will use {@link DLSThreadsManager} to allocate 1 {@link WorkerThread} 
 * and then start {@link PrefetchingServices}.<br><br>
 *  
 * @author bingzhan@buffalo.edu
 * 
 * @see	PrefetchingServices	
 * @see DLSThreadsManager
 * @see DLSIOAdapter
 * @see WorkerThread
 */

final public class MemoryCache {
	final boolean cacheTimeMeasureTraceFlag = false;
	final boolean cacheBehaviorTraceFlag = false;
	
	private class NonblockingCounter{
		private AtomicInteger ticketNo = new AtomicInteger(0);
		public void reset(){
			int ticket;
			do{
				ticket = ticketNo.get();
			}while(!ticketNo.compareAndSet(ticket, 0) );
		}
		public int getTicket(){
			int ticket;
			ticket = ticketNo.get();
			return ticket;
		}
		public void waitTilZero(){
			int ticket;
			do{
				ticket = ticketNo.get();
				if(0 == ticket){
					break;
				}
			}while(true);
		}
		
		public int increment(){
			int ticket;
			do{
				ticket = ticketNo.get();
			}while(!ticketNo.compareAndSet(ticket, (ticket+1)) );
			return ticket+1;
		}
		public int decrease(){
			int ticket;
			do{
				ticket = ticketNo.get();
			}while(!ticketNo.compareAndSet(ticket, (ticket-1)) );
			return ticket-1;
		}		
	}
	
	private NonblockingCounter capacity_counter = new NonblockingCounter();
	private NonblockingCounter active_counter = new NonblockingCounter();
	//for the test performance only~!
	final static int MAXIMUM_TICKET = 2000;
	final static int inMemory_MAXIMUM_TICKET = 2000;
	final static int DBCache_MAXIMUM_TICKET = 2000;
	final static int REMOTE_MAXIMUM_TICKET = 2000;
	final int 	inMemoryCacheaccess_Threads[] = new int[inMemory_MAXIMUM_TICKET];
	final int 	DBCacheaccess_Threads[] = new int[inMemory_MAXIMUM_TICKET];
	final int 	Network_Threads[] = new int[inMemory_MAXIMUM_TICKET];//ms
	final long 	register_time_measure[] = new long[inMemory_MAXIMUM_TICKET];//nanos
	final long inMemoryCache_time_measure[] = new long[inMemory_MAXIMUM_TICKET];//nanos
	final long DBCache_time_measure[] = new long[DBCache_MAXIMUM_TICKET];//ms
	final long Network_time_measure[] = new long[REMOTE_MAXIMUM_TICKET];//ms	
	// for the test performance only~!
	private final ReadWriteLock spinlock = new ReentrantReadWriteLock();
	private final static long ELAPSE_TIME = 0;//120000;//2 minutes//"00:02:00:00";
	private class Sentinel{
		private final Boolean forceRefreshSyncObj = Boolean.TRUE;
		private DLSLogTime last_forceRefreshSyncTime = null;
		private String forceRefreshCacheEntry = null;
		AtomicReference<String> cache_entry_reference = new AtomicReference<String>(null);
	}
	private final float inMemoryCache_loadFactor = 1.0f;
	//private final int inMemoryCache_capacity = (1<<10);
	public static int inMemoryCache_capacity = (1<<10);
	private final HashMap<String, Sentinel> cacheSentinelMap = new HashMap<String, Sentinel>(inMemoryCache_capacity, inMemoryCache_loadFactor);
	
	public int getCacheEntriesNum(){
	    return capacity_counter.getTicket();
	}
	
	
	public void inMemoryCache_CLeanUp(long threadID){
		active_counter.waitTilZero();
		while(true){
			if(cacheBehaviorTraceFlag){
				System.out.println("\nthread ("+ threadID + ") trying to do in-Memory Cache cleanUP~!");
			}
			if(spinlock.writeLock().tryLock()){
				if(capacity_counter.getTicket() > inMemoryCache_capacity){
					try{
						cacheSentinelMap.clear();
						capacity_counter.reset();
						if(cacheBehaviorTraceFlag){
							System.out.println("thread ("+ threadID + ") did in-Memory Cache cleanUP~!");
						}
					}finally{
						spinlock.writeLock().unlock();
					}
				}else{	
					if(cacheBehaviorTraceFlag){
						System.out.println("thread ("+ threadID + ") has no chance to do in-Memory Cache cleanUP~!");
					}
					spinlock.writeLock().unlock();
				}
				break;
			}
		}
	}
	
	private boolean register(long threadID, DLSListingTask fetchingtask){
		boolean iRet = false;
		final String cachekey = fetchingtask.getCachekey();
		DLSLogTime curNonblocingCachePerformanceTime1 = new DLSLogTime();
		int states = 1;
		while(true){
			Sentinel sentinel = null;
			if(spinlock.readLock().tryLock()){
				try{
					sentinel = cacheSentinelMap.get(cachekey);
					if(null == sentinel){
						states = 1;
					}else{
						states = 2;
					}
				}finally{
					if(1 == states){
						sentinel = cacheSentinelMap.get(cachekey);
						if(null == sentinel){
							spinlock.readLock().unlock();
							while(true){
								if(spinlock.writeLock().tryLock()){
									try{
										sentinel = cacheSentinelMap.get(cachekey);
										if(null == sentinel){
											int try_capacity = capacity_counter.increment();//
											if(try_capacity > inMemoryCache_capacity){
												iRet = true;
												int activecounter = active_counter.getTicket();
												System.out.println("thread" + threadID +" saw the capacity full and active counter is"+ activecounter+ "\n");												
												break;
											}
											final Sentinel sentinel_true = new Sentinel();
											cacheSentinelMap.put(cachekey, sentinel_true);
										}
									}finally{
										states = 2;
										spinlock.writeLock().unlock();
									}
									break;
								}
							}
						}		
					}else{
						spinlock.readLock().unlock();
					}
				}//finally
				break;				
			}
		}
		if(cacheTimeMeasureTraceFlag){
			long elapsetime = curNonblocingCachePerformanceTime1.timeElapse();
			register_time_measure[(int) threadID] += elapsetime;
			fetchingtask.inmemCacheElapseTime += elapsetime;
		}
		return iRet;
	}
		
	//public String read(long threadID, String serverName, String pathEntry, URI uri, DLSProxyInfo dlsproxy, 
			//boolean enablePrefetch, boolean forceRefresh, String proxyCertContent, String token){
	public String read(long threadID, DLSListingTask fetchingtask, String token) throws Exception {
		if(true == fetchingtask.isForceRefresh()){
			return forceRefresh(threadID, fetchingtask, token);
		}else{
			if(cacheBehaviorTraceFlag){
				System.out.println("Thread: "+ threadID + "\t reading inMemory Cache\n");		
			}
			if(cacheTimeMeasureTraceFlag){
				inMemoryCacheaccess_Threads[(int) threadID] += 1;
			}
			active_counter.increment();
			return asynchronizeRead(threadID, fetchingtask, token);
		}
	}
	
	private String forceRefresh(long threadID, DLSListingTask fetchingtask, String token)throws Exception{
		if(cacheTimeMeasureTraceFlag){
			int i = 0;
			long inMemoryCache_totaltime = 0;
			long inMemory_accessTimes = 0;
			long registerTime = 0;
			for(long elem:inMemoryCache_time_measure){
				inMemoryCache_totaltime += elem;
				inMemory_accessTimes += inMemoryCacheaccess_Threads[i];
				inMemoryCacheaccess_Threads[i] = 0;
				registerTime += register_time_measure[i];
				register_time_measure[i] = 0;
				i++;
			}
			long DBCache_totaltime = 0;
			long DBCache_accessTimes = 0;
			i = 0;
			for(long elem:DBCache_time_measure){
				DBCache_totaltime += elem;
				DBCache_accessTimes += DBCacheaccess_Threads[i];
				DBCacheaccess_Threads[i] = 0;
				i++;
			}
			i = 0;
			long Network_totaltime = 0;
			long Network_accessTimes = 0;
			for(long elem:Network_time_measure){
				Network_totaltime += elem;
				Network_accessTimes += Network_Threads[i];
				Network_Threads[i] = 0;
				i++;
			}
			long totaltime = inMemoryCache_totaltime + DBCache_totaltime + Network_totaltime;
			System.out.println("\nregister takes (" + registerTime + ") ms\n");
			System.out.println("<<DLS cache access times is: " + inMemory_accessTimes);
			System.out.println("When hashMap size is: " + cacheSentinelMap.size() + ", " 
					+ "inMemoryCache accessTimes is (" +inMemory_accessTimes + ") and use (" + inMemoryCache_totaltime +  ") ms; avg is "+ inMemoryCache_totaltime*1.0f/inMemory_accessTimes+"\n"
					+ "LocalDBCache accessTimes is (" + DBCache_accessTimes + ") and use (" + DBCache_totaltime +  ") ms; avg is " + DBCache_totaltime*1.0f/DBCache_accessTimes + "\n"
					+ "Network accessTimes is (" + Network_accessTimes + ") and use (" + Network_totaltime +  ") ms; avg is " + Network_totaltime*1.0f/Network_accessTimes + "\n"
					+ "the total nonblocking cache use (" + totaltime + ") ms >>\n\n");
			for(i =0; i < MAXIMUM_TICKET; i++){
				inMemoryCache_time_measure[i] = 0;
				DBCache_time_measure[i] = 0;
				Network_time_measure[i] = 0;
			}
		}
		final boolean enablePrefetch = fetchingtask.isEnablePrefetch();
		
		active_counter.increment();
		register(threadID, fetchingtask);
		String result = null;
		final String cachekey = fetchingtask.getCachekey();
		final Sentinel curSentinel = cacheSentinelMap.get(cachekey);
		Boolean forceRefreshSyncObj = curSentinel.forceRefreshSyncObj;
		active_counter.decrease();		
		synchronized(forceRefreshSyncObj){
			DLSLogTime curThreadTime = new DLSLogTime();
			long curMilsec = curThreadTime.getTime();
			boolean isForceRefresh = true;
			if(null != curSentinel.last_forceRefreshSyncTime){
				long lastMilsec = curSentinel.last_forceRefreshSyncTime.getTime();				
				if( (curMilsec - lastMilsec) < ELAPSE_TIME){
					isForceRefresh = false;
				}
			}
			if(isForceRefresh){
				DLSIOAdapter ioAdapter = null;
				ioAdapter = new DLSIOAdapter();
				DLSResult dlsresult = new DLSResult(fetchingtask.getFethchingPath(), enablePrefetch, fetchingtask.getServerName());
				result = ioAdapter.getDirectoryContents("defaultThread", fetchingtask, dlsresult, -1, token);
				if(null == result){
					return null;
				}
				curSentinel.forceRefreshCacheEntry = result;
				while(true){
					AtomicReference<String> curCacheEntry = curSentinel.cache_entry_reference;
					String cacheEntry_volatile = curCacheEntry.get();
					if(true == curCacheEntry.compareAndSet(cacheEntry_volatile, curSentinel.forceRefreshCacheEntry)){
						break;
					}
				}
				curSentinel.last_forceRefreshSyncTime = new DLSLogTime();
				System.out.println(cachekey + "\tfinished forceRefresh~!");
				
				if(enablePrefetch){
					prefetch_service(dlsresult, fetchingtask, token);
				}
			}else{
				result = curSentinel.forceRefreshCacheEntry;
				System.out.println(cachekey + "\tforceRefresh the same CacheEntry within ELAPSE_TIME~!");
			}
		}
		return result;
	}	
	
	private void asynchronizeWrite(long threadID, DLSListingTask fetchingtask, String result, String token) throws Exception{	
		String cachekey = fetchingtask.getCachekey();
		active_counter.increment();
		if(cacheBehaviorTraceFlag){
			System.out.println("Thread: "+ threadID + "\t writing inMemory Cache\n");
		}
		boolean isFull = register(threadID, fetchingtask);
		if(true == isFull){
			active_counter.decrease();
			inMemoryCache_CLeanUp(threadID);
			active_counter.increment();
			asynchronizeRead(threadID, fetchingtask, token);
			return;
		}
		
		final Sentinel curSentinel = cacheSentinelMap.get(cachekey);
		while(true){
			AtomicReference<String> curCacheEntry = curSentinel.cache_entry_reference;
			String cacheEntry_volatile = curCacheEntry.get();
			if(curCacheEntry.compareAndSet(cacheEntry_volatile, result)){
				break;
			}
		}
		active_counter.decrease();
		if(cacheBehaviorTraceFlag){
			System.out.println("Thread: "+ threadID + "\t return to Client\n");
		}
		return;
	}
	
	void prefetch_service(DLSResult dlsresult, DLSListingTask fetchingtask, String token) throws Exception{
		List<String> prefetchSubdirPathLst = dlsresult.getPrefetchSubdirPath();
		if(0 < prefetchSubdirPathLst.size()){
			final String prefetchingStartPath = fetchingtask.getFethchingPath();
			final URI uri = fetchingtask.getUri();
			final String prefetch_protocol = uri.getScheme();
			final String threadGroupID = prefetch_protocol + " " + prefetchingStartPath;
			PrefetchingServices prefetcher = null;
			try{
				prefetcher = new PrefetchingServices(threadGroupID, prefetchSubdirPathLst, fetchingtask, token);
				// FIXME: There will be an exception if the thread is already started.
				WorkerThread wt = DLSThreadsManager.Resources.reserveSingle(threadGroupID, prefetcher);
				wt.setPriority(Thread.MIN_PRIORITY);
				wt.start();
			}catch (Exception ex){
				//throw ex;
				return;
			}
		}
	}
	
	private String asynchronizeRead(long threadID, DLSListingTask fetchingtask, String token) throws Exception{
		String result = null;
		final String serverName = fetchingtask.getServerName();
		final String pathEntry = fetchingtask.getFethchingPath();
		final String cachekey = serverName+"@"+pathEntry;
		DLSLogTime curNonblocingCachePerformanceTime1 = new DLSLogTime();
		boolean isFull = register(threadID, fetchingtask);
		if(isFull){
			active_counter.decrease();
			inMemoryCache_CLeanUp(threadID);
			active_counter.increment();
			return asynchronizeRead(threadID, fetchingtask, token);
		}
		if(cacheTimeMeasureTraceFlag){
			long elapsetime = curNonblocingCachePerformanceTime1.timeElapse();
			inMemoryCache_time_measure[(int) threadID] += elapsetime;
			fetchingtask.inmemCacheElapseTime += elapsetime;
		}
		final Sentinel curSentinel = cacheSentinelMap.get(cachekey);
		while(true){
			curNonblocingCachePerformanceTime1 = new DLSLogTime();
			AtomicReference<String> curCacheEntry = curSentinel.cache_entry_reference;
			String cacheEntry_volatile = curCacheEntry.get();
			if(null == cacheEntry_volatile){
				synchronized(curSentinel){
					cacheEntry_volatile = curCacheEntry.get();
					if(null != cacheEntry_volatile){
						active_counter.decrease();
						if(cacheBehaviorTraceFlag){
							System.out.println("Thread: "+ threadID + "\t return to Client\n");
						}
						if(cacheTimeMeasureTraceFlag){
							long elapsetime = curNonblocingCachePerformanceTime1.timeElapse();
							inMemoryCache_time_measure[(int) threadID] += elapsetime;
							fetchingtask.inmemCacheElapseTime += elapsetime;
						}
						return cacheEntry_volatile;
					}
					active_counter.decrease();
					if(cacheBehaviorTraceFlag){
						System.out.println("Thread: "+ threadID + "\t talking to DB cache\n");
					}
					
					final boolean enablePrefetch = fetchingtask.isEnablePrefetch();
					DLSIOAdapter ioAdapter = new DLSIOAdapter();
					DLSResult dlsresult = new DLSResult(pathEntry, enablePrefetch, fetchingtask.getServerName());
					result = ioAdapter.getDirectoryContents("fetchingThread", fetchingtask, dlsresult, -1, token);
					if(enablePrefetch){
						prefetch_service(dlsresult, fetchingtask, token);
					}

					if(cacheTimeMeasureTraceFlag){
						DLSLogTime curNonblocingCachePerformanceTime2 = new DLSLogTime();
						if(ioAdapter.accesstype()){
							Network_Threads[(int) threadID] += 1;
							Network_time_measure[(int) threadID] += curNonblocingCachePerformanceTime2.getTime() - curNonblocingCachePerformanceTime1.getTime();							
						}else{
							DBCacheaccess_Threads[(int) threadID] += 1;
							DBCache_time_measure[(int) threadID] += curNonblocingCachePerformanceTime2.getTime() - curNonblocingCachePerformanceTime1.getTime();							
						}
					}
					ioAdapter = null;
					if(null != result){
						curNonblocingCachePerformanceTime1 = new DLSLogTime();
						asynchronizeWrite(threadID, fetchingtask, result, token);
						if(cacheTimeMeasureTraceFlag){
							long elapsetime = curNonblocingCachePerformanceTime1.timeElapse();
							inMemoryCache_time_measure[(int) threadID] += elapsetime;
							fetchingtask.inmemCacheElapseTime += elapsetime;
						}
					}
					return result;
				}
			}else{
				active_counter.decrease();
				result = cacheEntry_volatile;
				if(cacheBehaviorTraceFlag){
					System.out.println("Thread: "+ threadID + "\t return to Client\n");
				}
				if(cacheTimeMeasureTraceFlag){
					long elapsetime = curNonblocingCachePerformanceTime1.timeElapse();
					inMemoryCache_time_measure[(int) threadID] += elapsetime;
					fetchingtask.inmemCacheElapseTime += elapsetime;
				}
				return result;
			}
		}	
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
