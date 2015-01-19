package stork.dls.io.local;

import com.sleepycat.je.*;
import com.sleepycat.persist.*;
import com.sleepycat.persist.model.*;
/*
 * bingzhan@buffalo.edu
 * JE = BDB java edition
 * 
 */

import java.io.*;
import java.util.regex.*;
/*
@Persistent
class CompositeKey {//CompositeKey
	@KeyField(1)
	String serverName;
	@KeyField(2)
	String pathEntry;
	CompositeKey() {}// for bindings
	CompositeKey(String serverName, String pathEntry) {
		this.serverName = serverName;
		this.pathEntry = pathEntry;
	}
	CompositeKey(CompositeKey obj) {
		this.serverName = obj.serverName;
		this.pathEntry = obj.pathEntry;
	}
	@Override
	public String toString() {
		return "CacheEntryKey: (" + serverName + "," + pathEntry + ")";
	}
}
@Entity
class ValueEntry {
	@PrimaryKey
	CompositeKey key;

	String metadataString;
	ValueEntry() {}// for bindings
	ValueEntry(CompositeKey key, String metadataString) {
		this.key = key;
		this.metadataString = metadataString;
	}
	@Override
	public String toString() {
		return "ValueEntry: (" + key + "," + metadataString + ")";
    }
}
*/
/**
 * very simple DB cache implementaion and related
 * IO transaction in DB
 * @author bingzhan@buffalo.edu (Bing Zhang)
 * 	
 * */
public class DBCache {
	private final static boolean SANITY_CHECK = false;
	private static boolean CLEAR_DBCACHE = false;
	Environment env = null;
	EntityStore store = null;
	DatabaseConfig dbConfig = null;
	StoreConfig storeConfig = null;
	EnvironmentConfig envConfig = null;
	PrimaryIndex<CompositeKey, ValueEntry> primaryIndex = null;
	public static final String NoEntry = "Metadata entry does not exist";
	//public static final String BDB_PATH = "/home/ubuntu/DLS/BDB/";
	public static String BDB_PATH = "/home/ubuntu/DLS/BDB/";
	static {
		System.out.println("bdb_path = "+BDB_PATH);
	}
	
	static class DBCacheConfig {
		static final boolean preload = true;
		static final boolean useDeferredWrite = true;
		static final boolean useSyncCommit = true;
		static final boolean useTxns = true;
		static final long cacheSize = 0;//(200 << 20);
		static int numThreads = 10;
	}

	// Makes a best effort attempt to delete disk cache files.
	public static void clearDBCacheFiles() {
		File[] files = new File(BDB_PATH).listFiles();
		if (files != null) for (File f : files)
			f.delete();
	}
	
	// Clear cache files and reinitialize cache.
	public synchronized void reset() throws Exception {
		boolean b = CLEAR_DBCACHE;
		init();
		CLEAR_DBCACHE = b;
	}
	
	public DBCache() throws Exception {
		init();
	}
	
	private synchronized void init() throws Exception {
		if (CLEAR_DBCACHE)
			clearDBCacheFiles();
		
		long st = System.currentTimeMillis();
		envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(DBCacheConfig.useTxns);
		envConfig.setTxnNoSyncVoid(!DBCacheConfig.useSyncCommit);
		envConfig.setCacheSize(DBCacheConfig.cacheSize);
		try{
			env = new Environment(new File(BDB_PATH), envConfig);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(DBCacheConfig.useTxns);
		store = new EntityStore(env, "DLS_Meta_Store", storeConfig);
		primaryIndex = store.getPrimaryIndex(CompositeKey.class, ValueEntry.class);
		if(DBCacheConfig.preload){
			try {
				DBCachePreload();
			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("DB preload failed~!");
			}
		}
		long en = System.currentTimeMillis();
		System.out.println("\nloading DB takes: " + (en - st) + " ms");
	}
	
	private String getNumbers(String content) {  
		Pattern pattern = Pattern.compile("\\d+");  
		Matcher matcher = pattern.matcher(content);  
		while (matcher.find()) {  
			return matcher.group(0);  
		}  
		return null;  
	}
	
    public void DBCachePreload() throws Exception {
    	int num_records = 0;
    	boolean sanityCheck[];
    	if(SANITY_CHECK){
    		sanityCheck = new boolean[10000];
    	}
        EntityCursor<ValueEntry> entities = primaryIndex.entities();
        try {
            for (ValueEntry e : entities) {
            	if(SANITY_CHECK){
	            	String checkNum = getNumbers(e.key.toString());
	            	if(null != checkNum){
	            		int indx = Integer.parseInt(checkNum);
	            		sanityCheck[indx] = true;
	            	}
            	}
            	//System.out.println(e.key.toString() + "\t" + e.metadataString);
            	num_records ++;
            }
        } finally {
        	System.out.println("DBCachePreload() scans (" + num_records + "). ~!");
            entities.close();
        }
        if(SANITY_CHECK){
	        for(int i = 0; i < 10000; i++){
	        	if(false == sanityCheck[i] ){
	        		System.out.println("/home/bing/flatten/" + i +"/; not in database~!" );
	        	}
	        }
        }
    }
	
	public String get(CompositeKey cacheKey) {
		String result;
		ValueEntry ce;
		try {
			ce = primaryIndex.get(cacheKey);
			result = ce.metadataString;

		} catch (NullPointerException e) {
			result = NoEntry;
		}
		return result;
	}

	public String Txnget(Object txn, CompositeKey cacheKey) {
		String result;
		ValueEntry ce;
		try {
			ce = primaryIndex.get((Transaction)txn, cacheKey, LockMode.DEFAULT);
			result = ce.metadataString;

		} catch (NullPointerException e) {
			result = NoEntry;
		}
		return result;
	}

	public void put(String serverName, String pathEntry, String metadataString) throws DatabaseException {
		primaryIndex.putNoReturn(new ValueEntry(new CompositeKey(serverName, pathEntry), metadataString));
	}

	public void Txnput(Object txn, String serverName, String pathEntry, String metadataString) throws DatabaseException {
		primaryIndex.putNoReturn((Transaction) txn, new ValueEntry(new CompositeKey(serverName, pathEntry), metadataString));
	}

	public boolean contains(String serverName, String pathEntry) {
		boolean result = false;
		try {
			result = primaryIndex.contains(new CompositeKey(serverName, pathEntry));
		} catch (NullPointerException e) {
			result = false;
		}
		return result;
	}
	
	public String Lookup(String serverName, String pathEntry) throws DatabaseException {
		ValueEntry ce;
		String result;
		try {
			ce = primaryIndex.get(new CompositeKey(serverName, pathEntry));
			result = ce.metadataString;

		} catch (NullPointerException e) {
			result = NoEntry;
		}
		return result;
	}	
	
	protected void finalize() throws DatabaseException, Throwable {
        try {
            if (store != null) store.close();
            if (env != null) env.close();
        } catch (DatabaseException DE) {
            System.err.println("Caught " + DE + " while closing env and store");
        }
		super.finalize();
	}

}

