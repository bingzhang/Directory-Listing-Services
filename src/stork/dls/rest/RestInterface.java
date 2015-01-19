package stork.dls.rest;

import java.io.File;
import java.io.FileWriter;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;

import com.sleepycat.je.DatabaseException;
import com.sun.jersey.multipart.*;

import stork.dls.ad.Ad;
import stork.dls.inmemcache.MemoryCache;
import stork.dls.io.local.DBCache;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.stream.DLSIOAdapter;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSProxyInfo;
import stork.dls.util.DLSLogTime;

@Path("/dls")
@Produces("application/json")
public class RestInterface {
	// URL information for the current request.
	@Context UriInfo ui;
	//time test to calculate # users DLS response time
	public static boolean TIME_TEST = false;
	private static double user10[] = new double[10];
	private static double user100[] = new double[100];
	private static double user1k[] = new double[1000];
	
    private final boolean enableDOMount = true;
    private final Boolean EnableFtpSitesRoot = new Boolean(true);
	
	private final static boolean enableLog = false;
	private static MemoryCache concurrentCache;
	public static String ip = "127.0.0.1";
	public static String hostname = "localhost";
	/**
	 * init/start the singleton in-Memory cache.
	 */
	public static void INIT() throws DatabaseException{
		concurrentCache =  new MemoryCache();
	}
		
	final static int MAXIMUM_TICKET = 2000;
	static final long DLS_time_measure[] = new long[MAXIMUM_TICKET];//ms
	
	// Special utility method for marshaling multivalued maps.
	// TODO: Remove this when ads support convenient conversion between
	// composite and singular types.
	public static <K,V> Ad mvMapToAd(Map<K,List<V>> m) {
		Ad ad = new Ad();
		for (Entry<K, List<V>> e : m.entrySet())
			ad.put(e.getKey(), e.getValue().get(0));
		return ad;
	}

	// The same thing, but with multipart form data instead.
	public static Ad formPartsToAd(FormDataMultiPart mp) {
		Map<String, List<FormDataBodyPart>> m = mp.getFields();
		Ad ad = new Ad();
		for (Entry<String, List<FormDataBodyPart>> e : m.entrySet())
			ad.put(e.getKey(), e.getValue().get(0).getValueAs(String.class));
		return ad;
	}
	
	// NOTE: These three methods let us have a lot more flexibility in how
	// we communicate with the service.
	
	// Handles GE)T requests with parameters in the query string.
	@GET
	@Path("/list")
	public Response handleGet() {
		return response(mvMapToAd(ui.getQueryParameters()));
	}

	@POST
	@Path("/list")
	@Consumes("multipart/form-data")
	public Response handleMultipartPost(FormDataMultiPart form) {
		return response(formPartsToAd(form));
	}
	
	// Handles POST requests with a URL encoded request body.
	@POST
	@Path("/list")
	@Consumes("application/x-www-form-urlencoded")
	public Response handleFormPost(MultivaluedMap<String, String> form) {
		return response(mvMapToAd(form));
	}
	/*
	// Handles POST requests with a multipart request body.
	@POST
	@Path("/list")
	@Consumes("multipart/form-data")
	public Response handleMultipartPost(FormDataMultiPart form) {
		return response(formPartsToAd(form));
	}*/
	
	// Wrapper handler which intercepts exceptions and returns responses as JSON.
	private Response response(Ad ad) {
		try {
			byte[] b = responseInner(ad).toJSON().getBytes("UTF-8");
			ResponseBuilder rb = Response.ok(b);
			rb.header("Content-Length", b.length);
			return rb.build();
		} catch (Exception e) {
			//e.printStackTrace();
			String m = e.getMessage();
			Ad err = new Ad("error", (m != null) ? m : "(unknown error)");
			return Response.serverError().entity(err.toJSON()).build();
		}
	}

	// The one that handles the real response. Feel free to throw exceptions anywhere.
	private Ad responseInner(Ad ad) throws Exception {
        if(enableDOMount){
            final boolean ismount = ad.getBoolean("domount");
            if(ismount){
                Ad filead = null;
                synchronized(EnableFtpSitesRoot){
                    File file = new File(DBCache.BDB_PATH+"/ftpsites");
                    filead = file.exists() ? Ad.parse(file) : null;
                }
                return filead;
            }
        }
	    
	    
	    
		if (!ad.has("URI")){
			throw new Exception("URI parameter is missing from request");
		}
		URI uri = URI.create(ad.get("URI"));
		String tmp1 = uri.getScheme();
		uri = uri.normalize();
		//String tmp1 = url.getProtocol();
		String tmp2 = uri.getHost();
		String tmp3 = uri.getPath();
		if(null != tmp2){
		    uri = new URI(tmp1+ "://" + tmp2+tmp3);
		    //System.out.println( tmp1+ "://" + tmp2+tmp3);
		}else{
		    uri = new URI(tmp1+ ":/" + tmp3);
		    //System.out.println(tmp1+ ":/" + tmp3);
		}
		
		final boolean forceRefresh = ad.getBoolean("forceRefresh");
		final boolean enablePrefetch = ad.getBoolean("enablePrefetch");
		final String account = ad.get("account");
		final String passphrase = ad.get("passphrase");
		final String proxyserver = ad.get("proxyserver");
		final String proxy = ad.get("proxyCertificate");
		final String zone = ad.get("irodsZone");
		final String resource = ad.get("irodsDefResource");
		final String irodshost = ad.get("irodsHost");
		final String irodsHomeDirectory = ad.get("irodsHomeDirectory");
		//final String requestType = ad.get("type");
		final String cacheSize = ad.get("cacheSize");
		String result = null;
        if(null != cacheSize){
            Ad adResult = new Ad();
            result = "Cache entries num: "+ Integer.toString(concurrentCache.getCacheEntriesNum()) + " of " + MemoryCache.inMemoryCache_capacity;
            adResult.put(result);
            result  = adResult.toJSON();
            return Ad.parse(result);
        }
		
		long threadID = Thread.currentThread().getId()%MAXIMUM_TICKET;
		DLSLogTime logStart = new DLSLogTime();
		DLSProxyInfo dlsproxy = null;
		if(null == proxy){
		    dlsproxy = DLSProxyInfo.createDLSProxy(proxyserver, account, passphrase);
		}
		DLSListingTask dlsfetchingTask = new DLSListingTask(threadID, uri, dlsproxy, forceRefresh, enablePrefetch, proxy, zone, resource);
		if(null != irodshost){
			dlsfetchingTask.setiRodsHost(irodshost);
			dlsfetchingTask.setiRodsHome(irodsHomeDirectory);
		}
		result = concurrentCache.read(threadID, dlsfetchingTask, null);
		/*
		if(false == forceRefresh){
			DLS_time_measure[(int) threadID] += en - st;			
		}else{
			long DLSresponseTime = 0;
			for(int i = 0; i < MAXIMUM_TICKET; i ++){
				DLSresponseTime += DLS_time_measure[i];
				DLS_time_measure[i] = 0;
			}
			System.out.println("DLS response time total is: " + DLSresponseTime + " ~! m.s.\n");
		}*/
		if(TIME_TEST){
			final String path = dlsfetchingTask.getFethchingPath();
			if(path.contains("/home/bing/test/")){
				if(!forceRefresh){
					double elapseSec = logStart.timeElapseStr(true);
					Pattern p = Pattern.compile("[/]+");
					String[] matching = p.split(path, 7);
					/*
					if(matching.length > 0){
						System.out.println("found dir: " + matching[5]);
					}*/
					if(matching.length >= 6 && null != matching[5] && !matching[5].isEmpty()){
						final String root = matching[4];
						int dir = Integer.parseInt(matching[5]);
						if(root.equals("10")){
							user10[dir] = elapseSec;
						}else if(root.equals("100")){
							user100[dir] = elapseSec;
						}else if(root.equals("1000")){
							user1k[dir] = elapseSec;
						}	
					}
					
				}else{
					double u10 = 0.0f;
					for(double f : user10){
						u10 += f;
					}
					for(int i = 0; i < 10; i ++){
						user10[i] = 0.0f;
					}
					double u10_avg = u10/10;
		
					double u100 = 0.0f;
					for(double f : user100){
						u100 += f;
					}
					for(int i = 0; i < 100; i ++){
						user100[i] = 0.0f;
					}
					double u100_avg = u100/100;
		
					
					double u1k = 0.0f;
					for(double f : user1k){
						u1k += f;
					}
					for(int i = 0; i < 1000; i ++){
						user1k[i] = 0.0f;
					}
					double u1k_avg = u1k/1000;
					
					System.out.println("u10 avg: "+ u10_avg + "; u100 avg: " + u100_avg + "; u1k_avg: " + u1k_avg);
				}
			}
		}
		
		if(enableLog){
			String servicetimeElapse = logStart.timeElapseStr();
			DLSLogTime logEnd = new DLSLogTime();
			final String logPrefix = "<<Log@";
			StringBuffer dlsLogStr = new StringBuffer(logPrefix);
			dlsLogStr.append(dlsfetchingTask.getUri());
			dlsLogStr.append(";\trecevied: ");
			dlsLogStr.append(logStart.timeStamp());
			dlsLogStr.append("\tfinished: ");
			dlsLogStr.append(logEnd.timeStamp());
			dlsLogStr.append(";\n\tTotal service time: ( ");
			dlsLogStr.append(servicetimeElapse);
			dlsLogStr.append("inMem-cache ( ");
			dlsLogStr.append(dlsfetchingTask.inmemCacheElapseTime);
			dlsLogStr.append(" ) ns >>");
			System.out.println(dlsLogStr);
		}
		
		if (null == result){
			throw new Exception("null returned from lower layer (unknown error)");
		}else if(enableDOMount){
            if(uri.getScheme().equals("ftp") && tmp3.equals("/")){
                String host = uri.getHost();
                host = host.toLowerCase();
                synchronized(EnableFtpSitesRoot){
                    try {
                        File file = new File(DBCache.BDB_PATH+"/ftpsites");
                        Ad filead = file.exists() ? Ad.parse(file) : new Ad("name", "domount").put("dir", true).put("size", 4096).put("perm", 755).put("owner", "dls").put("group", "didclab").put("files", new Ad());
                        Ad files = filead.getAd("files", new Ad());
                        filead.put("files", files);

                        boolean existed = false;
                        for(Ad cursor: filead.getAds("files")){
                            if(cursor.get("name", "").equals(host)){
                                existed = true; break;
                            }
                        }
                        if(!existed){
                            files.put(new Ad("name", host).put("dir", true).put("size", 4096).put("perm", 755).put("owner", "ftp").put("group", "ftp"));
                        }
                        filead.put("files", files);
                        FileWriter writer = new FileWriter(DBCache.BDB_PATH+"/ftpsites");
                        writer.write(filead.toJSON());
                        writer.close();

                    } catch (Exception x) {
                        //x.printStackTrace();
                        System.err.println(x);
                    }
                }
            }
        }

		return Ad.parse(result);
	}
	
	// Testing methods
	// ---------------
	// XXX: Remove or make these private before deploying for production.
	
	// Test method for converting form parameters to an ad.
	@GET
	@Path("/test_ad")
	public String testAd() {
		return mvMapToAd(ui.getQueryParameters()).toJSON();
	}
	
	// Test method for resetting the cache.
	@GET
	@Path("/reset")
	public String reset() throws Exception {
		if(false == DLSThreadsManager.noBusy()){
			Thread.sleep(5000);
			System.out.println("some threads still working, wait 5 sec~!");
		}
		DLSThreadsManager.INIT();
		DLSIOAdapter.reset();
		concurrentCache.inMemoryCache_CLeanUp(0);
		return "DLS cache has been reset.";
	}
}