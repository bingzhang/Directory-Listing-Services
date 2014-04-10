package stork.dls.service.prefetch;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import stork.dls.service.prefetch.PrefetchingService.PrefetchingVM;
import stork.dls.stream.DLSIOAdapter;
import stork.dls.stream.DLSIOAdapter.FETCH_PREFETCH;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSProxyInfo;
import stork.dls.stream.DLSStream;
import stork.dls.util.DLSLogTime;
import stork.dls.util.DLSResult;
import stork.dls.util.XMLString;

/**
 * prefetch 1 layer subdirs files into local DB<br>
 * <p>modification:<br> <br>
 * 	1.) For every fetching request with prefetching option checked, DLS will
 * put it into the same queue if there is such kind of service running, or will initiate a prefetching
 * service.<br>
 * 	2.) Note, the same "StreamKey" prefetching requests will have at most 1 queue
 * 
 * 
 * </p>
 * 
 * @author bingzhan@buffalo.edu (Bing Zhang) threadName: StreamKey+rootPath
 * 
 * @see DLSThreadsManager
 * @see MyThreadPoolExecutor
 * 
 */

public class PrefetchingServices implements Runnable {
	public static boolean DEBUG_PRINT;// = true;//false;//true
	static final boolean THREADS_RESOURCE = false;
	private final static Boolean prefetchTimeMeasure = new Boolean(true);
	private final String prefetchingThreadID;
	private final URI uri;
	private final DLSProxyInfo dlsproxy;
	private final String root_path;
	private final List<String> source;
	private final String protocol;
	private final String proxyCertContent;
	private final String token;
	private final String StreamKey;
	private final DLSListingTask fetchingtask;
	private int SAFE_MAX_WORKERTHREADS;
	private int MAXIMUM_WORKERTHREADS;

	private final static PrefetchingService prefetchingServicesVMs = new PrefetchingService();
	private PrefetchingVM prefetchingVM;
	/**
	 * 
	 * @param input
	 *            paths
	 * @param path
	 *            root path
	 * @param uri
	 * @param proxyCertContent
	 * @param token
	 * @throws Exception
	 */
	public PrefetchingServices(String prefetchingThreadID, List<String> source, DLSListingTask fetchingtask, String token)
			throws Exception {
	    this.fetchingtask = fetchingtask;
		this.prefetchingThreadID = prefetchingThreadID;
		this.dlsproxy = fetchingtask.getDlsproxy();
		this.source = source;
		this.root_path = fetchingtask.getFethchingPath();
		this.uri = fetchingtask.getUri();
		this.proxyCertContent = fetchingtask.getProxy();
		this.token = token;
		//if you read it from local DB, there is no assignedStream associate with it.
		this.StreamKey = fetchingtask.getServiceID();
		this.protocol = uri.getScheme();
	}

	public void run() {
		boolean iMaster = prefetchingServicesVMs.createPrefetchingService(StreamKey);
		prefetchingVM = prefetchingServicesVMs.getConfig(StreamKey);
		//1.) check memory if there is the same "StreamKey" prefecthing service running.
		if(!iMaster){
			System.out.println("slave vmID: " + StreamKey);
			boolean already = prefetchingVM.checkIn(root_path);
			//1.1) filter if there is the same "root_path" under the processing. Just return.
			if(already){
				return;
			}
			System.out.println("salve submit prefetching request");
		}else{//2.) else 
			DLSStream streamStat = null;
			try {
				streamStat = DLSIOAdapter.initStream(fetchingtask, uri, dlsproxy, this.proxyCertContent);
				MAXIMUM_WORKERTHREADS = streamStat.get_MAX_NUM_OPENStream();
				//String protocol = streamStat.get_realprotocol();
				String protocol = "no specfic protocol";
				if(protocol.equals("gsiftpdls")){
					SAFE_MAX_WORKERTHREADS = MAXIMUM_WORKERTHREADS;
				}else{
					SAFE_MAX_WORKERTHREADS = (int) Math
						.sqrt((double) MAXIMUM_WORKERTHREADS);
				}
				prefetchingVM.setSAFE_MAX_WORKERTHREADS(this.SAFE_MAX_WORKERTHREADS);
				prefetchingVM.setMAXIMUM_WORKERTHREADS(this.MAXIMUM_WORKERTHREADS);
			} catch (Exception e) {
				System.out.println("\nPrefetchingServices: " + this + " failed~!");
				return;
			}
		}
		//submission of prefetch worker.
		PrefetchingWorkers prefetchWorker = new PrefetchingWorkers(root_path, source, 
				prefetchingVM.getMAXIMUM_WORKERTHREADS(), prefetchingVM.getSAFE_MAX_WORKERTHREADS());
		prefetchingVM.submit(prefetchWorker);
		
		if(iMaster){
			for(;;){
				while(!(prefetchingVM.isTerminate())){
					prefetchWorker = prefetchingVM.peek();
					prefetchWorker.start();
					prefetchingVM.checkOut(prefetchWorker.getrootpath());
				}
				boolean isTerminated = prefetchingServicesVMs.tryTerminatePrefetchingVM(StreamKey);
				if(isTerminated){
					DLSThreadsManager.Resources.finalAll(prefetchingThreadID);
					break;
				}
			}
		}
	}

	/**
	 * @author bing a PrefetchingWorkers object uses a group of threads
	 */
	class PrefetchingWorkers {
		private int currentPoolSize;
		private final int SAFE_MAX_WORKERTHREADS;
		private final int MAXIMUM_WORKERTHREADS;
		private final String assignedThreadGroupName;
		private final Throttle throttle;
		private final List<String> pathList;
		private final String fetchingpath;
		private ThreadFactory threadFactory;
		private MyThreadPoolExecutor executor;
		
		private class Throttle {
			private int continuousReceived = 0;
			private final static double oversubscribtion_factor = 0.2; // 20%
			private int OVERSUBSCRIBTION = (int) (MAXIMUM_WORKERTHREADS * (1 + oversubscribtion_factor));
			private double inertia = 0.0f;

			private synchronized void augment() {
				int old = ++continuousReceived;
				if (continuousReceived >= currentPoolSize
						&& currentPoolSize < MAXIMUM_WORKERTHREADS) {
					stork.dls.stream.DLSIOAdapter.StreamInfo result = null;
					DLSIOAdapter ioAdapter = null;
					ioAdapter = new DLSIOAdapter();
					try {
					    DLSListingTask listingtask = new DLSListingTask(uri);
						result = ioAdapter.StreamSnapshot(listingtask);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						ioAdapter = null;
					}
					int waitCount = result.waited;
					int activeCount = result.available;
					if (DEBUG_PRINT) {
						System.out.println("Stream Snapshot:\t" + "wait: "
								+ waitCount + "; active: " + activeCount);
						System.out.println(assignedThreadGroupName
								+ ": core pool size: "
								+ executor.getCorePoolSize()
								+ ": Max pool size: "
								+ executor.getMaximumPoolSize() + " ~!");
					}
					if (waitCount >= OVERSUBSCRIBTION) {
						int oldcontinuousReceived = continuousReceived;
						shrink();
						if (oldcontinuousReceived > 1) {
							continuousReceived = ((OVERSUBSCRIBTION - waitCount) * waitCount)
									/ oldcontinuousReceived;
							if (DEBUG_PRINT) {
								System.out.println(assignedThreadGroupName
										+ "new continuousReceived is: "
										+ continuousReceived + "; old is: "
										+ old + "; pool size: "
										+ executor.getCorePoolSize() + " ~!");
							}
						}
					} else {
						int trySize = currentPoolSize + SAFE_MAX_WORKERTHREADS;
						trySize = Math.min(trySize, MAXIMUM_WORKERTHREADS);
						if (DEBUG_PRINT) {
							System.out.println(assignedThreadGroupName
									+ "setCorePoolSize " + currentPoolSize
									+ " to " + trySize + " ? ~!");
						}
						executor.setMaximumPoolSize(trySize);
						executor.setCorePoolSize(trySize);
						int setedSize = executor.getCorePoolSize();
						if (trySize == setedSize) {
							if (DEBUG_PRINT) {
								System.out.println(assignedThreadGroupName
										+ " augment threadingPool size from "
										+ currentPoolSize + " to " + setedSize
										+ " ~!");
							}
							currentPoolSize = setedSize;
							continuousReceived = 0;// reset to ``zero''
						}
					}
				}

			}

			private synchronized void shrink() {
				int trySize = currentPoolSize >> 1;
				trySize = Math.max(trySize, 1);
				executor.setCorePoolSize(trySize);
				executor.setMaximumPoolSize(trySize);
				int setedSize = executor.getCorePoolSize();
				continuousReceived = 0;
				if (trySize == setedSize) {
					if (DEBUG_PRINT) {
						System.out.println(assignedThreadGroupName
								+ "\tshrink threadingPool size from\t"
								+ currentPoolSize + "\tto\t" + setedSize);
					}
					currentPoolSize = setedSize;
				}
			}
		}

		private class PrefetchingFactory implements ThreadFactory {
			private int iThreadNum = 0;

			public Thread newThread(Runnable r) {
				iThreadNum++;
				Thread t = DLSThreadsManager.Resources.reserveSingle(
						assignedThreadGroupName, r);
				if (DEBUG_PRINT || THREADS_RESOURCE) {
					if (THREADS_RESOURCE) {
						if (iThreadNum > MAXIMUM_WORKERTHREADS) {
							System.out.println("why?");
						}
					}
				}
				if (t == null) {
					if (DEBUG_PRINT) {
						System.out.println("threads out of resources~!");
					}
				}
				return t;
			}
		}

		PrefetchingWorkers(String fetchingpath, List<String> pathList, int max, int safe) {
			StringBuffer sb = new StringBuffer();
			sb.append(StreamKey);
			this.fetchingpath = fetchingpath;
			this.pathList = pathList;
			assignedThreadGroupName = sb.toString();
			int size = pathList.size();
			if(size < safe){
				this.SAFE_MAX_WORKERTHREADS = size;
				this.MAXIMUM_WORKERTHREADS = size;
			}else {
				this.SAFE_MAX_WORKERTHREADS = safe;
				this.MAXIMUM_WORKERTHREADS = max;
			}
			this.currentPoolSize = SAFE_MAX_WORKERTHREADS;
			throttle = new Throttle();
		}

		public String getrootpath() {
			return this.fetchingpath;
		}

		private List<String> scanForPathInfo(String source) {
			return XMLString.helpToScan(source);
		}
		
		private String prefetcher(String rootPath) {
			String result = null;
			String thisThreadName = Thread.currentThread().getName();
			// final String assignedThreadName = this.assignedThreadGroupName
			// +"@"+thisThreadName;
			final String assignedThreadName = thisThreadName;
			// System.out.println(this.assignedThreadGroupName
			// +"\tuses Thread\t"+ thisThreadName
			// +"\tto call getDirectoryContents~!");
			DLSIOAdapter ioAdapter = null;
			//DLSResult dlsresult = new DLSResult(rootPath, true);
			DLSResult dlsresult = new DLSResult(rootPath, false, uri.getHost());
			DLSProxyInfo dlsproxy1 = dlsproxy;
			ioAdapter = new DLSIOAdapter(FETCH_PREFETCH.PREFETCH);
			String protocol_test = uri.getScheme();
			URI newuri = null;
			try {
				newuri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), rootPath, uri.getQuery(), uri.getFragment());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			DLSListingTask fetchingtask1 = new DLSListingTask(0, newuri, dlsproxy1, false, false, proxyCertContent, null, null);
			try {
				result = ioAdapter.getDirectoryContents(assignedThreadName, fetchingtask1, dlsresult, -1, token);
			} catch (Exception e) {
				dlsresult.clear();
				dlsresult = null;
				e.printStackTrace();
			} finally {
				ioAdapter = null;
			}
			throttle.augment();
			// System.out.println(assignedThreadGroupName +
			// "\talready continuously received without any congestion\t"+
			// combokills+"\trequests~!");
			//result = null;
			
			/*
			//prefetch next layer
			if (null != dlsresult) {
				List<String> prefetchSubdirPathLst = dlsresult.getPrefetchSubdirPath();
				if (0 < prefetchSubdirPathLst.size()) {
					PrefetchingWorkers prefetching = new PrefetchingWorkers(rootPath);
					prefetching.start(prefetchSubdirPathLst);
				}
			}*/

			return result;
		}

		private void start() {
			this.threadFactory = new PrefetchingFactory();
			this.executor = PrefetchThreadPool();
			this.executor.setName(assignedThreadGroupName);
			/**
			 * for debuging, sort the result.
			 */
			// Collections.sort(pathList);
			DLSLogTime st = new DLSLogTime();
			long starttime = st.getTime();
			System.out.println("prefetch path:\t" + this.fetchingpath+ 
					"\tstarts at:\t" + st.toString()
					+ " : " + starttime +" ms");
			System.out.println(".....");
			for (final String subdir : pathList) {
				executor.execute(new Runnable() {
					public void run() {
						String prefetchSubdir = root_path+subdir+"/";
						prefetcher(prefetchSubdir);
					}
				});
			}
			executor.shutdown();
			while (!executor.isTerminated()) {}
			if (prefetchTimeMeasure.booleanValue()) {
				DLSLogTime ed = new DLSLogTime();
				long endtime = ed.getTime();
				long prefetchElapse = endtime - starttime;
				System.out.println("Time Test: " + assignedThreadGroupName 
						+ "  " + this.fetchingpath
						+ "\tprefetch time ends at:\t" + st.toString()
						+ " : " + endtime +" ms"
						+ ".\t prefetchElapse is: " + prefetchElapse + " ms");
			}
			DLSThreadsManager.Resources.finalAll(assignedThreadGroupName);
		}

		private class prefetcherCallerRuns extends
				MyThreadPoolExecutor.CallerRunsPolicy {
			public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
				throttle.shrink();
				if (DEBUG_PRINT) {
					System.out.println("reject~!");
				}
				if (!e.isShutdown()) {
					r.run();
				}
			}
		}

		private MyThreadPoolExecutor PrefetchThreadPool() {
			int corePoolSize = this.SAFE_MAX_WORKERTHREADS;
			currentPoolSize = corePoolSize;
			int maximumPoolSize = this.SAFE_MAX_WORKERTHREADS;
			return new MyThreadPoolExecutor(
					corePoolSize,
					maximumPoolSize,
					0L,
					TimeUnit.MILLISECONDS,
					new LinkedBlockingQueue<Runnable>(2 * this.MAXIMUM_WORKERTHREADS/*-SAFE_MAX_WORKERTHREADS*/),
					this.threadFactory, new prefetcherCallerRuns()) {
				// only for debug the framework
				@Override
				public Future<?> submit(Runnable task) {
					if (task == null)
						throw new NullPointerException();
					RunnableFuture<Object> ftask = newTaskFor(task, null);
					execute(ftask);
					return ftask;
				}
			};
		}
	}

	/**
	 * @author bing
	 * @param <T>
	 *            use 2 threads.
	 */

	private class PrefetchingStream<T> {
		private List<? super T> data = new ArrayList<T>();
		private final SynchronousQueue<List<? super T>> resultSets = new SynchronousQueue<List<? super T>>();
		private final ExecutorService executor = Executors
				.newFixedThreadPool(2);

		void startStream(List<? super T> input) {
			data = input;
			List<Future<Void>> futures = null;
			try {
				futures = executor.invokeAll(Arrays.asList(producer, consumer));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (final Future<Void> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}

		Callable<Void> producer = new Callable<Void>() {
			public Void call() throws Exception {
				try {
					if (!resultSets.offer(data)) {
						throw new Exception("hihi, I do not know~!\n");
					}
				} catch (InterruptedException ex) {
					throw ex;
				}
				return null;
			}
		};

		Callable<Void> consumer = new Callable<Void>() {
			public Void call() throws Exception {
				try {
					List<? super T> result = resultSets.poll();
					// talk to DLSIOAdaptor
					if (null == result) {
						throw new Exception("hihi, result is null~!\n");
					}
				} catch (InterruptedException ex) {
					throw ex;
				}
				return null;
			}

		};
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
