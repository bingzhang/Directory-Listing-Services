package stork.dls.service.prefetch;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import stork.dls.config.DLSConfig;

interface ResourceManager{
	void releaseSingle(String threadGroupID, WorkerThread t);
	Thread reserveSingle(String threadGroupID, Runnable r);
	void finalAll(String threadGroupID);
}

public class DLSThreadsManager implements ResourceManager{
	public static boolean DEBUG_PRINT;//false;//true
	final static int MAX_THREADS = DLSConfig.DLS_MAX_THREADS;
	public static DLSThreadsManager Resources = null;
	/**
	 * init/start othe singleton DLS threads manager
	 * <br>the number of threads is MAX_THREADS
	 */
	public static void INIT(){
		if(null == Resources){
			Resources = new DLSThreadsManager(MAX_THREADS);
		}
	}
	private final static ConcurrentLinkedQueue<WorkerThread> availableThreadsPool = new ConcurrentLinkedQueue<WorkerThread >();
	private final static ConcurrentHashMap<String, LinkedList<WorkerThread>> busyThreadsPool = new ConcurrentHashMap<String, LinkedList<WorkerThread>>();
	
	DLSThreadsManager(int MAX_THREADS){
		int threadName = 0;
		final String prefixName = "preThrd-";
		for(int i = 0; i < MAX_THREADS; i ++){
			WorkerThread t = new WorkerThread();
			String threadID = String.valueOf(threadName);
			t.setName(prefixName+threadID);
			t.setPriority(Thread.MIN_PRIORITY);
			availableThreadsPool.offer(t);
			threadName++;
		}
	}
	
	public static boolean noBusy(){
		if(0 == busyThreadsPool.size()){
			return true;
		}
		return false;
	}
	
	public void releaseSingle(String threadGroupID, WorkerThread reserved){
		availableThreadsPool.add((WorkerThread) reserved);
		reserved.interrupt(false);
		LinkedList<WorkerThread> prev = busyThreadsPool.get(threadGroupID);
		prev.remove(reserved);
	}
	
	public void finalAll(String threadGroupID){
		LinkedList<WorkerThread> busy = busyThreadsPool.get(threadGroupID);
		for(WorkerThread wt: busy){
			String assignedGroupName = wt.getAssignedGroupName();
			if(/*!wt.isAlive() && */null != assignedGroupName && assignedGroupName.equals(threadGroupID)){
				wt.interrupt(false);	
			}else{
				if(DEBUG_PRINT){
					if(null != wt.getAssignedGroupName()){
						System.out.println(wt.getName()+"be re-assigned to " + wt.getAssignedGroupName() + "WRONG~!");
					}else{
						System.out.println(wt.getName()+"pre-interrupted");
					}
				}
			}
		}
		if(null != busy){
			availableThreadsPool.addAll(busy);
			busyThreadsPool.remove(threadGroupID);
		}
		if(DEBUG_PRINT){
			System.out.println(threadGroupID + " released " + busy.size() + " back~!");
		}
	}
	
	public WorkerThread reserveSingle(String threadGroupID, Runnable r) {
		WorkerThread prereserve = null;
		prereserve = availableThreadsPool.poll();
		if(null != prereserve){
			LinkedList<WorkerThread> new_ = new LinkedList<WorkerThread>();
			LinkedList<WorkerThread> prev = busyThreadsPool.get(threadGroupID);
			if(null != prev){
				new_.addAll(prev);
			}
			new_.add(prereserve);
			busyThreadsPool.put(threadGroupID, new_);
			prereserve.setTask(r, threadGroupID);
			//System.out.println("Thread: "+prereserve.getName() + "\tassigned to:\t" + threadGroupID +"~!");
		}
		return prereserve;
	}
	public static void main(String[] args) {
	}


}
