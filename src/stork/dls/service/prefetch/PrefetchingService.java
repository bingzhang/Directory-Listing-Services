package stork.dls.service.prefetch;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import stork.dls.service.prefetch.PrefetchingServices.PrefetchingWorkers;


public class PrefetchingService {
	private final ReadWriteLock spinlock = new ReentrantReadWriteLock();
	private class PrefetchingVMState{
		final private AtomicInteger ctl = new AtomicInteger(0);
		final private AtomicReference<PrefetchingVM> vm_state = new AtomicReference<PrefetchingVM>(null);
	}
	private final HashMap<String, PrefetchingVMState> dlsPrefetchingVMStateHashMap = new HashMap<String, PrefetchingVMState>();//capacity limited by the OS System.

	//interface class
	class PrefetchingVM{
		private final String vmID;
		private int SAFE_MAX_WORKERTHREADS;
		private int MAXIMUM_WORKERTHREADS;
		private BlockingQueue<PrefetchingWorkers> workQueue = new LinkedBlockingQueue<PrefetchingWorkers>();
		private PrefetchingWorkerRoster prefetchingWorkerRoster = new PrefetchingWorkerRoster();

		PrefetchingVM(String vmid){
			this.vmID = vmid;
		}
		
		public boolean checkIn(String request_path) {
			boolean already = prefetchingWorkerRoster.checkIn(request_path);
			return already;
		}
		
		public void checkOut(String request_path) {
			prefetchingWorkerRoster.checkOut(request_path);
			workQueue.poll();
			final PrefetchingVMState vmstate = dlsPrefetchingVMStateHashMap.get(vmID);
			for(;;){
				int c = vmstate.ctl.get();
				if(vmstate.ctl.compareAndSet(c, c-1)){
					break;
				}
			}
		}
		
		public boolean isTerminate() {
			return workQueue.isEmpty();
		}
		
		/**
		 * @return the streamKey
		 */
		public String getVMID() {
			return vmID;
		}

		private class PrefetchingWorkerRoster{
			private final ReadWriteLock spinlock = new ReentrantReadWriteLock();
			private HashMap<String, PrefetchingWorkers> prefetchingworkerset = new HashMap<String, PrefetchingWorkers>();
			private boolean checkIn(String workingKey){
				boolean already = false;
				for(;;){
					if(spinlock.readLock().tryLock()){
						PrefetchingWorkers old = null;
						try{
							old = prefetchingworkerset.get(workingKey);
						}finally{
							if(null != old){
								already = true;
							}
							spinlock.readLock().unlock();
						}
						break;
					}
				}
				return already;
			}
			
			private void checkOut(String workingKey){
				for(;;){
					if(spinlock.writeLock().tryLock()){
						try{
							prefetchingworkerset.remove(workingKey);
						}finally{
							spinlock.writeLock().unlock();
						}
						break;
					}
				}
			}
		}
		public void submit(PrefetchingWorkers prefetchWorker) {
			workQueue.offer(prefetchWorker);			
		}

		public PrefetchingWorkers peek() {
			return workQueue.peek();
		}

		/**
		 * @return the sAFE_MAX_WORKERTHREADS
		 */
		public int getSAFE_MAX_WORKERTHREADS() {
			return SAFE_MAX_WORKERTHREADS;
		}

		/**
		 * @return the mAXIMUM_WORKERTHREADS
		 */
		public int getMAXIMUM_WORKERTHREADS() {
			return MAXIMUM_WORKERTHREADS;
		}

		/**
		 * @param mAXIMUM_WORKERTHREADS the mAXIMUM_WORKERTHREADS to set
		 */
		public void setMAXIMUM_WORKERTHREADS(int mAXIMUM_WORKERTHREADS) {
			MAXIMUM_WORKERTHREADS = mAXIMUM_WORKERTHREADS;
		}
		
		/**
		 * @param mAXIMUM_WORKERTHREADS the mAXIMUM_WORKERTHREADS to set
		 */
		public void setSAFE_MAX_WORKERTHREADS(int sAFE_MAX_WORKERTHREADS) {
			SAFE_MAX_WORKERTHREADS = sAFE_MAX_WORKERTHREADS;
		}
	}
	
	private void register(String vmID){ 
		int states = 1;
		while(true){
			PrefetchingVMState sentinel = null;
			if(spinlock.readLock().tryLock()){
				try{
					sentinel = dlsPrefetchingVMStateHashMap.get(vmID);
					if(null == sentinel){
						states = 1;
					}else{
						states = 2;
					}
				}finally{
					if(1 == states){
						sentinel = dlsPrefetchingVMStateHashMap.get(vmID);
						if(null == sentinel){
							spinlock.readLock().unlock();
							while(true){
								if(spinlock.writeLock().tryLock()){
									try{
										sentinel = dlsPrefetchingVMStateHashMap.get(vmID);
										if(null == sentinel){
											final PrefetchingVMState sentinel_true = new PrefetchingVMState();
											dlsPrefetchingVMStateHashMap.put(vmID, sentinel_true);
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
	}
	/**
	 * 
	 * @param vmID
	 * @param cachevalue
	 * @return true: this thread is master thread; false no.
	 */
	public boolean createPrefetchingService(String vmID){
		boolean iMaster = false;
		
		//register sentinel
		register(vmID);
		//get unique sentinel
		final PrefetchingVMState thisVM = dlsPrefetchingVMStateHashMap.get(vmID);
		// broadcast current active counter
		// the master thread wants to do termination() after this sync line, will not terminate.
		// However, before this sync line, master can do termination() successfully:
		// master will do ctl.compareAndSet(c, c-1);
		PrefetchingVM newVM = new PrefetchingVM(vmID);
		for(;;){
			int c = thisVM.ctl.get();
			if(0 == c && thisVM.ctl.compareAndSet(c, c+1)){
				//this is master.
				iMaster = true;
				AtomicReference<PrefetchingVM> curVMstate = thisVM.vm_state;
				if(curVMstate.compareAndSet(null, newVM)){
					//System.out.println("master set vm_state to: " + cachevalue);
					break;
	            }
				break;
			}else if(thisVM.ctl.compareAndSet(c, c+1)){
				//this is slave.
				break;
			}
		}
		return iMaster;
	}
	
	/**
	 * triggger: workqueue.isEmpty() is true.
	 * @param vmID
	 * @return
	 */
	public boolean tryTerminatePrefetchingVM(String vmID){
		boolean iret = false;
		final PrefetchingVMState thisVM = dlsPrefetchingVMStateHashMap.get(vmID);
		if(null == thisVM){
			return false;}
		
		for(;;){
			int c = thisVM.ctl.get();
			//System.out.println("c: " + c);
			if(0 == c && thisVM.ctl.compareAndSet(c, c)){
				//successfully terminate
				iret = true;
			}
			break;
		}
		return iret;
	}
	
	public PrefetchingVM getConfig(String vmID){
		final PrefetchingVMState thisVM = dlsPrefetchingVMStateHashMap.get(vmID);
		if(null == thisVM){
			return null;}
		PrefetchingVM result = null;
		do{
			result = thisVM.vm_state.get();
			//System.out.println("slave get vm_state: "+ result);
		}while(null == result);
		
		return result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
