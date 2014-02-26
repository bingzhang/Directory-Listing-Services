package stork.dls.service.prefetch;

import stork.dls.client.DLSClient;


/**
 * the system basic computation unite
 * @author bing
 *
 */
public class WorkerThread extends Thread{
	public static boolean DEBUG_PRINT;//false;//true
	private Runnable target = null;
	private String assignedGroup = null;
	
	public void setTask(Runnable task, String assignedGroup){
		this.target = task;
		this.assignedGroup = assignedGroup;
	}
	public String getAssignedGroupName(){
		return this.assignedGroup;
	}
	
	@Override
	public void run(){
		if(this.target != null){
			this.target.run();
		}
	}

	public void interrupt() {
		interrupt(true);
	} public void interrupt(boolean immediate) {
		super.interrupt();
		if(null == this.assignedGroup){
			if(DEBUG_PRINT){
				System.out.println("stop duplicate interrupt");
			}
		}else{
			if(DEBUG_PRINT){
				System.out.println(this.getName()+ " interrupt and discard by "+ this.assignedGroup);
			}
			String threadGroupID = this.assignedGroup;
			this.assignedGroup = null;
			if(null != this.target){
				this.target = null;
			}
			if(immediate){
				DLSThreadsManager.Resources.releaseSingle(threadGroupID, this);
			}
		}
	}
}
