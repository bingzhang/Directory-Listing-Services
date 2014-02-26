package stork.dls.config;

import stork.dls.client.DLSClient;
import stork.dls.client.DLSGSIFTPClient;
import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.io.network.DLSMetaCmdTask;
import stork.dls.io.network.DLSSimpleTransferReader;
import stork.dls.io.network.MetaChannel;
import stork.dls.io.network.MetaChannel_State;
import stork.dls.service.prefetch.DLSThreadsManager;
//import stork.dls.service.PipeStream;
import stork.dls.service.prefetch.MyThreadPoolExecutor;
import stork.dls.service.prefetch.PrefetchingServices;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamManagement;
import stork.dls.stream.DLSStreamPool;
import stork.dls.stream.type.DLSFTPStream;
/**
 * debug print Config
 * @author bing
 */
public class DebugConfig {
	/**
	 * for debug
	 */
	public static void DebugPrint(){
	    MetaChannel_State.DEBUG_PRINT = false;
		MetaChannel.DEBUG_PRINT = false;//true;
		MetaChannel_State.DEBUG_PRINT = false;
		DLSMetaCmdTask.DEBUG_PRINT = false;//true;
		
		DLSStreamPool.DEBUG_PRINT = false;//false;true;
		PrefetchingServices.DEBUG_PRINT = false;//true;
		
		WorkerThread.DEBUG_PRINT = false;
		DLSThreadsManager.DEBUG_PRINT = false;	
		
		//DLS network protocol client: false
		DLSClient.DEBUG_PRINT = false;
		DLSGSIFTPClient.DEBUG_PRINT = false;
		
		//PipeStream.DEBUG_PRINT = false;
		
		MyThreadPoolExecutor.DEBUG_PRINT = false;//true;
		DLSFTPStream.DEBUG_PRINT = false;//true;
		DLSFTPMetaChannel.DEBUG_PRINT = false;//true;
				
		DLSSimpleTransferReader.DEBUG_PRINT = false;//true;
		
		//all the protocols stream class
		DLSStream.DEBUG_PRINT = false;//true;
		
		DLSStreamManagement.DEBUG_PRINT = false;
	}
	/**
	 * time measurement config
	 */
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}