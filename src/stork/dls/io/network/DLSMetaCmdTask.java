package stork.dls.io.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.globus.ftp.dc.LocalReply;
import org.globus.ftp.dc.Task;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;

import stork.dls.service.prefetch.WorkerThread;
import stork.dls.util.DLSLog;

/**
 * task in network IO queue
 * @author bing
 * @see WorkerThread
 */
public class DLSMetaCmdTask{
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSMetaCmdTask.class.getName());
	final String assignedThreadName;
	boolean isComplete = false;
	boolean isException = false;
	//private final WorkerThread wt;
	private final Thread wt;
	private final List<Command> cmds;
	private final List<ReplyParser> replyParserChain;
	private final BlockingQueue<Reply> replies;
	private final List<LocalReply> writeBacks;
	private final int cmdsNum;
	private int value = 0;
	private int notify =0;
	private int waitCount = 0;
	private int waitOn = 0;
	private int wakeUp = 0;
	private int writeBacksNum = 0;

	public List<ReplyParser> getReplayParserChain(){
		return Collections.unmodifiableList(this.replyParserChain);
	}
	
	public String getAssignedThreadName(){
		return this.assignedThreadName;
	}
	
	DLSMetaCmdTask(final String assignedThreadName, final List<Command> cmds, final List<ReplyParser> replyParserChain, final List<LocalReply> writeBacks){
		this.assignedThreadName = assignedThreadName;
		this.cmds = cmds;
		Thread t = Thread.currentThread();
		//this.wt  = (WorkerThread)t;
		this.wt = t;
		if(null != writeBacks){
			this.writeBacks = Collections.unmodifiableList(writeBacks);
		}else{
			this.writeBacks = null;
		}
		this.replyParserChain = Collections.unmodifiableList(replyParserChain);
		cmdsNum = this.replyParserChain.size();
		replies = new LinkedBlockingQueue<Reply>(cmdsNum);
	}
	
	@Override
	public String toString(){
		
		Iterator<Command> it = cmds.iterator();
		Command cmd = it.next();
		String tostring = this.assignedThreadName + ": " + cmd.toString();
		tostring = tostring.trim();
		tostring = tostring + "; ";
		while(it.hasNext()){
			cmd = it.next();
			tostring += cmd.toString();
			tostring = tostring.trim();
			tostring = tostring + "; ";
		}
		return tostring;
		
	}
	
	public List<Command> getCmds(){
		return Collections.unmodifiableList(this.cmds);
	}
	
	public synchronized Reply waitOn() throws ServerException, IOException{
		if(waitOn < this.cmdsNum){
			logger.debug("PipeEvent: "+wt.getName()+" wait on " + this);
			if (value <= waitCount) {
				waitCount++;
				try {
					do {
						wait();
					} while (notify == 0);
				} catch (InterruptedException e) {
					System.out.println("PipeEvent: thread = " + wt.getName() + " got interrupted~!");
					notify();
				} finally {
					waitCount --;
				}
				notify --;
			}
			logger.debug("PipeEvent: "+wt.getName()+" acquired " + this);
			waitOn ++;
		}
		Reply reply = null;
		reply = replies.poll();
		value--;
		return reply;
	}
	
	public synchronized boolean wakeUp(Reply reply, boolean isException){
		boolean ret = wakeUp < this.cmdsNum;
		if(ret){
			if(isException){
				this.isException = isException;	
			}
			value++;
			if (waitCount > notify) {
				notify++;
				notify();
			}
			logger.debug("PipeEvent: thread = " + wt.getName() + " got wakedUp~!");
			replies.add(reply);
			wakeUp ++;
			ret = wakeUp < this.cmdsNum;
		}
		this.isComplete = !ret;
		return ret;
	}
	
	public synchronized boolean execute(){
		boolean ret = wakeUp < this.cmdsNum;
		if(ret){
			if(null != writeBacks){
				final Reply writeback = writeBacks.get(writeBacksNum);
				if(null != writeback){
					ret = wakeUp(writeback, false);
					writeBacksNum ++;
				};
			}
		}
		return ret;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}



