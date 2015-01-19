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
import stork.dls.stream.DLSListingTask;
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
	final private DLSListingTask listingtask;
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
	private MetaChannel metachannel = null;
    private int repliesNum;
	
	
	public DLSListingTask getListingTask(){
	    return this.listingtask;
	}
	
	public List<ReplyParser> getReplayParserChain(){
		return Collections.unmodifiableList(this.replyParserChain);
	}
	
	public String getAssignedThreadName(){
		return this.assignedThreadName;
	}
	
	DLSMetaCmdTask(DLSListingTask listingtask, final String assignedThreadName, 
	        final List<Command> cmds, final List<ReplyParser> replyParserChain, 
	        final List<LocalReply> writeBacks, final MetaChannel channel){
	    this.repliesNum = 0;
	    this.metachannel = channel;
	    this.listingtask = listingtask;
		this.assignedThreadName = assignedThreadName;
		this.cmds = cmds;
		this.wt = Thread.currentThread();	
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
	
    public synchronized boolean wakeUpInplace(Reply reply, boolean isException){
        if(isException){
            this.isException = isException;
            logger.debug("PipeEvent: thread = " + wt.getName() + " got exception~" + reply);
        }else{
            logger.debug("PipeEvent: thread = " + wt.getName() + " got wakedUp~!");
        }
        boolean ret = wakeUp < this.cmdsNum;
        if(ret){
            value++;
            if (waitCount > notify) {
                notify++;
                notify();
            }
            replies.add(reply);
            //wakeUp ++;
            ret = wakeUp < this.cmdsNum;
        }
        if(false == isException){
            this.isComplete = !ret;
        }else{
            this.isComplete = false;
        }
        return ret;
    }
    
	public synchronized boolean wakeUp(Reply reply, boolean isException){
        if(isException){
            this.isException = isException;
            logger.debug("PipeEvent: thread = " + wt.getName() + " got exception~" + reply);
        }else{
            logger.debug("PipeEvent: thread = " + wt.getName() + " got wakedUp~!");
        }
	    boolean ret = wakeUp < this.cmdsNum;
		if(ret){
		    
            /*
             * aimming to solve the retransmit exception bug.
             * 
             * to check whether this metatask finished sending all cmds or not
             * if done, then remove it from wakupQueue first, then do wakeup,
             * this is very important, because we do not do synchronization between data channels and control channel
             * So, when we begin to read from data channel, we must make sure the metatask is remove from wakupQueue,
             * then later any data channel exception can be migrate into the same stream (same wakupQueue) possibly. 
             * This consideration prevents the duplicates metatasks in the same wakupQueue.
             * */
	        replies.offer(reply);
	        repliesNum ++;
            if(repliesNum == this.cmdsNum){
                this.metachannel.pollWakupQueue();
            }
		    
			value++;
			if (waitCount > notify) {
				notify++;
				notify();
			}

			wakeUp ++;
			ret = wakeUp < this.cmdsNum;
		}
		if(false == isException){
		    this.isComplete = !ret;
		}else{
		    this.isComplete = false;
		}
		return ret;
	}
	
	public synchronized boolean execute(){
		boolean ret = wakeUp < this.cmdsNum;
		boolean hasWriteback = false; //for debug
		if(ret){
			if(null != writeBacks){
				final Reply writeback = writeBacks.get(writeBacksNum);
				if(null != writeback){
					ret = wakeUp(writeback, false);
					writeBacksNum ++;
					hasWriteback = true;
				};
			}
		}
		ret = hasWriteback;// for debug
		return ret;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}



