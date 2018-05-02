package stork.dls.io.network;

import java.io.IOException;

import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Reply;

interface MetaChannelMonitor extends Runnable{
    final static String NOSUCHEXIST = "No such file or directory";
    final static String PERMISSIONDENY = "Permission denied";
    boolean isTermintate();
    boolean isTobeRevived(boolean flip);
    Reply read() throws ServerException, IOException, FTPReplyParseException ;
    void channelTerminate(Reply reply, Exception ex);
}
