package stork.dls.io.network;

import java.io.IOException;

import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.Reply;

public interface ReplyParser {
	String getCurrentCmd();
	void replyParser(Reply reply) throws IOException, ServerException;
}
