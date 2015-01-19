package stork.dls.io.network;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.globus.ftp.Buffer;
import org.globus.ftp.DataSink;
import org.globus.ftp.FileInfo;
import org.globus.ftp.dc.AbstractDataChannel;
import org.globus.ftp.dc.DataChannelReader;
import org.globus.ftp.dc.SocketBox;
import org.globus.ftp.dc.TransferContext;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;

import stork.dls.ad.Ad;
import stork.dls.ad.AdSorter;
import stork.dls.ad.FTPListParser;
import stork.dls.util.DLSLog;
/**
 * parser & read of network io bytes
 * @author bing
 *
 */
public class DLSSimpleTransferReader {
	public static boolean DEBUG_PRINT;
	private static DLSLog logger = DLSLog.getLog(DLSSimpleTransferReader.class.getName());
	private static String INVALID_TOKENNUM = "Invalid number of tokens";
    protected SocketBox socketBox;
    protected TransferContext context;
    protected DataChannelReader reader;
    private ByteArrayDataSink sink;
    
    public DLSSimpleTransferReader(AbstractDataChannel dataChannel, 
    		SocketBox socketBox, TransferContext context) throws Exception{
    	this.socketBox = socketBox;
    	this.context = context;
    	this.reader = dataChannel.getDataChannelSource(context);
    	reader.setDataStream(socketBox.getSocket().getInputStream());
    	sink = new ByteArrayDataSink();
    }
    
	public Vector<FileInfo> read() throws Exception {
    	Buffer buf;
    	long transferred = 0;
    	String received = null;
    	Vector<FileInfo> fileList = new Vector<FileInfo>();
    	
    	while ((buf = reader.read()) != null) {
    	    transferred += buf.getLength();
    	    sink.write(buf);
    	}
    	logger.debug("finished receiving data; received " + 
    		     transferred + " bytes");
    	if(0 < transferred){
    		received = sink.getData().toString();
    	}
    	if(null == received) {
    		throw new SocketTimeoutException();
    		//return null;
    	}
        BufferedReader reader =
                new BufferedReader(new StringReader(received.toString()));
    	
        FileInfo fileInfo = null;
        String line = null;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            logger.debug("line ->" + line);
            if(line.equals("")){
                continue;
            }
            if (line.startsWith("total")){
                continue;
            }
            try {
                fileInfo = new FileInfo(line);
            } catch (FTPException e) {
                
                int existed = e.toString().indexOf(INVALID_TOKENNUM);
                if(-1 != existed){
                    //continue;
                }
                ClientException ce =
                    new ClientException(
                                        ClientException.UNSPECIFIED,
                                        "Could not create FileInfo");
                ce.setRootCause(e);
                throw ce;
            }
            fileList.addElement(fileInfo);
        }
    	return fileList;
    }
	
	public Vector<FileInfo> read3() throws Exception {
    	Buffer buf;
    	long transferred = 0;
    	String received = null;
    	Vector<FileInfo> fileList = new Vector<FileInfo>();
    	
    	while ((buf = reader.read()) != null) {
    	    transferred += buf.getLength();
    	    sink.write(buf);
    	}
    	logger.debug("finished receiving data; received " + 
    		     transferred + " bytes");
    	if(0 < transferred){
    		received = sink.getData().toString();
    	}
    	if(null == received) {
    		return null;
    	}
        BufferedReader reader =
                new BufferedReader(new StringReader(received.toString()));
    	
        FileInfo fileInfo = null;
        String line = null;
        FTPListParser parser = new FTPListParser('U');
        StringBuilder sb = new StringBuilder();
        AdSorter sorter = new AdSorter("-dir", "name");
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            logger.debug("line ->" + line);
            if(line.equals("")){
                continue;
            }
            if (line.startsWith("total"))
                continue;
            Ad ad = parser.parseEntry(line);
            //remain "." and ".."
            if (ad != null /*&& !parser.ignoreName(ad.get("name"))*/ ){
                sorter.add(ad);
            }
            sb.append(line);
            
            try {
                fileInfo = new FileInfo(line);
            } catch (FTPException e) {
                ClientException ce =
                    new ClientException(
                                        ClientException.UNSPECIFIED,
                                        "Could not create FileInfo");
                ce.setRootCause(e);
                throw ce;
            }
            fileList.addElement(fileInfo);
        }
        
    	return fileList;
    }

    protected void shutdown(Object quitToken) throws IOException {
    	logger.debug("shutdown");
    	reader.close();
    	socketBox.setSocket(null);
    	if (quitToken != null) {
    	    sink.close();
    	}
    }
    private class ByteArrayDataSink implements DataSink {

        private ByteArrayOutputStream received;

        public ByteArrayDataSink() {
            this.received = new ByteArrayOutputStream(1000);
        }
        
        public void write(Buffer buffer) throws IOException {
                logger.debug(
                             "received "
                             + buffer.getLength()
                             + " bytes of directory listing");
            this.received.write(buffer.getBuffer(), 0, buffer.getLength());
        }

        public void close() throws IOException {
        }
        
        public ByteArrayOutputStream getData() {
            return this.received;
        }
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
