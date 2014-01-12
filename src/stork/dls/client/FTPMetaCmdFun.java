package stork.dls.client;

import java.io.IOException;
import java.util.Date;
import org.globus.ftp.exception.ServerException;

/**
 * all the meta-commands definition
 * @author bing
 * @see DLSClient
 */
public interface FTPMetaCmdFun {
	
    /** Closes connection. Sends QUIT and closes connection 
     *  even if the server reply was not positive. Also, closes
     *  the local server.
     *
     * @param ignoreQuitReply if true the <code>QUIT</code> command
     *        will be sent but the client will not wait for the
     *        server's reply. If false, the client will block
     *        for the server's reply.
     **/
    public void close(boolean ignoreQuitReply) 
        throws IOException, ServerException; 
	
    /**
    Aborts the current transfer. FTPClient is not thread
    safe so be careful with using this procedure, which will
    typically happen in multi threaded environment.
    Especially during client-server two party transfer,
    calling abort() may result with exceptions being thrown in the thread
    that currently perform the transfer.
     **/
	public void abort() throws IOException, ServerException;
	
    /**
     * Returns the remote file size.
     *
     * @param     filename filename get the size for.
     * @return    size of the file.
     * @exception ServerException if the file does not exist or 
     *            an error occured.
     */
    public long getSize(String filename) 
        throws IOException, ServerException;
    
    /**
     * Returns last modification time of the specifed file.
     *
     * @param     filename filename get the last modification time for.
     * @return    the time and date of the last modification.
     * @exception ServerException if the file does not exist or
     *            an error occured.
     */
    public Date getLastModified(String filename)
        throws IOException, ServerException;
    
    /**
     * Checks if given file/directory exists on the server.
     *
     * @param  filename 
     *         file or directory name
     * @return true if the file exists, false otherwise.
     */
    public boolean exists(String filename)
        throws IOException, ServerException;
    /**
     * Changes the remote current working directory.
     */
    public void changeDir(String dir) 
        throws IOException, ServerException;
    /**
     * Deletes the remote directory.
     */
    public void deleteDir(String dir) 
        throws IOException, ServerException;
    /**
     * Deletes the remote file.
     */
    public void deleteFile(String filename)
        throws IOException, ServerException;
    /**
     * Creates remote directory.
     */
    public void makeDir(String dir) 
        throws IOException, ServerException;
    /**
     * Renames remote directory.
     */
    public void rename(String oldName, String newName)
        throws IOException, ServerException;
    /**
     * Returns remote current working directory.
     * @return remote current working directory.
     */
    public String getCurrentDir() throws IOException, ServerException;
    /**
     * Changes remote current working directory to the higher level.
     */
    public void goUpDir() throws IOException, ServerException;
}
