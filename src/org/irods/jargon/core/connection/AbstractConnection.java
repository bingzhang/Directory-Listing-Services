package org.irods.jargon.core.connection;

import static org.irods.jargon.core.connection.ConnectionConstants.INT_LENGTH;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;

import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.utils.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract connection to iRODS, representing the network layer of communication
 * between Jargon and iRODS.
 * <p/>
 * This abstraction will eventually be able to handle TCP as well as NIO, with
 * indicators to determine which techniques are available depending on the lower
 * level implementation. This will evolve over time but will not change the
 * public API.
 * 
 * @author Mike Conway - DICE (www.irods.org) see http://code.renci.org for
 *         trackers, access info, and documentation
 * 
 */
public abstract class AbstractConnection {

	static final Logger log = LoggerFactory.getLogger(AbstractConnection.class);

	protected IRODSProtocolManager irodsProtocolManager;
	private String connectionInternalIdentifier;
	protected volatile boolean connected = false;
	protected Socket connection;
	protected InputStream irodsInputStream;
	protected OutputStream irodsOutputStream;
	private IRODSSession irodsSession = null;
	protected final IRODSAccount irodsAccount;
	protected final PipelineConfiguration pipelineConfiguration;

	public enum EncryptionType {
		NONE, SSL_WRAPPED
	}

	private EncryptionType encryptionType = EncryptionType.NONE;
	/**
	 * 4 bytes at the front of the header, outside XML
	 */
	public static final int HEADER_INT_LENGTH = 4;
	/**
	 * Buffer output to the socket.
	 */
	protected byte outputBuffer[] = null;
	/**
	 * Holds the offset into the outputBuffer array for adding new data.
	 */
	private int outputOffset = 0;

	/**
	 * Constructor with account info to set up socket and information about
	 * buffering and other networking details
	 * 
	 * @param irodsAccount
	 *            {@link IRODSAccount} that defines the connection
	 * @param pipelineConfiguration
	 *            {@link PipelineConfiguration} that defines the low level
	 *            connection and networking configuration
	 * @param irodsProtocolManager
	 *            {@link irodsProtocolManager} that requested this connection
	 * @throws JargonException
	 */
	protected AbstractConnection(final IRODSAccount irodsAccount,
			final PipelineConfiguration pipelineConfiguration,
			final IRODSProtocolManager irodsProtocolManager)
			throws JargonException {
		log.info("AbstractConnection()");
		if (irodsAccount == null) {
			throw new IllegalArgumentException("null irodsAccount");
		}
		if (pipelineConfiguration == null) {
			throw new IllegalArgumentException("null pipelineConfiguration");
		}

		if (irodsProtocolManager == null) {
			throw new IllegalArgumentException("null irodsProtocolManager");
		}

		this.irodsAccount = irodsAccount;
		this.pipelineConfiguration = pipelineConfiguration;
		this.irodsProtocolManager = irodsProtocolManager;

		/*
		 * If using the custom internal buffer, initialize it
		 */

		if (pipelineConfiguration.getInternalCacheBufferSize() > 0) {
			log.info("using internal cache buffer of size:{}",
					pipelineConfiguration.getInternalCacheBufferSize());
			outputBuffer = new byte[pipelineConfiguration
					.getInternalCacheBufferSize()];
		}
		initializeConnection(irodsAccount);

	}

	protected void initializeConnection(final IRODSAccount irodsAccount)
			throws JargonException {
		// connect to irods, do handshake
		// save the irods startup information to the IRODSServerProperties
		// object in the irodsConnection

		log.debug("initializing connection with account:{}", irodsAccount);

		if (irodsAccount == null) {
			log.error("no irods account");
			throw new JargonException(
					"no irods account specified, cannot connect");
		}

		if (irodsProtocolManager == null) {
			log.error("null irods connection manager");
			throw new JargonException("null irods connection manager");
		}

		log.info("opening irods socket");

		connect(irodsAccount);
		this.setConnected(true);

		// build an identifier for this connection, at least for now
		StringBuilder connectionInternalIdentifierBuilder = new StringBuilder();
		connectionInternalIdentifierBuilder.append(irodsAccount.toURI(false)
				.toASCIIString());
		connectionInternalIdentifierBuilder.append('/');
		connectionInternalIdentifierBuilder.append(Thread.currentThread()
				.getName());
		connectionInternalIdentifierBuilder.append('/');
		connectionInternalIdentifierBuilder.append(System.currentTimeMillis());
		connectionInternalIdentifier = connectionInternalIdentifierBuilder
				.toString();
	}

	/**
	 * Do an initial (first) connection to iRODS based on account and
	 * properties. This is differentiated from the <code>reconnect()</code>
	 * method which is used to periodically renew a socket
	 * <p/>
	 * At the successful completion of this method, the networking is created,
	 * though the handshake and authentication steps remain
	 * 
	 * @param irodsAccount
	 *            {@link IRODSAccount} that contains information on host/port
	 * @param startupResponseData
	 *            {@link StartupResponseData} that would carry necessary info to
	 *            divert the socket to a reconnect host/port. Otherwise, this is
	 *            set to <code>null</code> and ignored.
	 * @throws JargonException
	 */
	protected abstract void connect(final IRODSAccount irodsAccount)
			throws JargonException;

	public void disconnectWithForce() {

		log.info("disconnecting...");
		// disconnect from irods and close
		irodsProtocolManager.returnConnectionWithForce(this);
	}

	public boolean isConnected() {
		return connected;
	}

	public IRODSProtocolManager getIRODSProtocolManager() {
		return irodsProtocolManager;
	}

	@Override
	public String toString() {
		return connectionInternalIdentifier;
	}

	/**
	 * Writes value.length bytes to this output stream.
	 * 
	 * @param value
	 *            value to be sent
	 * @throws NullPointerException
	 *             Send buffer is empty
	 * @throws IOException
	 *             If an IOException occurs
	 */
	public void send(final byte[] value) throws IOException {

		try {
			// packing instructions may be null, in which case nothing is sent
			if (value == null) {
				return;
			}

			if (value.length == 0) {
				// nothing to send, warn and ignore
				return;
			}

			if (pipelineConfiguration.getInternalCacheBufferSize() <= 0) {
				irodsOutputStream.write(value);
			} else if ((value.length + outputOffset) >= pipelineConfiguration
					.getInternalCacheBufferSize()) {
				// in cases where OUTPUT_BUFFER_LENGTH isn't big enough
				irodsOutputStream.write(outputBuffer, 0, outputOffset);
				irodsOutputStream.write(value);
				outputOffset = 0;
			} else {

				// the message sent isn't longer than OUTPUT_BUFFER_LENGTH
				System.arraycopy(value, 0, outputBuffer, outputOffset,
						value.length);
				outputOffset += value.length;

			}
		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}
	}

	/**
	 * Writes a certain length of bytes at some offset in the value array to the
	 * output stream, by converting the value to a byte array and calling send(
	 * byte[] value ).
	 * 
	 * @param value
	 *            value to be sent
	 * @param offset
	 *            offset into array
	 * @param length
	 *            number of bytes to read
	 * @throws IOException
	 *             If an IOException occurs
	 */
	public void send(final byte[] value, final int offset, final int length)
			throws IOException {

		if (value == null) {
			log.error("value cannot be null");
			throw new IllegalArgumentException("value cannot be null");
		}

		if (value.length == 0) {
			// nothing to send, warn and ignore
			log.warn("nothing to send, ignoring...");
			return;
		}

		if (offset > value.length) {
			String err = "trying to send a byte buffer from an offset that is out of range";
			log.error(err);
			disconnectWithForce();
			throw new IllegalArgumentException(err);
		}

		if (length <= 0) {
			// nothing to send, warn and ignore
			String err = "send length is zero";
			log.error(err);
			disconnectWithForce();
			throw new IllegalArgumentException(err);
		}

		byte temp[] = new byte[length];

		System.arraycopy(value, offset, temp, 0, length);

		try {
			send(temp);
		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}
	}

	/**
	 * Writes value.length bytes to this output stream.
	 * 
	 * @param value
	 *            value to be sent
	 * @throws IOException
	 *             If an IOException occurs
	 */
	public void send(final String value) throws IOException {
		if (value == null) {
			log.debug("null input packing instruction, do not send");
			return;
		}
		try {
			send(value.getBytes(pipelineConfiguration.getDefaultEncoding()));
		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}

	}

	/**
	 * Writes an int to the output stream as four bytes, network order (high
	 * byte first).
	 * 
	 * @param value
	 *            value to be sent
	 * @throws IOException
	 *             If an IOException occurs
	 */
	protected void sendInNetworkOrder(final int value) throws IOException {
		byte bytes[] = new byte[INT_LENGTH];

		try {
			Host.copyInt(value, bytes);
			send(bytes);
			flush();
		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}

	}

	/**
	 * Writes the given input stream content, for the given length, to the iRODS
	 * agent
	 * 
	 * @param source
	 *            <code>InputStream</code> to the data to be written. This
	 *            stream will have been buffered by the caller, no buffering is
	 *            done here.
	 * @param length
	 *            <code>long</code> with the length of data to send
	 * @param lengthLeftToSend
	 * @param connectionProgressStatusListener
	 *            {link ConnectionProgressStatusListener} or <code>null</code>
	 *            if no listener desired. This listener can then receive
	 *            call-backs of instantaneous byte counts.
	 * @throws IOException
	 *             If an IOException occurs
	 */
	protected long send(
			final InputStream source,
			long length,
			final ConnectionProgressStatusListener connectionProgressStatusListener)
			throws IOException {

		if (source == null) {
			String err = "value is null";
			log.error(err);
			throw new IllegalArgumentException(err);
		}

		try {
			int lenThisRead = 0;
			long lenOfTemp = Math.min(
					pipelineConfiguration.getInputToOutputCopyBufferByteSize(),
					length);
			long dataSent = 0;

			byte[] temp = new byte[(int) lenOfTemp];

			while (length > 0) {
				if (temp.length > length) {
					temp = new byte[(int) length];
				}
				lenThisRead = source.read(temp);

				if (lenThisRead == -1) {
					log.info("done with stream");
					break;
				}

				length -= lenThisRead;
				dataSent += lenThisRead;
				send(temp, 0, lenThisRead);
				/*
				 * If a listener is specified, send call-backs with progress
				 */
				if (connectionProgressStatusListener != null) {
					connectionProgressStatusListener
							.connectionProgressStatusCallback(ConnectionProgressStatus
									.instanceForSend(lenThisRead));
				}
			}

			log.debug("final flush of data sent");
			flush();
			log.info("total sent:{}", dataSent);
			return dataSent;

		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}
	}

	/**
	 * Flushes all data in the output stream and sends it to the server.
	 * 
	 * @throws NullPointerException
	 *             Send buffer empty
	 * @throws IOException
	 *             If an IOException occurs
	 */
	public void flush() throws IOException {
		if (connection.isClosed()) {
			// hopefully this isn't too slow to check.
			throw new ClosedChannelException();
		}

		try {
			if (pipelineConfiguration.getInternalCacheBufferSize() > 0) {
				irodsOutputStream.write(outputBuffer, 0, outputOffset);
				irodsOutputStream.flush();
				byte zerByte = (byte) 0;
				java.util.Arrays.fill(outputBuffer, zerByte);
				outputOffset = 0;
			} else {
				irodsOutputStream.flush();
			}

		} catch (IOException ioe) {
			disconnectWithForce();
			throw ioe;
		}

	}

	/**
	 * Reads a byte from the server.
	 * 
	 * @throws IOException
	 *             If an IOException occurs
	 */
	protected byte read() throws JargonException {
		try {
			return (byte) irodsInputStream.read();
		} catch (IOException ioe) {
			log.error("io exception reading", ioe);
			disconnectWithForce();
			throw new JargonException(ioe);
		}
	}

	/**
	 * Reads an int from the server
	 * 
	 * @param value
	 * @return
	 * @throws JargonException
	 */
	protected int read(final byte[] value) throws JargonException {
		try {
			return read(value, 0, value.length);
		} catch (IOException ioe) {
			log.error("io exception reading", ioe);
			disconnectWithForce();
			throw new JargonException(ioe);
		}
	}

	/**
	 * read length bytes from the server socket connection and write them to
	 * destination
	 */
	void read(final OutputStream destination, final long length)
			throws IOException {
		read(destination, length, null);
	}

	/**
	 * Read from the iRODS connection for a given length, and write what is read
	 * from iRODS to the given <code>OutputStream</code>.
	 * 
	 * @param destination
	 *            <code>OutputStream</code> to which data will be streamed from
	 *            iRODS. Note that this method will wrap the output stream with
	 *            a buffered stream for you.
	 * @param length
	 *            <code>long</code> with the length of data to be read from
	 *            iRODS and pushed to the stream.
	 * @param intraFileStatusListener
	 *            {@link ConnectionProgressStatusListener} that will receive
	 *            progress on the streaming, or <code>null</code> for no such
	 *            call-backs.
	 */
	public void read(final OutputStream destination, long length,
			final ConnectionProgressStatusListener intraFileStatusListener)
			throws IOException {

		if (destination == null) {
			String err = "destination is null";
			log.error(err);
			throw new IllegalArgumentException(err);
		}

		if (length == 0) {
			String err = "read length is set to zero";
			log.error(err);
			throw new IllegalArgumentException(err);
		}

		BufferedOutputStream bos = new BufferedOutputStream(destination);

		try {
			byte[] temp = new byte[Math.min(
					pipelineConfiguration.getInputToOutputCopyBufferByteSize(),
					(int) length)];

			int n = 0;
			while (length > 0) {
				n = read(temp, 0, Math.min(pipelineConfiguration
						.getInputToOutputCopyBufferByteSize(), (int) length));
				if (n > 0) {
					length -= n;
					bos.write(temp, 0, n);
					/*
					 * If a listener is specified, send call-backs with progress
					 */
					if (intraFileStatusListener != null) {
						intraFileStatusListener
								.connectionProgressStatusCallback(ConnectionProgressStatus
										.instanceForSend(n));
					}
				} else {
					length = n;
				}
			}

			bos.flush();

		} catch (IOException ioe) {
			log.error("io exception reading", ioe);
			disconnectWithForce();
			throw ioe;
		} finally {
			try {
				bos.close();
			} catch (Exception e) {
				// ignore
			}
		}
	}

	/**
	 * Reads a byte array from the server. Blocks until <code>length</code>
	 * number of bytes are read.
	 * 
	 * @param length
	 *            length of byte array to be read
	 * @return byte[] bytes read from the server
	 * @throws OutOfMemoryError
	 *             Read buffer overflow
	 * @throws ClosedChannelException
	 *             if the connection is closed
	 * @throws NullPointerException
	 *             Read buffer empty
	 * @throws IOException
	 *             If an IOException occurs
	 */
	protected int read(final byte[] value, final int offset, final int length)
			throws ClosedChannelException, InterruptedIOException, IOException {

		if (value == null) {
			String err = "no data sent";
			log.error(err);
			disconnectWithForce();
			throw new IllegalArgumentException(err);
		}

		if (log.isDebugEnabled()) {
			log.debug("IRODSConnection.read, byte array size =  {}",
					value.length);
			log.debug("offset = {}", offset);
			log.debug("length = {}", length);
		}

		if (length == 0) {
			String err = "read length is set to zero";
			log.error(err);
			disconnectWithForce();
			throw new IOException(err);
		}

		int result = 0;
		if (length + offset > value.length) {
			log.error("index out of bounds exception, length + offset larger then byte array");
			disconnectWithForce();
			throw new IllegalArgumentException(
					"length + offset larger than byte array");
		}

		try {
			int bytesRead = 0;
			while (bytesRead < length) {
				int read = irodsInputStream.read(value, offset + bytesRead,
						length - bytesRead);
				if (read == -1) {
					break;
				}
				bytesRead += read;
			}
			result = bytesRead;

			return result;
		} catch (ClosedChannelException e) {
			log.error("exception reading from socket", e);
			disconnectWithForce();
			throw e;

		} catch (InterruptedIOException e) {
			log.error("exception reading from socket", e);
			disconnectWithForce();
			throw e;

		} catch (IOException e) {
			log.error("exception reading from socket", e);
			disconnectWithForce();
			throw e;
		}
	}

	/**
	 * @return the irodsSession that created this connection
	 */
	protected IRODSSession getIrodsSession() {
		return irodsSession;
	}

	/**
	 * @param irodsSession
	 *            the irodsSession that created this connection
	 */
	protected void setIrodsSession(final IRODSSession irodsSession) {
		this.irodsSession = irodsSession;
	}

	/**
	 * @return the irodsAccount associated with this connection
	 */
	public IRODSAccount getIrodsAccount() {
		return irodsAccount;
	}

	@Override
	protected void finalize() throws Throwable {
		/*
		 * Check if a still-connected agent connection is being finalized, and
		 * nag in the log, then try and disconnect
		 */

		if (connected) {
			log.error("**************************************************************************************");
			log.error("********  WARNING: POTENTIAL CONNECTION LEAK  ******************");
			log.error("********  finalizer has run and found a connection left opened, please check your code to ensure that all connections are closed");
			log.error("********  connection is:{}, will attempt to disconnect",
					this.connectionInternalIdentifier);
			log.error("**************************************************************************************");
			triggerSessionCacheCleanupViaConnection();
		}

		super.finalize();
	}

	/**
	 * @param irodsProtocolManager
	 *            the irodsProtocolManager to set
	 */
	public void setIrodsProtocolManager(
			final IRODSProtocolManager irodsProtocolManager) {
		this.irodsProtocolManager = irodsProtocolManager;
	}

	/**
	 * @return the irodsInputStream
	 */
	protected InputStream getIrodsInputStream() {
		return irodsInputStream;
	}

	/**
	 * @return the irodsOutputStream
	 */
	protected OutputStream getIrodsOutputStream() {
		return irodsOutputStream;
	}

	/**
	 * @return the pipelineConfiguration
	 */
	protected PipelineConfiguration getPipelineConfiguration() {
		return pipelineConfiguration;
	}

	/**
	 * @return the connection
	 */
	protected Socket getConnection() {
		return connection;
	}

	/**
	 * Set the status to disconnected. This is only used in special
	 * circumstances, such as when wrapping a socket in an SSL connection when
	 * doing PAM authentication. These are special occasions where an
	 * <code>IRODSConnection</code> is created outside of the normal factory.
	 * <p/>
	 * For general usage, this method should not called.
	 * 
	 * @param connected
	 *            the connected to set
	 */
	protected void setConnected(final boolean connected) {
		this.connected = connected;
	}

	/**
	 * Close down the actual network connection
	 * 
	 * @throws JargonException
	 */
	protected abstract void shutdown() throws JargonException;

	/**
	 * Close doen the actual connection and quash any errors (avoids
	 * boiler-plate try-catch in code)
	 */
	protected abstract void obliterateConnectionAndDiscardErrors();

	/**
	 * Clean up class, when finalizing, that can trigger the removal of any
	 * cached AbstractIRODSMidLevelProtocol implemenetion to iRODS when a
	 * connection is being finalized.
	 * <p/>
	 * Typically, connections are cleaned up when the containing
	 * <code>AbstractIRODSMidLevelProtocol</code> is closed in an orderly
	 * fashion. The cleanup via a handle to the connection is a recovery
	 * procedure when this doesn't occur.
	 * 
	 * @throws JargonException
	 */
	protected void triggerSessionCacheCleanupViaConnection()
			throws JargonException {
		if (!connection.isConnected()) {
			log.debug("not connected, just bypass");
		}
		log.info("disconnecting...");
		// disconnect from irods and close
		irodsProtocolManager.returnConnectionWithForce(this);

		log.info("disconnected");
	}

	/**
	 * @return the connectionInternalIdentifier
	 */
	public String getConnectionInternalIdentifier() {
		return connectionInternalIdentifier;
	}

	/**
	 * @return the encryptionType
	 */
	protected EncryptionType getEncryptionType() {
		return encryptionType;
	}

	/**
	 * @param encryptionType
	 *            the encryptionType to set
	 */
	protected void setEncryptionType(final EncryptionType encryptionType) {
		this.encryptionType = encryptionType;
	}

}