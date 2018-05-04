package org.irods.jargon.core.transfer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;

import org.irods.jargon.core.connection.ConnectionConstants;
import org.irods.jargon.core.connection.ConnectionProgressStatus;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.utils.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle parallel file transfer put operation, this is used within jargon.core,
 * and is not meant for public API use. See
 * {@link org.irods.jargon.core.pub.DataTransferOperations} for public API used
 * for file transfers.
 * 
 * @author Mike Conway - DICE (www.irods.org)
 * 
 */
public final class ParallelPutTransferThread extends
		AbstractParallelTransferThread implements
		Callable<ParallelTransferResult> {

	private final ParallelPutFileTransferStrategy parallelPutFileTransferStrategy;
	private BufferedInputStream bis = null;

	public static final Logger log = LoggerFactory
			.getLogger(ParallelPutTransferThread.class);

	/**
	 * Represents a thread used in a parallel file transfer. There will be
	 * multiple threads controlled from the
	 * <code>ParalellPutFileTransferStrategy</code>. This is an immutable object
	 * , as is the <code>parallelFileTransferStrategy</code> that this object
	 * holds a reference to.
	 * 
	 * @param parallelPutFileTransferStrategy
	 *            {@link org.irods.jargon.core.transfer.ParallelPutFileTransferStrategy}
	 *            that controls the transfer threads.
	 * @return <code>ParallelPutTransferThread</code>
	 * @throws JargonException
	 */
	public static ParallelPutTransferThread instance(
			final ParallelPutFileTransferStrategy parallelPutFileTransferStrategy)
			throws JargonException {
		return new ParallelPutTransferThread(parallelPutFileTransferStrategy);
	}

	private ParallelPutTransferThread(
			final ParallelPutFileTransferStrategy parallelPutFileTransferStrategy)
			throws JargonException {

		super();

		if (parallelPutFileTransferStrategy == null) {
			throw new JargonException("parallelPutFileTransferStrategy is null");
		}

		this.parallelPutFileTransferStrategy = parallelPutFileTransferStrategy;

		try {
			log.info(
					"opening socket to parallel transfer (high) port at port:{}",
					parallelPutFileTransferStrategy.getPort());
			setS(new Socket(parallelPutFileTransferStrategy.getHost(),
					parallelPutFileTransferStrategy.getPort()));
			if (parallelPutFileTransferStrategy
					.getParallelSocketTimeoutInSecs() > 0) {
				log.info(
						"timeout (in seconds) for parallel transfer sockets is:{}",
						parallelPutFileTransferStrategy
								.getParallelSocketTimeoutInSecs());
				getS().setSoTimeout(
						parallelPutFileTransferStrategy
								.getParallelSocketTimeoutInSecs() * 1000);
			}
			setOut(new BufferedOutputStream(getS().getOutputStream()));
			setIn(new BufferedInputStream(getS().getInputStream()));
		} catch (Exception e) {
			log.error("unable to create transfer thread", e);
			throw new JargonException(e);
		}
	}

	@Override
	public ParallelTransferResult call() throws JargonException {

		try {

			log.info("getting input stream for local file");
			bis = new BufferedInputStream(new FileInputStream(
					parallelPutFileTransferStrategy.getLocalFile()),
					ConnectionConstants.OUTPUT_BUFFER_LENGTH);

			log.info("writing the cookie (password) for the output thread");

			// write the cookie
			byte b[] = new byte[4];
			Host.copyInt(parallelPutFileTransferStrategy.getPassword(), b);
			getOut().write(b);
			getOut().flush();

			log.debug("cookie written for output thread...calling put() to start read/write loop");
			put();
			log.debug("put operation completed");

		} catch (Exception e) {
			log.error(
					"An exception occurred during a parallel file put operation",
					e);
			setExceptionInTransfer(e);
			throw new JargonException("error during parallel file put", e);
		} finally {
			log.info("closing sockets, this eats any exceptions");
			close();
			log.info("socket conns for parallel transfer closed, now close the file stream");
			// close file stream
			try {
				bis.close();
				log.info("streams and files closed");
			} catch (IOException e) {
			}
		}

		ParallelTransferResult result = new ParallelTransferResult();
		result.transferException = getExceptionInTransfer();
		return result;

	}

	/**
	 * @throws IOException
	 * @throws JargonException
	 */
	private void seekToStartingPoint(final long offset) throws JargonException {
		long totalSkipped = 0;
		long toSkip = 0;

		// guard against occasions where skip does not skip the full amount

		try {
			if (offset > 0) {
				long skipped = bis.skip(offset);
				totalSkipped += skipped;

				while (totalSkipped < offset) {
					log.warn("did not skip entire offset amount, call skip again");
					toSkip = offset - totalSkipped;
					skipped = bis.skip(toSkip);
				}

				if (totalSkipped != offset) {
					throw new JargonException(
							"totalSkipped not equal to offset");
				}

			}
		} catch (IOException e) {
			log.error("IOException in seek", e);
			throw new JargonException(e);
		}
	}

	private void put() throws JargonException {
		log.info("put()..");

		byte[] buffer = null;
		boolean done = false;

		buffer = new byte[ConnectionConstants.OUTPUT_BUFFER_LENGTH];
		long currentOffset = 0;

		try {
			while (!done) {

				log.debug("in main put() loop, reading header data");

				// read the header
				int operation = readInt();
				if (log.isInfoEnabled()) {
					log.info("   operation:" + operation);
				}

				if (operation == AbstractParallelTransferThread.PUT_OPR) {
					log.debug("put operation");
				} else if (operation == AbstractParallelTransferThread.DONE_OPR) {
					log.info("done received");
					done = true;
					break;
				} else {
					throw new JargonException("unknown operation received");
				}

				// read the flags
				int flags = readInt();
				if (log.isInfoEnabled()) {
					log.info("   flags:" + flags);
				}
				// Where to seek into the data
				long offset = readLong();
				if (log.isInfoEnabled()) {
					log.info("   offset:" + offset);
				}
				// How much to read/write
				long length = readLong();
				if (log.isInfoEnabled()) {
					log.info("   length:" + length);
				}

				if (offset != currentOffset) {
					seekToStartingPoint(offset);
					currentOffset = offset;
				}

				log.info("buffer length for put is: {}", buffer.length);

				/*
				 * Read/write loop moves data from file starting at offset down
				 * the socket until the anticipated transfer length is consumed.
				 */

				readWriteLoopForCurrentHeaderDirective(buffer, length);
				currentOffset += length;

			}

		} catch (IOException e) {
			log.error(
					"An IO exception occurred during a parallel file put operation",
					e);
			throw new JargonException("IOException during parallel file put", e);
		}
	}

	/**
	 * @param buffer
	 * @param length
	 * @throws IOException
	 * @throws JargonException
	 */
	private void readWriteLoopForCurrentHeaderDirective(final byte[] buffer,
			final long length) throws IOException, JargonException {
		int read = 0;
		long totalRead = 0;
		long transferLength = length;
		long totalWritten = 0;
		log.debug("readWriteLoopForCurrentHeaderDirective()");
		try {
			while (transferLength > 0) {
				log.debug("read/write loop at top");

				read = bis.read(buffer, 0, (int) Math.min(
						ConnectionConstants.OUTPUT_BUFFER_LENGTH,
						transferLength));

				log.debug("bytes read: {}", read);

				if (read > 0) {

					totalRead += read;
					transferLength -= read;
					log.debug(
							"getting ready to write to iRODS, new txfr length:{}",
							transferLength);

					getOut().write(buffer, 0, read);

					/*
					 * Make an intra-file status call-back if a listener is
					 * configured
					 */
					if (parallelPutFileTransferStrategy
							.getConnectionProgressStatusListener() != null) {
						parallelPutFileTransferStrategy
								.getConnectionProgressStatusListener()
								.connectionProgressStatusCallback(
										ConnectionProgressStatus
												.instanceForSend(read));
					}

					log.debug("wrote data to the buffer");
					totalWritten += read;

				} else {
					log.debug("no read...break out of read/write");
					break;
				}/*
				 * else if (read < 0) { } throw new JargonException(
				 * "unexpected end of data in transfer operation"); }
				 */
				Thread.yield();
			}

			log.info("final flush of output buffer");
			getOut().flush();

			log.info("for thread, total read: {}", totalRead);
			log.info("   total written: {}", totalWritten);
			log.info("   transferLength: {}", transferLength);

		} catch (Exception e) {
			log.error("error writing to iRODS parallel transfer socket", e);
			setExceptionInTransfer(e);
			throw new JargonException(e);
		}

		if (totalRead != totalWritten) {
			throw new JargonException("totalRead and totalWritten do not agree");
		}

		if (transferLength != 0) {
			throw new JargonException(
					"transferLength and totalWritten do not agree");
		}
	}
}
