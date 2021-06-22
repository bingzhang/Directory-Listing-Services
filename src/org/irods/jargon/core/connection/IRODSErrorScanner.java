/**
 * 
 */
package org.irods.jargon.core.connection;

import org.irods.jargon.core.exception.AuthenticationException;
import org.irods.jargon.core.exception.CatNoAccessException;
import org.irods.jargon.core.exception.CatalogSQLException;
import org.irods.jargon.core.exception.CollectionNotEmptyException;
import org.irods.jargon.core.exception.DataNotFoundException;
import org.irods.jargon.core.exception.DuplicateDataException;
import org.irods.jargon.core.exception.FileIntegrityException;
import org.irods.jargon.core.exception.FileNotFoundException;
import org.irods.jargon.core.exception.InvalidArgumentException;
import org.irods.jargon.core.exception.InvalidGroupException;
import org.irods.jargon.core.exception.InvalidResourceException;
import org.irods.jargon.core.exception.InvalidUserException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.JargonFileOrCollAlreadyExistsException;
import org.irods.jargon.core.exception.NoAPIPrivException;
import org.irods.jargon.core.exception.NoMoreRulesException;
import org.irods.jargon.core.exception.NoResourceDefinedException;
import org.irods.jargon.core.exception.RemoteScriptExecutionException;
import org.irods.jargon.core.exception.SpecificQueryException;
import org.irods.jargon.core.exception.UnixFileMkdirException;
import org.irods.jargon.core.exception.UnixFileRenameException;
import org.irods.jargon.core.exception.ZoneUnavailableException;
import org.irods.jargon.core.protovalues.ErrorEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object is interposed in the process of interpreting the iRODS responses
 * to various protocol operations. The job of this class is to inspect the iRODS
 * response, and to detect and throw appropriate Exceptions based on the status
 * of the iRODS response. Specifically, this object detects iRODS error codes in
 * the 'intInfo' part of the response, and maps them to Jargon exceptions.
 * <p/>
 * Note that this is an early implementation, and a fuller error hierarchy will
 * develop over time.
 * 
 * @author Mike Conway - DICE (www.irods.org)
 * 
 */
public class IRODSErrorScanner {

	public static final Logger log = LoggerFactory
			.getLogger(IRODSErrorScanner.class);

	/**
	 * Scan the response for errors, and incorporate any message information
	 * that might expand the error
	 * 
	 * @param infoValue
	 *            <code>int</code> with the iRODS info value from a packing
	 *            instruction response header
	 * @param message
	 *            <code>String</code> with any additional error information
	 *            coming from the response in the <code>msg</code> field of the
	 *            header
	 * @throws JargonException
	 */
	public static void inspectAndThrowIfNeeded(final int infoValue,
			String message) throws JargonException {

		log.debug("inspectAndThrowIfNeeded:{}", infoValue);

		if (infoValue == 0) {
			return;
		}

		if (message == null) {
			message = "";
		}

		// non-zero value, create appropriate exception, first try some ranges
		// (especially for unix file system exceptions, which can have subcodes

		if (infoValue >= -520013 && infoValue <= -520000) {
			throw new UnixFileMkdirException("Exception making unix directory",
					infoValue);
		} else if (infoValue >= -528036 && infoValue <= -528000) {
			throw new UnixFileRenameException(
					"Exception renaming file in file system", infoValue);
		}

		ErrorEnum errorEnum;

		try {
			log.debug("scanning for info value...");
			errorEnum = ErrorEnum.valueOf(infoValue);
			log.debug("errorEnum val:{}", errorEnum);
		} catch (IllegalArgumentException ie) {
			log.error("error getting error enum value", ie);
			throw new JargonException(
					"error code received from iRODS, not in ErrorEnum translation table:"
							+ infoValue, infoValue);
		}

		// non-zero value, create appropriate exception

		switch (errorEnum) {
		case OVERWITE_WITHOUT_FORCE_FLAG:
			throw new JargonFileOrCollAlreadyExistsException(
					"Attempt to overwrite file without force flag.", infoValue);
		case CAT_INVALID_AUTHENTICATION:
			throw new AuthenticationException("AuthenticationException",
					infoValue);
		case CAT_INVALID_USER:
			throw new InvalidUserException("InvalidUserException");
		case SYS_NO_API_PRIV:
			throw new NoAPIPrivException(
					"User lacks privileges to invoke the given API");
		case CAT_NO_ROWS_FOUND:
			throw new DataNotFoundException("No data found");
		case CAT_NAME_EXISTS_AS_COLLECTION:
			throw new JargonFileOrCollAlreadyExistsException(
					"Collection already exists", infoValue);
		case CAT_NAME_EXISTS_AS_DATAOBJ:
			throw new JargonFileOrCollAlreadyExistsException(
					"Attempt to overwrite file without force flag", infoValue);
		case CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
			throw new DuplicateDataException(
					"Catalog already has item by that name");
		case USER_CHKSUM_MISMATCH:
			throw new FileIntegrityException(
					"File checksum verification mismatch");
		case CAT_UNKNOWN_FILE:
			throw new DataNotFoundException("Unknown file");
		case CAT_UNKNOWN_COLLECTION:
			throw new DataNotFoundException("Unknown collection");
		case CAT_COLLECTION_NOT_EMPTY:
			throw new CollectionNotEmptyException("Collection not empty",
					infoValue);
		case EXEC_CMD_ERROR:
			throw new RemoteScriptExecutionException(
					"Remote script execution error" + infoValue);
		case USER_FILE_DOES_NOT_EXIST:
			throw new FileNotFoundException("File not found", infoValue);
		case CAT_INVALID_GROUP:
			throw new InvalidGroupException("Invalid iRODS group", infoValue);
		case CAT_NO_ACCESS_PERMISSION:
			throw new CatNoAccessException("No access to item in catalog");
		case COLLECTION_NOT_EMPTY:
			throw new CollectionNotEmptyException("The collection is not empty");
		case USER_NO_RESC_INPUT_ERR:
			throw new NoResourceDefinedException("No resource defined");
		case NO_MORE_RULES_ERR:
			throw new NoMoreRulesException("No more rules");
		case CAT_SQL_ERR:
			throw new CatalogSQLException("Catalog SQL error");
		case SPECIFIC_QUERY_EXCEPTION:
			throw new SpecificQueryException(
					"Exception processing specific query", infoValue);
		case CAT_INVALID_ARGUMENT:
			throw new InvalidArgumentException(message, infoValue);
		case CAT_INVALID_RESOURCE:
			throw new InvalidResourceException(message, infoValue);
		case FEDERATED_ZONE_NOT_AVAILABLE:
			throw new ZoneUnavailableException(
					"the federated zone is not available");
		case PAM_AUTH_ERROR:
			throw new AuthenticationException("PAM authentication error");
		default:
			StringBuilder sb = new StringBuilder();
			if (message.isEmpty()) {
				sb.append("error code received from iRODS:");
				sb.append(infoValue);

				throw new JargonException(sb.toString(), infoValue);
			} else {
				sb.append("error code received from iRODS:");
				sb.append(infoValue);
				sb.append(" message:");
				sb.append(message);
				throw new JargonException(sb.toString(), infoValue);
			}
		}
	}

	/**
	 * Inspect the <code>info</code> value from an iRODS packing instruction
	 * response header and throw an exception if an error was detected
	 * 
	 * @param infoValue
	 * @throws JargonException
	 */
	public static void inspectAndThrowIfNeeded(final int infoValue)
			throws JargonException {

		inspectAndThrowIfNeeded(infoValue, "");
	}

}
