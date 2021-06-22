package org.irods.jargon.core.pub;

import java.io.FileNotFoundException;
import java.util.List;

import org.irods.jargon.core.exception.DataNotFoundException;
import org.irods.jargon.core.exception.DuplicateDataException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.protovalues.FilePermissionEnum;
import org.irods.jargon.core.pub.domain.AvuData;
import org.irods.jargon.core.pub.domain.DataObject;
import org.irods.jargon.core.pub.domain.ObjStat;
import org.irods.jargon.core.pub.domain.Resource;
import org.irods.jargon.core.pub.domain.UserFilePermission;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.query.AVUQueryElement;
import org.irods.jargon.core.query.JargonQueryException;
import org.irods.jargon.core.query.MetaDataAndDomainData;

/**
 * This is an access object that can be used to manipulate iRODS data objects
 * (files). This object treats the IRODSFile as an object, not as a
 * <code>java.io.File</code> object. For normal read and other familier
 * <code>java.io.*</code> operations, see
 * {@link org.irods.jargon.core.pub.io.IRODSFile}.
 * <p/>
 * This interface has a default implementation within Jargon. The access object
 * should be obtained using a factory, either by creating from
 * {@link org.irods.jargon.core.pub.IRODSFileSystem}, or from an
 * {@link org.irods.jargon.core.pub.IRODSAccessObjectFactory} implementation.
 * This class is handy for retrieving and manipulating system and user metadata
 * associated with data objects (files), as well as performing common query
 * operations. This class also supports various iRODS file operations that are
 * not included in the standard <code>java.io.*</code> libraries.
 * <p/>
 * For general data movement operations, also see
 * {@link org.irods.jargon.core.pub.DataTransferOperations}.
 * <p/>
 * <h2>Notes</h2>
 * For soft links, AVU metadata always attaches to the canonical path. There is
 * some inconsistency with the operation of the imeta command, where AVU
 * operations against the soft link target path result in file not found
 * exceptions. This is a slight departure, but perhaps less surprising. The
 * behavior of AVU metadata for data objects, which always operates on the
 * canonical path, is different than the behavior of iRODS for collection AVU
 * metadata. For collections, the AVU's are attached to the soft linked path
 * separately from the AVU metadata attached to the canonical path. Jargon tries
 * to maximize consistency by always operating on the canonical path for
 * metadata operations on data objects.
 * 
 * @author Mike Conway - DICE (www.irods.org)
 * 
 */
public interface DataObjectAO extends FileCatalogObjectAO {

	/**
	 * Query method will return the first data object found with the given
	 * collectionPath and dataName.
	 * <p/>
	 * Note that this method will return 'null' if the object is not found.
	 * 
	 * @param collectionPath
	 *            <code>String</code> with the absolute path to the collection
	 * @param dataName
	 *            <code>String</code> with the data Object name
	 * @return {@link org.irods.jargon.core.pub.DataObject}
	 * @throws DataNotFoundException
	 *             is thrown if the data object does not exist
	 * @throws JargonException
	 * @throws FileNotFoundException
	 */
	DataObject findByCollectionNameAndDataName(final String collectionPath,
			final String dataName) throws JargonException,
			FileNotFoundException;

	/**
	 * For a given absolute path, get an <code>IRODSFileImpl</code> that is a
	 * data object. If the data exists, and is not a File, this method will
	 * throw an exception. If the given file does not exist, then a File will be
	 * returned.
	 * <p/>
	 * The given path may be a soft-linked path, and it will behave as normal.
	 * 
	 * @param fileAbsolutePath
	 *            <code>String</code> with absolute path to the collection
	 * @return {@link org.irods.jargon.core.pub.io.IRODSFileImpl}
	 * @throws JargonException
	 */
	IRODSFile instanceIRODSFileForPath(final String fileAbsolutePath)
			throws JargonException;

	/**
	 * Add AVU metadata for this data object
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the target
	 *            collection
	 * @param avuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData}
	 * @throws JargonException
	 * @throws DataNotFoundException
	 *             when data object is missing
	 * @throws DuplicateDataException
	 *             when an AVU already exists. Note that iRODS (at least at 2.5)
	 *             is inconsistent, where a duplicate will only be detected if
	 *             units are not blank
	 */
	void addAVUMetadata(final String absolutePath, final AvuData avuData)
			throws DataNotFoundException, DuplicateDataException,
			JargonException;

	/**
	 * List the AVU metadata for a particular data object, as well as
	 * identifying information about the data object itself, based on a metadata
	 * query.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @param dataObjectCollectionAbsPath
	 *            <code>String with the absolute path of the collection for the dataObject of interest.
	 * @param dataObjectFileName
	 *            <code>String with the name of the dataObject of interest.
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesForDataObjectUsingAVUQuery(
			final List<AVUQueryElement> avuQuery,
			final String dataObjectCollectionAbsPath,
			final String dataObjectFileName) throws JargonQueryException,
			JargonException;

	/**
	 * List the AVU metadata for a particular data object, as well as
	 * identifying information about the data object itself, based on a metadata
	 * query.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * <p/>
	 * This version of the method will compare AVU values using case-insensitive
	 * queries
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @param dataObjectCollectionAbsPath
	 *            <code>String with the absolute path of the collection for the dataObject of interest.
	 * @param dataObjectFileName
	 *            <code>String with the name of the dataObject of interest.
	 * @param caseInsensitive
	 *            <code>boolean</code> where <code>true</code> indicates to
	 *            treat avu queries as case-insensitive
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesForDataObjectUsingAVUQuery(
			List<AVUQueryElement> avuQuery, String dataObjectCollectionAbsPath,
			String dataObjectFileName, boolean caseInsensitive)
			throws JargonQueryException, JargonException;

	/**
	 * List the AVU metadata for a particular data object, as well as
	 * identifying information about the data object itself, based on a metadata
	 * query.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @param dataObjectAbsolutePath
	 *            <code>String</code> with the absolute path of the data object
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesForDataObjectUsingAVUQuery(
			List<AVUQueryElement> avuQuery, String dataObjectAbsolutePath)
			throws JargonQueryException, JargonException;

	/**
	 * List the AVU metadata for a particular data object, as well as
	 * identifying information about the data object itself. Other methods are
	 * available for this object to refine to query to include an AVU metadata
	 * query. This method will get all of the metadata for a data object.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param dataObjectCollectionAbsPath
	 *            <code>String with the absolute path of the collection for the dataObject of interest.
	 * @param dataObjectFileName
	 *            <code>String with the name of the dataObject of interest.
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */

	List<MetaDataAndDomainData> findMetadataValuesForDataObject(
			final String dataObjectCollectionAbsPath,
			final String dataObjectFileName) throws JargonQueryException,
			JargonException;

	/**
	 * List the data objects that answer the given AVU metadata query.
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesByMetadataQuery(
			final List<AVUQueryElement> avuQuery) throws JargonQueryException,
			JargonException;

	/**
	 * List the data objects that answer the given AVU metadata query with the
	 * ability to page through a partial start index.
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @param partialStartIndex
	 *            <code>int</code> with a partial start value for paging
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesByMetadataQuery(
			final List<AVUQueryElement> avuQuery, final int partialStartIndex)
			throws JargonQueryException, JargonException;

	/**
	 * List the data objects that answer the given AVU metadata query with the
	 * ability to page through a partial start index.
	 * <p/>
	 * This version supports case-insensitive metadata queries
	 * 
	 * @param avuQuery
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElement} that
	 *            defines the metadata query
	 * @param partialStartIndex
	 *            <code>int</code> with a partial start value for paging
	 * @param caseInsensitive
	 *            <code>boolean</code> indicates that the queries should be
	 *            case-insensitive
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}\
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesByMetadataQuery(
			List<AVUQueryElement> avuQuery, int partialStartIndex,
			boolean caseInsensitive) throws JargonQueryException,
			JargonException;

	/**
	 * Given a set of metadata query parameters, return a list of IRODS Data
	 * Objects that match the metadata query
	 * 
	 * @param avuQueryElements
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElements} with the
	 *            query specification
	 * @return
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<DataObject> findDomainByMetadataQuery(
			final List<AVUQueryElement> avuQueryElements)
			throws JargonQueryException, JargonException;

	/**
	 * Given a set of metadata query parameters, return a list of IRODS Data
	 * Objects that match the metadata query. This query method allows a partial
	 * start as an offset into the result set to get paging behaviors.
	 * 
	 * @param avuQueryElements
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElements} with the
	 *            query specification
	 * @param partialStartIndex
	 *            <code>int</code> that has the partial start offset into the
	 *            result set
	 * @return
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<DataObject> findDomainByMetadataQuery(
			final List<AVUQueryElement> avuQueryElements,
			final int partialStartIndex) throws JargonQueryException,
			JargonException;

	/**
	 * Given a set of metadata query parameters, return a list of IRODS Data
	 * Objects that match the metadata query. This query method allows a partial
	 * start as an offset into the result set to get paging behaviors.
	 * <p/>
	 * This method allows the specification of case-insensitive queries on the
	 * AVU values. This is an iRODS3.2+ capability
	 * 
	 * @param avuQueryElements
	 *            <code>List</code> of
	 *            {@link org.irods.jargon.core.query.AVUQueryElements} with the
	 *            query specification
	 * @param partialStartIndex
	 *            <code>int</code> that has the partial start offset into the
	 *            result set
	 * @param caseInsensitive
	 *            <code>boolean</code> that indicates that the AVU query should
	 *            be processed as case-insensitive
	 * @return
	 * @throws JargonQueryException
	 * @throws JargonException
	 */
	List<DataObject> findDomainByMetadataQuery(
			List<AVUQueryElement> avuQueryElements, int partialStartIndex,
			boolean caseInsensitive) throws JargonQueryException,
			JargonException;

	/**
	 * Replicate the given file to the given target resource. Note that this
	 * method replicates one data object. The
	 * {@link org.irods.jargon.core.pub.DataTransferOperations} access object
	 * has more comprehensive methods for replication, including recursive
	 * replication with the ability to process callbacks.
	 * <p/>
	 * This method will work if a soft linked name is provided as expected.
	 * 
	 * @param irodsFileAbsolutePath
	 *            <code>String</code> containing the absolute path to the target
	 *            iRODS file to replicate.
	 * @param dataObjectName
	 *            <code>String<code> with the name of the data object.
	 * @param targetResource
	 *            <code>String</code> containing the resource to which the
	 *            target file should be replicated.
	 * @throws JargonException
	 */
	void replicateIrodsDataObject(final String irodsFileAbsolutePath,
			final String targetResource) throws JargonException;

	/**
	 * Get a list of <code>Resource</code> objects that contain this data
	 * object.
	 * <p/>
	 * This method will work if a soft linked name is provided as expected.
	 * 
	 * @param dataObjectPath
	 *            <code>String</code> containing the absolute path to the target
	 *            iRODS collection containing the file
	 * @param dataObjectName
	 *            <code>String</code> containing the name of the target iRODS
	 *            file.
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.pub.domain.Resource}
	 * @throws JargonException
	 */
	List<Resource> getResourcesForDataObject(final String dataObjectPath,
			final String dataObjectName) throws JargonException;

	/**
	 * Compute a checksum on a File, iRODS uses MD5 by default.
	 * <p/>
	 * This method will work if a soft linked name is provided as expected.
	 * 
	 * @param irodsFile
	 *            {@link org.irods.jargon.core.pub.io.IRODSFile} upon which the
	 *            checksum value will be calculated
	 * @return <code>String</code> with the MD5 Checksum value
	 * @throws JargonException
	 */
	String computeMD5ChecksumOnDataObject(final IRODSFile irodsFile)
			throws JargonException;

	/**
	 * Replicate the data object given as an absolute path to all of the
	 * resources defined in the <code>irodsResourceGroupName</code>. This is
	 * equivilant to an irepl -a command.
	 * <p/>
	 * The {@link org.irods.jargon.core.pub.DataTransferOperations} access
	 * object has more comprehensive methods for replication, including
	 * recursive replication with the ability to process callbacks.
	 * <p/>
	 * This method will work if a soft linked name is provided as expected.
	 * 
	 * @param irodsFileAbsolutePath
	 *            <code>String</code> containing the absolute path to the target
	 *            iRODS file to replicate.
	 * @param irodsResourceGroupName
	 *            <code>String<code> with the name of the resource group to which the file will be replicated.  The replication will be to 
	 * all members of the resource group.
	 * @throws JargonException
	 */
	void replicateIrodsDataObjectToAllResourcesInResourceGroup(
			final String irodsFileAbsolutePath,
			final String irodsResourceGroupName) throws JargonException;

	/**
	 * Delete the given AVU from the data object identified by absolute path.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param absolutePath
	 *            <code>String</code> with he absolute path to the data object
	 *            from which the AVU triple will be deleted.
	 * @param avuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} to be
	 *            removed.
	 * @throws DataNotFoundException
	 *             if the target data object is not found in iRODS
	 * @throws JargonException
	 */
	void deleteAVUMetadata(final String absolutePath, final AvuData avuData)
			throws DataNotFoundException, JargonException;

	/**
	 * Find the object representing the data object (file) in iRODS.
	 * <p/>
	 * This method will handle soft-linked paths and return the data object
	 * representing the data at the given soft linked location.
	 * 
	 * @param absolutePath
	 *            <code>String</code> with the full absolute path to the iRODS
	 *            data object.
	 * @return {@link org.irods.jargon.core.pub.domain.DataObject} with catalog
	 *         information for the given data object
	 * @throws JargonException
	 * @throws FileNotFoundException
	 */
	DataObject findByAbsolutePath(final String absolutePath)
			throws JargonException, FileNotFoundException;

	/**
	 * Set the permissions on a data object to read for the given user.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionRead(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Set the permissions on a data object to write for the given user.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionWrite(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Set the permissions on a data object to own for the given user.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionOwn(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Removes the permissions on a data object to own for the given user.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void removeAccessPermissionsForUser(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Get the file permission pertaining to the given data object
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be checked.
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @return {@link FilePermissionEnum} value with the permissions for the
	 *         file and user.
	 * @throws JargonException
	 */
	FilePermissionEnum getPermissionForDataObject(String absolutePath,
			String userName, String zone) throws JargonException;

	/**
	 * List the user permissions for the given iRODS data object.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param irodsDataObjectAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS data
	 *            object.
	 * @return <code>List</code> of {@link UserFilePermission} with the ACL's
	 *         for the given file.
	 * @throws JargonException
	 */
	List<UserFilePermission> listPermissionsForDataObject(
			String irodsDataObjectAbsolutePath) throws JargonException;

	/**
	 * List the AVU metadata associated with this irods data object.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param irodsFile
	 *            {@link IRODSfile} that points to the data object whose
	 *            metadata will be retrieved
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesForDataObject(
			IRODSFile irodsFile) throws JargonException;

	/**
	 * List the AVU metadata associated with this irods data object.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param dataObjectAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS data
	 *            object
	 * @return <code>List</code> of
	 *         {@link org.irods.jargon.core.query.MetaDataAndDomainData}
	 * @throws JargonException
	 */
	List<MetaDataAndDomainData> findMetadataValuesForDataObject(
			String dataObjectAbsolutePath) throws JargonException;

	/**
	 * This is a special method to modify the Avu value for a given attribute
	 * name and unit. Often, it is the case that applications want to keep
	 * unique values for a data object, and be able to easily change the value
	 * while preserving the attribute name and units. This method allows the
	 * specification of an AVU with the known name and units, and an arbitrary
	 * value. The method will find the unique attribute by name and unit, and
	 * overwrite the existing value with the value given in the
	 * <code>AvuData</code> parameter.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param absolutePath
	 * @param currentAvuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the existing Avu name and unit, with the desired new value
	 * @throws DataNotFoundException
	 *             if the AVU data or collection is not present
	 * @throws JargonException
	 */
	void modifyAvuValueBasedOnGivenAttributeAndUnit(String absolutePath,
			AvuData avuData) throws DataNotFoundException, JargonException;

	/**
	 * Modify the AVU metadata for a data object, giving the absolute path to
	 * the data object, as well as the current and desired AVU data.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param dataObjectAbsolutePath
	 *            <code>String</code> with the absolute path to the data object
	 * @param currentAvuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the existing Avu
	 * @param newAvuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the desired new AVU
	 * @throws DataNotFoundException
	 *             if the file or AVU was not found
	 * @throws JargonException
	 */
	void modifyAVUMetadata(String dataObjectAbsolutePath,
			AvuData currentAvuData, AvuData newAvuData)
			throws DataNotFoundException, JargonException;

	/**
	 * Modify the AVU metadata for a data object, giving the absolute path to
	 * the data object parent collection, and the data object file name, as well
	 * as the current and desired AVU data.
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param irodsCollectionAbsolutePath
	 *            <code>String</code> with the absolute path to the data object
	 * @param dataName
	 *            <code>String</code> with the data object name
	 * @param currentAvuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the existing Avu
	 * @param newAvuData
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the desired new AVU
	 * @throws DataNotFoundException
	 *             if the file or AVU was not found
	 * @throws JargonException
	 */
	void modifyAVUMetadata(String irodsCollectionAbsolutePath, String dataName,
			AvuData currentAvuData, AvuData newAvuData)
			throws DataNotFoundException, JargonException;

	/**
	 * Add the AVU Metadata for the given irods parent collection/data name
	 * <p/>
	 * Note that, in the case of a soft-linked path, the metadata is associated
	 * with the canonical file path, and AVU metadata associated with the
	 * canonical file path will be reflected if querying the soft link target
	 * path.
	 * 
	 * @param irodsCollectionAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS parent
	 *            collection
	 * @param dataName
	 *            <code>String</code> with the file name
	 *            {@link org.irods.jargon.core.pub.domain.AvuData} describing
	 *            the desired new AVU
	 * @throws JargonException
	 * @throws DataNotFoundException
	 *             when data object is missing
	 * @throws DuplicateDataException
	 *             when an AVU already exists. Note that iRODS (at least at 2.5)
	 *             is inconsistent, where a duplicate will only be detected if
	 *             units are not blank
	 */
	void addAVUMetadata(String irodsCollectionAbsolutePath, String dataName,
			AvuData avuData) throws DataNotFoundException, JargonException;

	/**
	 * List the user permissions for the given iRODS data object.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param irodsCollectionAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS data
	 *            object parent collection
	 * @param dataname
	 *            <code>String</code> with the name of the iRODS data Object
	 * @return <code>List</code> of {@link UserFilePermission} with the ACL's
	 *         for the given file.
	 * @throws JargonException
	 */
	List<UserFilePermission> listPermissionsForDataObject(
			String irodsCollectionAbsolutePath, String dataName)
			throws JargonException;

	/**
	 * List the user permissions for the given iRODS data object for a given
	 * user. Note that <code>null</code> will be returned if no permissions are
	 * available.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param irodsCollectionAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS data
	 *            object parent collection
	 * @param dataName
	 *            <code>String</code> with the name of the iRODS data Object
	 * @param userName
	 *            <code>String</code> with the name of the iRODS User
	 * @return <code>List</code> of {@link UserFilePermission} with the ACL's
	 *         for the given file.
	 * @throws JargonException
	 */
	UserFilePermission getPermissionForDataObjectForUserName(
			String irodsCollectionAbsolutePath, String dataName, String userName)
			throws JargonException;

	/**
	 * List the user permissions for the given iRODS data object for a given
	 * user. Note that <code>null</code> will be returned if no permissions are
	 * available.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param irodsAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS data
	 *            object
	 * @param userName
	 *            <code>String</code> with the name of the iRODS User
	 * @return <code>List</code> of {@link UserFilePermission} with the ACL's
	 *         for the given file.
	 * @throws JargonException
	 */
	UserFilePermission getPermissionForDataObjectForUserName(
			String irodsAbsolutePath, String userName) throws JargonException;

	/**
	 * Set the permissions on a data object to read for the given user as an
	 * admin. This admin mode is equivalent to the -M switch of the ichmod
	 * icommand.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionReadInAdminMode(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Set the permissions on a data object to write for the given user as an
	 * admin. This admin mode is equivalent to the -M switch of the ichmod
	 * icommand.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionWriteInAdminMode(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Set the permissions on a data object to own for the given user as an
	 * admin. This admin mode is equivalent to the -M switch of the ichmod
	 * icommand.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void setAccessPermissionOwnInAdminMode(String zone, String absolutePath,
			String userName) throws JargonException;

	/**
	 * Remove the permissions on a data object to own for the given user as an
	 * admin. This admin mode is equivalent to the -M switch of the ichmod
	 * icommand.
	 * <p/>
	 * Note that permissions are kept by the canonical path name. This method
	 * will find the canonical path if this is a soft link and operate on that
	 * data object.
	 * 
	 * @param zone
	 *            <code>String</code> with an optional zone for the file. Leave
	 *            blank if not used, it is not required.
	 * @param absolutePath
	 *            <code>String</code> with the absolute path to the data object.
	 * @param userName
	 *            <code>String</code> with the user name whose permissions will
	 *            be set.
	 * @throws JargonException
	 */
	void removeAccessPermissionsForUserInAdminMode(String zone,
			String absolutePath, String userName) throws JargonException;

	/**
	 * List the resources that have a copy of the given iRODS file
	 * <p/>
	 * Note that this method will follow a soft link and list the resources
	 * based on the canonical path.
	 * 
	 * @param irodsAbsolutePath
	 *            <code>String</code> with the absolute path to the iRODS file
	 *            that represents a data object.
	 * @return <code>List</code> of {@Resource} that represent the
	 *         resources in iRODS that have a copy of the file/
	 * @throws JargonException
	 */
	List<Resource> listFileResources(String irodsAbsolutePath)
			throws JargonException;

	/**
	 * Given a <code>ObjStat</code>, return a DataObject reflecting the
	 * representation of that data object in the iRODS iCAT. this
	 * <code>DataObject</code> takes special collection status into account, so
	 * that if this is a soft link, it will carry information about the
	 * canoncial path.
	 * 
	 * @param objStat
	 *            {@link ObjStat} reflecting the iRODS data object
	 * @return {@link DataObject} representing the iCAT data for the file in
	 *         iRODS
	 * @throws DataNotFoundException
	 * @throws JargonException
	 */
	DataObject findGivenObjStat(ObjStat objStat) throws DataNotFoundException,
			JargonException;

	/**
	 * Method to set access permission to the desired state, this variant makes
	 * it less necessary to stack 'if' tests in permission setting code.
	 * <p/>
	 * Note that <code>FilePermissionEnum</code> has more permission states
	 * defined then are currently supported by this method. This may require
	 * more iRODS core server updates to make this range of permissions
	 * meaningful.
	 * <p/>
	 * For the current variant of this method, only READ, WRITE, and OWN are
	 * supported, Other permission values will cause a
	 * <code>JargonException</code>. This may be relaxed in the future. Also
	 * note that NONE is supported, and actually causes the access permission to
	 * be removed.
	 * 
	 * @param zone
	 *            <code>String</code> with the zone for the user. This method
	 *            will work cross-zone if appropriate permissions are in place
	 * @param absolutePath
	 *            <code>String</code> with the absolute path for the data object
	 * @param userName
	 *            <code>userName</code> (just the name, no name#zone format) for
	 *            the user
	 * @param filePermission
	 *            {@link FilePermissionEnum}
	 * @throws JargonException
	 */
	void setAccessPermission(String zone, String absolutePath, String userName,
			FilePermissionEnum filePermission) throws JargonException;

	/**
	 * Find the data object (file) given it's unique id (the iCAT primary key)
	 * 
	 * @param id
	 *            <code>int</code> with the primary key for the data object in
	 *            the ICAT
	 * @return {@link DataObject} corresponding to the given id
	 * @throws FileNotFoundException
	 *             if hte id does not exist
	 * @throws JargonException
	 */
	DataObject findById(int id) throws FileNotFoundException, JargonException;

}
