package stork.dls.client;


import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.globus.ftp.FileInfo;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.CollectionAndDataObjectListAndSearchAO;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.domain.UserFilePermission;
import org.irods.jargon.core.query.CollectionAndDataObjectListingEntry;

import stork.dls.io.network.DLSFTPMetaChannel;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;


public class iRODSJargonClient extends DLSClient{

	
	public iRODSJargonClient(String host, int port, DLSStream input){
		super(host, port, input);
	}
	
	@Override
    public CollectionAndDataObjectListAndSearchAO authenticate(final String host, final int port,
			final String userName, final String password,
			final String homeDirectory, final String userZone,
			final String defaultStorageResource) throws Exception {
		IRODSAccount irodsAccount = new IRODSAccount(host, port, userName, password, homeDirectory,
        		userZone, defaultStorageResource);
		System.out.println(irodsAccount);
		
        IRODSFileSystem irodsFileSystem = null;
        try {
            irodsFileSystem = IRODSFileSystem.instance();
        } catch (JargonException e1) {
            e1.printStackTrace();
        }
        CollectionAndDataObjectListAndSearchAO actualCollection = null;
        try {
            actualCollection = irodsFileSystem
                    .getIRODSAccessObjectFactory()
                    .getCollectionAndDataObjectListAndSearchAO(irodsAccount);
        } catch (JargonException e1) {
            e1.printStackTrace();
        }
        return actualCollection;
    }
	
    public iRODSJargonClient(DLSStream dlsirodsStream) {
    	super(dlsirodsStream);
	}

	public Vector<FileInfo> list(
            final DLSListingTask listingtask, 
            final CollectionAndDataObjectListAndSearchAO actualCollection, 
            final String threadAssignedName,
            final String targetIrodsCollection) throws Exception{ 
    	Vector fileList = new Vector();
        List<CollectionAndDataObjectListingEntry> entries = null;
        try {
        	//entries = actualCollection.listDataObjectsAndCollectionsUnderPath(targetIrodsCollection);
        	entries = actualCollection.listDataObjectsAndCollectionsUnderPathWithPermissions(targetIrodsCollection);
        } catch (org.irods.jargon.core.exception.FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (JargonException e1) {
            e1.printStackTrace();
        }
        String loginuser = listingtask.getUserName();
        Iterator<CollectionAndDataObjectListingEntry> datacursor = entries.iterator();
        while(datacursor.hasNext()){
        	CollectionAndDataObjectListingEntry entry = datacursor.next();
        	FileInfo fileinfo = new FileInfo();
        	fileList.add(fileinfo);
        	System.out.println("type: " + entry.getObjectType());
        	switch (entry.getObjectType()){
        	case DATA_OBJECT:
        		fileinfo.setFileType(FileInfo.FILE_TYPE);
        		break;
        	case COLLECTION:
        		fileinfo.setFileType(FileInfo.DIRECTORY_TYPE);
        		break;
        	default:
        		fileinfo.setFileType(FileInfo.UNKNOWN_TYPE);
        		break;
        	}
            System.out.println("name: " + entry.getNodeLabelDisplayValue());        	
        	fileinfo.setName(entry.getNodeLabelDisplayValue());
        	System.out.println("size: " + entry.getDataSize());
        	fileinfo.setSize(entry.getDataSize());
        	System.out.println("owner: " + entry.getOwnerName());
        	fileinfo.setOwner(entry.getOwnerName());
            System.out.println("Ctime: " + entry.getCreatedAt());
            fileinfo.setDate(entry.getCreatedAt().toString());
            System.out.println("Mtime: " + entry.getModifiedAt());
            fileinfo.setDate(entry.getModifiedAt().toString());
            //System.out.println("permission: " + entry.getUserFilePermission());
            List<UserFilePermission> permissionlist = entry.getUserFilePermission();
            Iterator<UserFilePermission> iter = permissionlist.iterator();
            fileinfo.setUserCanRead();
            while(iter.hasNext()){
            	UserFilePermission perm = iter.next();
            	//System.out.println("user: " + perm.getUserName());
            	if(perm.getUserName().equals(loginuser)){
            		fileinfo.setUserCanWrite();
            	}
            	//System.out.println("permission: " + perm.getFilePermissionEnum());
            }
            System.out.println("abs path: " + entry.getFormattedAbsolutePath());
        }
		return fileList;
    }
}
