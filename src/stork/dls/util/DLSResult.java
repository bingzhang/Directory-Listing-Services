package stork.dls.util;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import org.globus.ftp.FileInfo;
import stork.dls.ad.Ad;
import stork.dls.client.DLSGSIFTPClient;

/**
 * 
 * the permission format: http://www.askapache.com/security/chmod-stat.html#chmod-0-to-7777
 * 
 * @author bing
 *
 */

class Attributes{
	String mdtm = null;
	long size = 0;
	String owner = null;
	String group = null;
	int perm = 0;
	String format = null;
	boolean index = false;
}

public class DLSResult {
	private String root = null;
	private String host = null;
	//ad string stored in DB
	private String adString = null;
	


	/**
	 * dls internally uses subdirPathInfo to do prefetching.
	 * preparePrefetchingList is true
	 */
	private boolean preparePrefetchingList;
	private List<String> subdirPathInfo = null;
	
	public String getAdString(){
		return this.adString;
	}

	public String getJsonString(){
		//return this.jsonString;
		return this.adString;
	}
	
	public DLSResult(String absPath, boolean preparePrefetchingList, String hostname){
		this.root = absPath;
		this.host = hostname;
		this.preparePrefetchingList = preparePrefetchingList;
	}

	public List<String> getPrefetchSubdirPath(){
		return Collections.unmodifiableList(subdirPathInfo);
	}
	private static List<String> xmlDecode(String source){
		return XMLString.helpToScan(source);
	}
	
	private static Attributes getFileAttr(FileInfo fileInfo){
		Attributes attrs = new Attributes();
		
		String date = fileInfo.getDate();
		if(null != date){
			attrs.mdtm = date;
		}
		String time = fileInfo.getTime();
		if(null != time){
			attrs.mdtm +=" " + time;
		}
		
		long size = fileInfo.getSize();
    	if(0 < size){
    		attrs.size = size;
    	}
    	attrs.owner = fileInfo.getOwner();
    	attrs.group = fileInfo.getGroup();
    	if(DLSGSIFTPClient.CONTROLCHANNEL_LISTING){
    		attrs.perm  = fileInfo.getMode();
    	}else{
    		attrs.perm  = Integer.parseInt(fileInfo.getModeAsString());
    	}
        if(fileInfo.allCanRead() | fileInfo.groupCanRead()){
        	attrs.index = true;
        }
        return attrs;
	}
	
	private static Ad adEncode(final Vector<FileInfo> fileList, final Attributes attrs){
		Ad main = new Ad();
		for (FileInfo fileInfo : fileList) {
			String rel_path = fileInfo.getName();
			if (fileInfo.isDirectory()){
				if(rel_path.equals(".")){
					String date = fileInfo.getDate();
					if(null != date){
						attrs.mdtm = date;
					}
					String time = fileInfo.getTime();
					if(null != time){
						attrs.mdtm +=" " + time;
					}
					
					long size = fileInfo.getSize();
	            	if(0 < size){
	            		attrs.size = size;
	            	}
	            	attrs.owner = fileInfo.getOwner();
	            	attrs.group = fileInfo.getGroup();
	            	attrs.perm  = fileInfo.getMode();// octal
	            	/*
	            	if(DLSGSIFTPClient.CONTROLCHANNEL_LISTING){
	            		attrs.perm  = fileInfo.getMode();
	            	}else{
	            		attrs.perm  = Integer.parseInt(fileInfo.getModeAsString());
	            	}*/
	            	
	                if(fileInfo.allCanRead() | fileInfo.groupCanRead()){
	                	attrs.index = true;
	                }
					continue;
				}
				if(rel_path.equals("..")){
					continue;
				}
			}
			Ad ad = new Ad("name", rel_path);
			String date = fileInfo.getDate();
			if(null != date){
				attrs.mdtm = date;
			}
			String time = fileInfo.getTime();
			if(null != time){
				attrs.mdtm +=" " + time;
			}
			ad.put("mdtm", attrs.mdtm);
			
            if (fileInfo.isDirectory()){
            	ad.put("dir", true);
            	ad.put("size", 4096);
            }else{
            	long size = fileInfo.getSize();
            	if(0 < size){
            		ad.put("size", size);
            	}
            	ad.put("file", true);
            }
            //ad.put("perm", fileInfo.getMode());
            ad.put("owner", fileInfo.getOwner());
            ad.put("group", fileInfo.getGroup());
            ad.put("perm", fileInfo.getMode() );//octal
            /*
        	if(DLSGSIFTPClient.CONTROLCHANNEL_LISTING){
        		ad.put("perm", fileInfo.getMode() );//octal
        	}else{
        		ad.put("perm", Integer.parseInt(fileInfo.getModeAsString()) );//octal to decimal
        	}*/
            
            //if(fileInfo.allCanRead() | fileInfo.groupCanRead()){
            if(fileInfo.allCanRead()){
            	ad.put("indx", true);
            }
            main.put(ad);
        }
		return main;
	}
	
	//adString to jsonString
	public static void preparePrefetchingList(final String adString, final DLSResult dlsresult){
			//1. adString to Ad
	        Ad ad = Ad.parse(adString);
	        //3. generate subdirPathInfo
	        if(true == dlsresult.preparePrefetchingList){
	        dlsresult.subdirPathInfo = new LinkedList<String>();
	        if(ad.has("files")){
	        	for(Ad a : ad.getAds("files")){
	        		if(a.getBoolean("dir")){
	        			String subdir = a.get("name");
	        			if(subdir== null || subdir.equals(".") || subdir.equals("..")){
	        				continue;
	        			}
	        			//permission checking
	        			if(a.getBoolean("index")){
	        				dlsresult.subdirPathInfo.add(subdir);
	        			}
	        		}
	        	}
	        }
		}
	}

	public static void preparePrefetchingList(final Vector<FileInfo> fileList, final DLSResult dlsresult){
		if(null != dlsresult.subdirPathInfo){
			dlsresult.subdirPathInfo.clear();
		}
		dlsresult.subdirPathInfo = new LinkedList<String>();
		for (FileInfo fileInfo : fileList) {
			String rel_path = fileInfo.getName();
            if (fileInfo.isDirectory()){
    			if(null == rel_path || rel_path.equals(".") || rel_path.equals("..")){
            		continue;
            	}
    			//permission checking
    			if(fileInfo.allCanRead()/* || fileInfo.groupCanRead()*/){
    				dlsresult.subdirPathInfo.add(rel_path);	
    				//System.out.println(rel_path);
    			}
            }
		}
	}
	/**
	 * 
	 * vector to ad
	 * 
	 * @param fileList: only contains the relative path
	 * @param dlsresult: if not null, will store filelist into subdirPathInfo
	 * @return the AD format string which client requires
	 */
	public static String convertion(Vector<FileInfo> fileList, DLSResult dlsresult){
        if(null == fileList){
        	return null;
        }
        Ad ad = new Ad("name", dlsresult.root);
        Attributes attrs = null;
        ad.put("host", dlsresult.host);
        FileInfo fileInfo = fileList.get(0);
        Ad body = null;
        if(1 == fileList.size() && fileInfo.isFile()){
        	attrs = getFileAttr(fileInfo);
        	ad.put("file", true);
        }else{
        	attrs = new Attributes();
        	body = adEncode(fileList, attrs);
        	ad.put("dir", true);

        }
        ad.put("mdtm", attrs.mdtm);
        ad.put("size", attrs.size);
        ad.put("owner", attrs.owner);
        ad.put("group", attrs.group);
        ad.put("perm", attrs.perm);
        ad.put("index", attrs.index);
    	ad.put("files", body);                
        
        dlsresult.adString = ad.toString(false);
        if(true == dlsresult.preparePrefetchingList){
        	preparePrefetchingList(fileList, dlsresult);
        }
        return dlsresult.adString;
	}
	
	public void clear(){
		if(null != subdirPathInfo){
			subdirPathInfo.clear();
			subdirPathInfo = null;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}