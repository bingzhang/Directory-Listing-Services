package stork.dls.util;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.net.ftp.FTPFile;
import org.globus.ftp.FileInfo;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * string in XML format
 * @author bing
 *
 */
public class XMLString {
	private final String BYTES_TOKEN = " bytes";
	private final int BYTES_TOKEN_LEN = BYTES_TOKEN.length();
	private String xmlstring = null;
	
	public static List<String> helpToScan(String source){
		final String separator = File.separator;
		DocumentBuilder db = null;
		StringBuilder xmlResult = new StringBuilder(source);
		DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();

		try {
			db = dbfac.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		
		Document doc = null;
		try {
			doc = db.parse(new InputSource(new StringReader(xmlResult.toString())));
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		org.w3c.dom.Element rootElem = doc.getDocumentElement();
		final String abs_path = rootElem.getAttribute("RootDirectoryPath");
		NodeList nl = doc.getElementsByTagName("Directory");
		int totalDirs = nl.getLength();
		LinkedList<String> pathInfo = new LinkedList<String>();
		
		for (int i = 0; i < totalDirs; i++) {
			String rel_path = nl.item(i).getTextContent();
			if(rel_path.equals(".") || rel_path.equals("..")){
        		continue;
        	}
        	
        	StringBuffer sb = new StringBuffer();
        	sb.append(abs_path);
        	sb.append(rel_path);
        	sb.append(separator);
        	String target_dir = sb.toString();
        	pathInfo.add(target_dir);
		}
		return pathInfo;
	}
	
	public String getXMLString(FTPFile[] ftpFiles, String root_path){
		org.w3c.dom.Document document = null;
		org.w3c.dom.Element xml_element = null;
		try {
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
			document = docBuilder.newDocument();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		xml_element = document.createElement("DirectoryListing");
		xml_element.setAttribute("RootDirectoryPath", root_path);
		document.appendChild(xml_element);
		
		if(null == ftpFiles){
			org.w3c.dom.Element entry = null;
			entry = document.createElement("Directory");
			entry.setAttribute("LastUpdated", null);
			entry.setTextContent(".");
			xml_element.appendChild(entry);
			entry = document.createElement("Directory");
			entry.setAttribute("LastUpdated", null);
			entry.setTextContent("..");
			xml_element.appendChild(entry);
		}else{
			for (int i = 0; i < ftpFiles.length; i++) {
				org.w3c.dom.Element entry = null;
				String lastUpdated = ftpFiles[i].getTimestamp().getTime().toString();
				String fileName = ftpFiles[i].getName();
				if (ftpFiles[i].isDirectory()){
					entry = document.createElement("Directory");
					entry.setAttribute("LastUpdated", lastUpdated);
					entry.setTextContent(fileName);
					xml_element.appendChild(entry);
				}else{
					entry = document.createElement("File");
					int len = BYTES_TOKEN_LEN;
					len += String.valueOf(ftpFiles[i].getSize()).length();
					StringBuilder sbsize = new StringBuilder(len);
					sbsize.append(String.valueOf(ftpFiles[i].getSize()));
					sbsize.append(BYTES_TOKEN);
					entry.setAttribute("Size", sbsize.toString());
					entry.setAttribute("LastUpdated", lastUpdated);
					entry.setTextContent(fileName);
					xml_element.appendChild(entry);
				}
			}
		}
		
		TransformerFactory transfac = TransformerFactory.newInstance();
		Transformer trans = null;
		try {
			trans = transfac.newTransformer();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
			return null;
		}
		trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		trans.setOutputProperty(OutputKeys.INDENT, "yes");

		StringWriter sw = new StringWriter();
		StreamResult result = new StreamResult(sw);
		DOMSource source = new DOMSource(document);
		try {
			trans.transform(source, result);
		} catch (TransformerException e) {
			e.printStackTrace();
			return null;
		}
		xmlstring = sw.toString();		
		return xmlstring;
	}
	public String getXMLString(Vector<FileInfo> fileList, String root_path){
		org.w3c.dom.Document document = null;
		org.w3c.dom.Element xml_element = null;
		try {
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
			document = docBuilder.newDocument();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		xml_element = document.createElement("DirectoryListing");
		xml_element.setAttribute("RootDirectoryPath", root_path);
		document.appendChild(xml_element);
		
		if(null == fileList){
			org.w3c.dom.Element entry = null;
			entry = document.createElement("Directory");
			entry.setAttribute("LastUpdated", null);
			entry.setTextContent(".");
			xml_element.appendChild(entry);
			entry = document.createElement("Directory");
			entry.setAttribute("LastUpdated", null);
			entry.setTextContent("..");
			xml_element.appendChild(entry);
		}else{
			for (FileInfo fii : fileList) {
				String fileName = fii.getName();
				if (fii.isDirectory()){
					org.w3c.dom.Element entry = null;
					entry = document.createElement("Directory");
					entry.setAttribute("LastUpdated", fii.getTime());
					entry.setTextContent(fileName);
					xml_element.appendChild(entry);
				}
			}
			for (FileInfo fii : fileList) {
				String fileName = fii.getName();
				if (fii.isFile()){
					org.w3c.dom.Element entry = null;
					entry = document.createElement("File");
					int len = BYTES_TOKEN_LEN;
					len += String.valueOf(fii.getSize()).length();
					StringBuilder sbsize = new StringBuilder(len);
					sbsize.append(String.valueOf(fii.getSize()));
					sbsize.append(BYTES_TOKEN);
					entry.setAttribute("Size", sbsize.toString());
					entry.setAttribute("LastUpdated", fii.getTime());
					entry.setTextContent(fileName);
					xml_element.appendChild(entry);
				}
			}			
		}
		
		TransformerFactory transfac = TransformerFactory.newInstance();
		Transformer trans = null;
		try {
			trans = transfac.newTransformer();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
			return null;
		}
		trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		trans.setOutputProperty(OutputKeys.INDENT, "yes");

		StringWriter sw = new StringWriter();
		StreamResult result = new StreamResult(sw);
		DOMSource source = new DOMSource(document);
		try {
			trans.transform(source, result);
		} catch (TransformerException e) {
			e.printStackTrace();
			return null;
		}
		xmlstring = sw.toString();		
		return xmlstring;
	}
	
	public static void main(String[] args) {
	}

}
