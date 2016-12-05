import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class BulkPublisher {
	
	static final int batchSize = 100;
	static final String solrUrl = ".....";
	static XPath xPath =  XPathFactory.newInstance().newXPath();
	
	static String idexpr1 = "/pages/DocumentPage/div/SpecificationDocument/@id";
	static String instFileNameExpr1 = "/pages/DocumentPage/div/SpecificationDocument/@instanceFileName";
	static String contentsExpr1 = "/pages/DocumentPage/div/SpecificationDocument/Specification";
	
	static String idexpr2 = "/pages/DocumentPage/div/ClaimsDocument/@id";
	static String instFileNameExpr2 = "/pages/DocumentPage/div/ClaimsDocument/@instanceFileName";
	static String contentsExpr2 = "/pages/DocumentPage/div/ClaimsDocument/ClaimSet";
	
	static XPathExpression exp1, exp2, exp3, exp4, exp5, exp6;	
	List<Page> docs = new ArrayList<Page>();
	
	static Pattern clmPattern = Pattern.compile("(?u)(CLM)");
	static Pattern abstPattern = Pattern.compile("(?u)(ABST)");
	static Pattern specPattern = Pattern.compile("(?u)(SPEC)");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			exp1 = xPath.compile(idexpr1);
			exp2 = xPath.compile(instFileNameExpr1);
			exp3 = xPath.compile(contentsExpr1);
			
			exp4 = xPath.compile(idexpr2);
			exp5 = xPath.compile(instFileNameExpr2);
			exp6 = xPath.compile(contentsExpr2);
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Started fetching records from database: "+ new Date());
		List<DbResultset> resultList = selectRecords();
		System.out.println("Finished fetching records from database: "+ new Date());
		System.out.println("Number of records fetched from database: " + resultList.size());
		
		System.out.println("---");
		System.out.println("Started loading and parsing and indexing xml files: "+ new Date());
		loadFile(resultList);		
		System.out.println("Finished loading and parsing and indexing xml files: "+ new Date());
		
		
	}

	
	


	public static List<DbResultset> selectRecords() {
		List<DbResultset> list = new ArrayList<DbResultset>();
		boolean success = false;
		Connection conn = null;
		Statement statement = null;
		
		try{
			//register the driver
			Class.forName(Constants.JDBC_DRIVER);
			
			//get the connection
			conn = DriverManager.getConnection(Constants.DB_URL, Constants.userName, Constants.password);
			
			//create the statement
			statement = conn.createStatement();
			
			String sql = "select res.DOCUMENT_LOCATION_TX as path, rend.FK_DOCUMENT_ID as docId"+ 
					" from DOCUMENT_RESOURCE res inner join DOCUMENT_RENDITION rend ON res.FK_DOCUMENT_RENDITION_ID = rend.DOCUMENT_RENDITION_ID AND rend.DELETE_IN = 0 "+
					" inner join DOCUMENT doc ON rend.FK_DOCUMENT_ID = doc.DOCUMENT_ID and doc.DELETE_IN = 0 "+
					" inner join STND_RENDITION_TYPE srt on srt.STND_RENDITION_TYPE_ID = rend.FK_STND_RENDITION_TYPE_ID  and srt.RENDITION_TYPE_CD = 'xml' "+ 
					" where  res.DELETE_IN = 0  and  doc.FK_CLAIM_ID is null";						   
			
			//execute the statement
			ResultSet rs = statement.executeQuery(sql);
			
			while(rs.next()){
				DbResultset dbr = new DbResultset();
				dbr.setFilePath(rs.getString(1));
				dbr.setDocId(rs.getLong(2));
				list.add(dbr);
			}
			rs.close();
		} catch(Exception e){
			e.printStackTrace();
		}finally{
			//close the resources
			try{
				if(statement != null){
					statement.close();
				}
			}catch(Exception e1){
				e1.printStackTrace();
			}
			try{
				if(conn != null){
					conn.close();
				}
			}catch(Exception e1){
				e1.printStackTrace();
			}
		}	
		return list;
	}
	
	private static void loadFile(List<DbResultset> resultList) {
		List<Page> pageList = new ArrayList<Page>();
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		
		
		Iterator<DbResultset> resultSetIter = resultList.iterator();
		long count = 0;
		
		while(resultSetIter.hasNext()) {
			DbResultset dbr = resultSetIter.next();
			Page page = parseXmlDoc(dbr, dbFactory);
			if (page != null) {
				pageList.add(page);
			}
			if (pageList.size() >= batchSize) {
				count += pageList.size();
				System.out.println("reached batch size parsing xml files- sending to solr , total count = " + count + "at "+ new Date());
				addToSolrServer(pageList);	
				System.out.println("Finished indexing xml files: "+ new Date());
				System.out.println("Number of docs indexed: " + count);
				
				pageList.clear();
			}
		}
		
		if (pageList.size() > 0) {
			count += pageList.size();
			
			System.out.println("Started indexing xml files: "+new Date());
			addToSolrServer(pageList);	
			System.out.println("Finished indexing xml files: "+ new Date());
			System.out.println("Number of docs indexed: " + pageList.size());
			
			pageList.clear();
		}
	}
	
	private static Page parseXmlDoc(DbResultset dbr, DocumentBuilderFactory dbFactory) {
		StringBuilder sb = new StringBuilder(Constants.rootPath).append(dbr.getFilePath()).append("full.xml");
		DocumentBuilder dBuilder;
		
		File fXmlFile = new File(sb.toString());
		Page page = null;

		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			String id = "";
			String instanceFileName = "";
			String contents = "";
			String docType = "";
			String caseNumber = "";

			NodeList nodeList = (NodeList) exp1.evaluate(doc, XPathConstants.NODESET);
			// specifications or abstract
			if (nodeList != null && nodeList.getLength() > 0) {
				id = nodeList.item(0).getTextContent();
				NodeList nodeList2 = (NodeList) exp2.evaluate(doc, XPathConstants.NODESET);
				if (nodeList2 != null && nodeList2.getLength() > 0) {
					instanceFileName = nodeList2.item(0).getTextContent();

					Matcher m1 = abstPattern.matcher(instanceFileName);
					Matcher m2 = specPattern.matcher(instanceFileName);
					docType = (m1.find()) ? m1.group(0) : ((m2.find()) ? m2.group(0) : "");
					caseNumber = retriveCaseNumber(instanceFileName);
				}
				NodeList nodeList3 = (NodeList) exp3.evaluate(doc, XPathConstants.NODESET);
				if (nodeList3 != null && nodeList3.getLength() > 0) {
					contents = nodeList3.item(0).getTextContent();
				}
			}

			// claims
			nodeList = (NodeList) exp4.evaluate(doc, XPathConstants.NODESET);
			if (nodeList != null && nodeList.getLength() > 0) {
				id = nodeList.item(0).getTextContent();
				// System.out.println("id : " + id);
				NodeList nodeList2 = (NodeList) exp5.evaluate(doc, XPathConstants.NODESET);
				if (nodeList2 != null && nodeList2.getLength() > 0) {
					instanceFileName = nodeList2.item(0).getTextContent();
					Matcher m = clmPattern.matcher(instanceFileName);
					docType = (m.find()) ? m.group(0) : "";
					caseNumber = retriveCaseNumber(instanceFileName);
				}
				NodeList nodeList3 = (NodeList) exp6.evaluate(doc,
						XPathConstants.NODESET);
				if (nodeList3 != null && nodeList3.getLength() > 0) {
					contents = nodeList3.item(0).getTextContent();
				}
			}
			page = new Page(id, instanceFileName, contents, docType, caseNumber, dbr.getDocId());
			return page;

		} catch (ParserConfigurationException | SAXException | IOException
				| XPathExpressionException e) {
			// TODO Auto-generated catch block
			System.out.println("caught error parsing xml file " + sb.toString()
					+ " at " + new Date());
			e.printStackTrace();
		}
		return page;
	}


	private static String retriveCaseNumber(String instanceFileName) {
		String[] strArr = instanceFileName.split("\\.");
		if (strArr.length > 0) 
			return strArr[0];
		else 
			return "";
	}



	private static void addToSolrServer(List<Page> pageList) {
		SolrServer solrServer = new HttpSolrServer(solrUrl);		
    	DefaultIndexer defaultIndexer = new DefaultIndexer(solrServer, batchSize);
    
		SolrInputDocPageIterator solrPageItr = new SolrInputDocPageIterator(pageList.iterator());
		List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();
		while (solrPageItr.hasNext()) {
			SolrInputDocument inputDoc = solrPageItr.next();
			docList.add(inputDoc);
		}
		if (!docList.isEmpty()) {
			defaultIndexer.index(docList.iterator());
		}
	}
	
	
	
	
	/*
	private static void createSolrDocument(Page page){
		SolrServer solrServer = new HttpSolrServer(solrUrl);		
        DefaultIndexer defaultIndexer = new DefaultIndexer(solrServer, batchSize);
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", page.getId());
        doc.addField("instanceFileName", page.getInstanceFileName());
        doc.addField("contents", page.getContents());    
        doc.addField("documentType", page.getDocumentType());
        doc.addField("caseNumber", page.getCaseNumber());
        doc.addField("documentId", page.getDocumentId());
        defaultIndexer.index(doc);
	}*/
	
}
