package com.ibm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;


public class BWCConnector extends Thread{
	
	static BWCConnector client;
	static ConnectorAdmin Admincon;
	
	static String PollFrequency = null;
	static String QMGRName = null;
	static String MetaData_Q = null;
	static String Host = null;
	static String SrvChannel = null;
	static String Userid = null;
	static String Password = null;
	static String Port = null;
	static String Admin_Q = null;
	
	
	static MQQueueManager qMgr = null;
	static MQQueue MetaDataqueue;
	
	static String BWC_Webserver = null;
	static String BWC_User =  null;
	static String BWC_Password =  null;
	static String BWC_LIMIT = "25";//default
	static String BWC_MODE = "0";//automatic default
	static String BWC_FILE = "";//default (PSS-20151207-123639UTC-0500.mp4)
	static String BWC_DEVICE = "401155";//default
	static String BWC_TIMEZONE = "EST";
	
	private static Long startTime=0L;
	private static String TopMostProcessedFile="";
	private static String LastRunProcessedFile="";

	//DefaultHttpClient httpClient = new DefaultHttpClient();
	HttpClient httpClient = HttpClientBuilder.create().build();
	
	boolean isloggedIn = false;
	static String ConnectorName = null;
	Properties lastrunprop = new Properties();
	String lastrunpropFileName = "lastrun";
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		// create a second calendar with different timezone and locale
		Locale locale= Locale.US;
		TimeZone tz = TimeZone.getTimeZone(BWC_TIMEZONE);
		Calendar cal = Calendar.getInstance(tz, locale);
		while (client.isAlive()){
			//Date today = cal.getTime();
			cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH),0,0,0);
			Date today = cal.getTime();
			Long todayMs = today.getTime(); //today 
			ConnectorLogger.loginfo("Today's date is " + today.toString() + " MilliSeconds: " + todayMs + "\n");
			
			LoadandSetlastrun(0);
			if (todayMs > startTime){
				ConnectorLogger.loginfo("New start time " + cal.get(Calendar.YEAR)+ cal.get(Calendar.MONTH)+ cal.get(Calendar.DAY_OF_MONTH));
				startTime=todayMs;
				LoadandSetlastrun(2);
			}
			ConnectorLogger.loginfo("Next run...");
			if (!isloggedIn)
				client.login();
			String metaData = client.getMetaData();
			//String metaData = readTestFile();
			client.parseMetaData(metaData);
			try {
				ConnectorLogger.loginfo("Wait for 15 sceonds before next run");
				Thread.sleep(Integer.parseInt(PollFrequency));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				ConnectorLogger.logsevere(e.getMessage());
				System.exit(-1);
			}
		}
	}      
	
	public void login() {
		try {
			ConnectorLogger.loginfo("Login into BWC VMS web server");
			HttpPost postRequest = new HttpPost(BWC_Webserver + "api/account/login");
	
			//final String password = "";
			StringEntity input = new StringEntity(
					"{\"userName\":\"" + BWC_User + "\",\"password\":\"" + BWC_Password + "\"}");
			input.setContentType("application/json");
			postRequest.setEntity(input);

			HttpResponse response = httpClient.execute(postRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			String output = null;
			// System.out.println("Output from Server .... \n");
		    while ((output = br.readLine()) != null) {
		    	ConnectorLogger.loginfo(output);
			}
		    isloggedIn = true;
		    ConnectorLogger.loginfo("Logged in");

		} catch (MalformedURLException e) {
			e.printStackTrace();
			ConnectorLogger.logsevere(e.getMessage());
			System.exit(-1);

		} catch (IOException e) {
			e.printStackTrace();
			ConnectorLogger.logsevere(e.getMessage());
			System.exit(-1);
		}

	}
		
	public String getMetaData() {
		try {
			ConnectorLogger.loginfo("Getting meta-data from the VMS");
			ConnectorLogger.logconfig("Query Limit is " + BWC_LIMIT);
			//HttpGet getRequest = new HttpGet(BWC_Webserver + "api/videos?order=download&start=" + startTimeMinusPollTime.toString());
			HttpGet getRequest=null;
			//To get downloads from all device
			if (BWC_MODE.equals("0")){
				getRequest = new HttpGet(BWC_Webserver + "api/videos?order=download&limit=" + BWC_LIMIT);
			}else{
				//To get downloads from a specific device in the config.json
				ConnectorLogger.logconfig("MANUAL PROCESS");		
				getRequest = new HttpGet(BWC_Webserver + "api/videos?order=download&device=" + BWC_DEVICE);		
			}
			StringEntity input = new StringEntity("{}");
			input.setContentType("application/json");
			HttpResponse response = httpClient.execute(getRequest);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			String output;
			StringBuffer buf = new StringBuffer();
			while ((output = br.readLine()) != null) {
				// System.out.println(output);
				buf.append(output);
			}
			br.close();
			return buf.toString();

		} catch (Exception e) {

			e.printStackTrace();
			ConnectorLogger.logsevere(e.getMessage());
		}
		return null;
	}

	public void parseMetaData(String json) {
		
		JSONParser parser = new JSONParser();
		try {
			
			Object jsonObj = parser.parse(json);
			JSONArray array = (JSONArray) jsonObj;
			//List<JSONObject> metaDataArray = new ArrayList<JSONObject>();
			ConnectorLogger.loginfo("Number of meta-data pulled is ["+ array.size() +"]");
			for (int i = 0; i < array.size(); i++) {
				JSONObject jsonMetaData = (JSONObject) array.get(i);

				if (i == 0){
					//store the topmost filename in the array as last run filename
					TopMostProcessedFile = (String) jsonMetaData.get("name");
				}
				String fileName = (String) jsonMetaData.get("name");
				Long startTime = (Long) jsonMetaData.get("startTime");
				Date startTimeInDate = new Date(startTime);
				JSONObject owner = (JSONObject) jsonMetaData.get("owner");
				String ownerName = (String) owner.get("name");
				String deviceName = (String) jsonMetaData.get("deviceName");
				Long incidentCount = (Long) jsonMetaData.get("incidentCount");
				
				ConnectorLogger.loginfo("fileName: " + fileName + " StartTime: " + startTimeInDate.toString()
						+ " ownerName: " + ownerName + " deviceName: " + deviceName + " incidentcount: " + incidentCount);
				
				ConnectorLogger.loginfo("Cecking if the file name [" + fileName + "] is equal lastest file processed [" + LastRunProcessedFile + "]");
				if (fileName.equalsIgnoreCase(LastRunProcessedFile)){
					ConnectorLogger.loginfo("It is equal to last run. No more new recordings.....");
					break;
				}else{
					if (BWC_MODE.equals("0")){//automatic
						if (incidentCount.intValue()==0){
							ConnectorLogger.loginfo("It is not equal. Processing file name [" + fileName + "]...");
							writeMetaDataToQueue(fileName, jsonMetaData.toJSONString());
						}else{
							ConnectorLogger.loginfo("Cannot Process Incident Files. The file name [" + fileName + "]...");
						}
					}else{//manual
						if (incidentCount.intValue()==0 && fileName.equalsIgnoreCase(BWC_FILE)){
							ConnectorLogger.loginfo("Found the File. Processing file name [" + fileName + "]...");
							writeMetaDataToQueue(fileName, jsonMetaData.toJSONString());
							System.exit(0);
						}else{
							ConnectorLogger.loginfo("Not Found or Incident status is 1. The file name [" + fileName + "]...");
						}
					}
				}						
			}
			LastRunProcessedFile=TopMostProcessedFile;
		} catch (Exception e) {
			e.printStackTrace();
			ConnectorLogger.logsevere(e.getMessage());
		}
		ConnectorLogger.loginfo("This run lastest filename is " + LastRunProcessedFile);
		LoadandSetlastrun(1);//set the LastRunProcessedFile property to lastrun file
	}
	

	public void writeMetaDataToQueue(String filename, String metaData) {
		try {
			ConnectorLogger.loginfo("Sending meta-data to queue for " + filename);
			ConnectorLogger.loginfo("JSON string is [" + metaData + "]");
			if (qMgr == null)
				connectToQMGR();
			// Specify default get message options
			MQPutMessageOptions pmo = new MQPutMessageOptions();
			// Define a simple WebSphere MQ Message ...
			MQMessage msg = new MQMessage();
			// ... and write some text in UTF8 format
			msg.writeString(metaData);
			// Put the message to the queue
			ConnectorLogger.loginfo("Sending a message to the queue.");
			MetaDataqueue.put(msg, pmo);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	
	public static String readConfigInformation(){
		 ConnectorLogger.logconfig("Reading Configuration Information for BWC Connector");
	       String configJson =  "";
		   try{

	          //Create object of FileReader
	          FileReader inputFile = new FileReader("config.json");

	          //Instantiate the BufferedReader Class
	          BufferedReader bufferReader = new BufferedReader(inputFile);

	          String line;
	          // Read file line by line and print on the console
	          while ((line = bufferReader.readLine()) != null)   {
	        	  configJson=configJson + line;
	            //System.out.println(line);
	          }
	          //Close the buffer reader
	          bufferReader.close();
	          
	       }catch(Exception e){
	    	   ConnectorLogger.logsevere("Error while reading file line by line:" + e.getMessage());
	          System.exit(-1);
	       }
	       return configJson;	 
	}
	 
	 public static void setConfigformation(String configJson) {
		 	ConnectorLogger.logconfig("Setting Configurion Information for BWC Connector");
			JSONParser parser = new JSONParser();
			try {
				Object jsonObj = parser.parse(configJson);
				JSONArray array = (JSONArray) jsonObj;
				
				for (int i = 0; i < array.size(); i++) {
					JSONObject jsonMetaData = (JSONObject) array.get(i);

					ConnectorLogger.logconfig(i + " : " + jsonMetaData);
				
					ConnectorName = (String) jsonMetaData.get("ConnectorName");
					//connector information
					QMGRName = (String) jsonMetaData.get("QMGR");
					MetaData_Q = (String) jsonMetaData.get("MetaData_Q");
					Host = (String) jsonMetaData.get("Host");
					SrvChannel = (String) jsonMetaData.get("SrvChannel");
					Userid = (String) jsonMetaData.get("Userid");
					Password = (String) jsonMetaData.get("Password");
					BWC_Webserver = (String) jsonMetaData.get("BWC_Webserver");
					BWC_User = (String) jsonMetaData.get("BWC_User");
					BWC_Password = (String) jsonMetaData.get("BWC_Password");
					BWC_LIMIT = (String) jsonMetaData.get("BWC_LIMIT");
					BWC_MODE = (String) jsonMetaData.get("BWC_MODE");
					BWC_FILE = (String) jsonMetaData.get("BWC_FILE");
					BWC_DEVICE = (String) jsonMetaData.get("BWC_DEVICE");
					BWC_TIMEZONE = (String) jsonMetaData.get("BWC_TIMEZONE");
					PollFrequency = (String) jsonMetaData.get("PollFrequency");
					Port = (String) jsonMetaData.get("Port");
					Admin_Q = (String) jsonMetaData.get("Admin_Q");
				}	
			}
			catch (Exception exp){
				exp.printStackTrace();
				ConnectorLogger.logsevere(exp.getMessage());
				System.exit(-1);
			}
		}	
	 
	 	public static void connectToQMGR(){
			// Create a connection to the QueueManager
		 	ConnectorLogger.loginfo("Connecting to queue manager: " + QMGRName);
			Properties props =  new Properties();
			 props.put(CMQC.HOST_NAME_PROPERTY, Host);
			 props.put(CMQC.USER_ID_PROPERTY, Userid);
			 props.put(CMQC.PASSWORD_PROPERTY, Password);
		     props.put(CMQC.CHANNEL_PROPERTY, SrvChannel);
		     props.put(CMQC.PORT_PROPERTY, Port); // port number
		     props.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES);
			try {
				qMgr = new MQQueueManager(QMGRName, props);
			} catch (MQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(-1);
			}
			ConnectorLogger.loginfo("Successfully Connected to queue manager: " + QMGRName);
			// Set up the options on the queue we wish to open
			int openInputOptions = CMQC.MQOO_FAIL_IF_QUIESCING | CMQC.MQOO_OUTPUT;
			// Now specify the queue that we wish to open and the open options
			ConnectorLogger.loginfo("Accessing queue: " + MetaData_Q);
			try {
				MetaDataqueue = qMgr.accessQueue(MetaData_Q, openInputOptions);
			} catch (MQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				ConnectorLogger.logsevere(e.getMessage());
				System.exit(-1);
			}
	 	}
	 
	 	public void LoadandSetlastrun(int mode){
	 		InputStream ifslastrun = null;
			try {
				ifslastrun = new FileInputStream(lastrunpropFileName);
				if (ifslastrun!=null){
					lastrunprop.load(ifslastrun);
					ifslastrun.close();
					if (mode==0){
	 				//read the property
						LastRunProcessedFile = lastrunprop.getProperty("lastrun");
						startTime = Long.valueOf(lastrunprop.getProperty("startTime"));
					}else if (mode==1){
	 				//write to property
						lastrunprop.setProperty("lastrun", LastRunProcessedFile);
						FileOutputStream out = new FileOutputStream("lastrun");
						lastrunprop.store(out, "--- Last latest Meta-Data Filename ---");
						out.close();
					}else if (mode==2){
						//write to property
						lastrunprop.setProperty("startTime", startTime.toString());
						lastrunprop.setProperty("lastrun", LastRunProcessedFile);
						FileOutputStream out = new FileOutputStream("lastrun");
						lastrunprop.store(out, "--- Meta-Data Date ---");
						out.close();
					}
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
	 	}
	 	
	 	public static void main(String[] args) {
	 		ConnectorLogger.setlog();
	 		//Meta-data connector agent
			client = new BWCConnector();
			String configJson = BWCConnector.readConfigInformation();
			BWCConnector.setConfigformation(configJson);
			connectToQMGR();
			client.start();
			//Admin connector agent
			Admincon = new ConnectorAdmin();
			Admincon.connectToQMGR();
			Admincon.start();
		}
	 	 
}
