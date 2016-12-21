package com.ibm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DefaultHttpClient httpClient = new DefaultHttpClient();
			System.out.println("Login into Indigo VMS web server");
			HttpPost postRequest = new HttpPost("http://it26.evm.online:9080/api/account/login");
			
			//final String password = "";
			StringEntity input = new StringEntity(
					"{\"userName\":\"" + "p_mariadoss" + "\",\"password\":\"" + "+P4ndian"+ "\"}");
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
		    	System.out.println(output);
			}
		    System.out.println("Logged in");

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.exit(-1);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	/*
	 * 
	 public static String readTestFile() 
	 {
		   //Variable to hold the one line data
           String jsondata = "";
           ConnectorLogger.loginfo("Reading BWC Test File");
	       //Name of the file
	       String fileName="c:\\IndigoVision.json";
	       try{
	          //Create object of FileReader
	          FileReader inputFile = new FileReader(fileName);
	          //Instantiate the BufferedReader Class
	          BufferedReader bufferReader = new BufferedReader(inputFile);
	          String line;
	          // Read file line by line and print on the console
	          while ((line = bufferReader.readLine()) != null)   {
	        	  jsondata=jsondata + line;
	            //System.out.println(line);
	          }
	          //Close the buffer reader
	          bufferReader.close();
	          
	       }catch(Exception e){
	          System.out.println("Error while reading file line by line:" + e.getMessage());                      
	       }
		return jsondata;
	 }
	 */

}
