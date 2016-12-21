package com.ibm;

import java.util.Properties;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

public class ConnectorAdmin extends Thread{
	static MQQueueManager qMgr = null;
	static MQQueue Adminqueue;
	
	public void connectToQMGR(	){
		// Create a connection to the QueueManager
	 	ConnectorLogger.loginfo("Connecting to Admin queue manager: " + BWCConnector.QMGRName);
		Properties props =  new Properties();
		 props.put(CMQC.HOST_NAME_PROPERTY, BWCConnector.Host);
		 props.put(CMQC.USER_ID_PROPERTY, BWCConnector.Userid);
		 props.put(CMQC.PASSWORD_PROPERTY, BWCConnector.Password);
	     props.put(CMQC.CHANNEL_PROPERTY, BWCConnector.SrvChannel);
	     props.put(CMQC.PORT_PROPERTY, BWCConnector.Port); // port number
	     props.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES);
	     
		try {
			qMgr = new MQQueueManager(BWCConnector.QMGRName, props);
		} catch (MQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		ConnectorLogger.loginfo("Successfully Connected to Admin queue manager: " + BWCConnector.QMGRName);
		// Set up the options on the queue we wish to open
		int openInputOptions = CMQC.MQOO_FAIL_IF_QUIESCING | CMQC.MQOO_OUTPUT;
		// Now specify the queue that we wish to open and the open options
		ConnectorLogger.loginfo("Accessing admin queue: " + BWCConnector.Admin_Q);
		try {
			Adminqueue = qMgr.accessQueue(BWCConnector.Admin_Q, openInputOptions);
		} catch (MQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			ConnectorLogger.logsevere(e.getMessage());
			System.exit(-1);
		}
 	}
	
	public void writeMetaDataToQueue() {
		try {
			ConnectorLogger.loginfo("Sending i'm alive message to admin_q");
			if (qMgr == null)
				connectToQMGR();
			// Specify default get message options
			MQPutMessageOptions pmo = new MQPutMessageOptions();
			// Define a simple WebSphere MQ Message ...
			MQMessage msg = new MQMessage();
			msg.expiry  = 300; // expiry is in tenths of a second; 30 seconds
			// ... and write some text in UTF8 format
			msg.writeString("{\"Status\":\"1\" }");
			// Put the message to the queue
			Adminqueue.put(msg, pmo);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (this.isAlive()){
			try {
				writeMetaDataToQueue();
				ConnectorLogger.loginfo("Wait 30 sceonds and send heart beat");
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				ConnectorLogger.logsevere(e.getMessage());
				System.exit(-1);
			}
		}
	}	

}
