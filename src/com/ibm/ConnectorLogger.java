package com.ibm;

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.IOException;

import java.util.logging.FileHandler;
import java.util.logging.Handler;

public class ConnectorLogger {
	public static final Logger LOGGER = Logger.getLogger("BWC Connector");
	
	static Handler fileHandler  = null;
	static Formatter simpleFormatter = null;
	
	public static void setlog(){
		try {
			fileHandler  = new FileHandler("./Connector.log", 5242880, 2);
			// Creating SimpleFormatter
			simpleFormatter = new SimpleFormatter();
			
			//Setting levels to handlers and LOGGER
			fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);
            
 			// Assigning handler to logger
			LOGGER.addHandler(fileHandler);
			
			// Setting formatter to the handler
			fileHandler.setFormatter(simpleFormatter);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}
	public static void loginfo(String msg){
		LOGGER.log(Level.INFO, msg);
	}
	public static void logsevere(String msg){
		LOGGER.log(Level.SEVERE, msg);
	}
	public static void logconfig(String msg){
		LOGGER.log(Level.CONFIG, msg);
	}
}
