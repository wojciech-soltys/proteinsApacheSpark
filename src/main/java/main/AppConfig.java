package main;

import java.io.Serializable;

import utils.ConfigFileParser;
import utils.AppsToUse;

public class AppConfig implements Serializable{

	private static final long serialVersionUID = -5983204351808005030L;
	
	public String msgfPath;
	public String pepnovoPath;
	public String inputFilesPath;
	public int numberOfPartitions;
	public AppsToUse appToUse;
	public String outputFileName;
	public boolean saveToDatabase = false;
	public int jobId;
	public long startJobMillis;
	
	public String databaseHostName = "";
	public String databaseUser = "";
	public String databasePassword = "";
	public String databaseName = "proteins";
	
	public AppConfig(String[] args) {
		this.startJobMillis = System.currentTimeMillis();
		checkNumberOfParameters(args);
		final String configFilePath = args[0];
		ConfigFileParser.parse(configFilePath, this);
	}

	public void setAppToUse(String paramProgram) {
		if(!paramProgram.equals("PEPNOVO") && !paramProgram.equals("MSGF+")) {
			System.out.println("Bad program to use");
			System.exit(1);
		} else if (paramProgram.equals("PEPNOVO")) {
			appToUse = AppsToUse.PEPNOVO;
			outputFileName = "PEPNOVO_output.txt";
		} else {
			appToUse = AppsToUse.MSGFPLUS;
			outputFileName = "MSGF+_output.tsv";
		}
	}
	
	public void setDatabaseHostName(String databaseHostName) {
		if(saveToDatabase && databaseHostName.length() == 0) {
			System.out.println("Database host name is not included in config file");
			System.exit(1);
		}
		this.databaseHostName = databaseHostName;
	}
	
	public void setDatabaseUser(String databaseUser) {
		if(saveToDatabase && databaseUser.length() == 0) {
			System.out.println("Database user is not included in config file");
			System.exit(1);
		}
		this.databaseUser = databaseUser;
	}
	
	public void setDatabasePassword(String databasePassword) {
		if(saveToDatabase && databasePassword.length() == 0) {
			System.out.println("Database password is not included in config file");
			System.exit(1);
		}
		this.databasePassword = databasePassword;
	}
	
	public String getBashScriptPath() {
		switch (appToUse) {
			case PEPNOVO :
				return pepnovoPath;
			case MSGFPLUS :
				return msgfPath;
		}
		return "";
	}
	
	public String getProgramName() {
		switch (appToUse) {
			case PEPNOVO :
				return "PEPNOVO";
			case MSGFPLUS :
				return "MSGF+";
		}
		return "";
	}
	
	private void checkNumberOfParameters(String[] args) {
		if(args.length < 1) {
			System.out.println("Too few arguments");
			System.exit(1);
		} else if (args.length > 1) {
			System.out.println("Too many arguments");
			System.exit(1);
		}
	}
	
}
