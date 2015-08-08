package main;

import utils.ConfigFileParser;
import utils.AppsToUse;

public class AppConfig {
	public String appPath;
	public String mySQLConnectorPath;
	public String msgfPath;
	public String pepnovoPath;
	public String inputFilesPath;
	public int numberOfPartitions;
	public AppsToUse appToUse;
	public String outputFileName;
	public boolean saveToDatabase = false;
	public long jobId;
	
	public AppConfig(String[] args) {
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
