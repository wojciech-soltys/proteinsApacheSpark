package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import main.AppConfig;

public class ConfigFileParser {

	public static void parse(String configFilePath, AppConfig appConfig) {
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(configFilePath));
			String line = br.readLine();
			do {
				if (line.startsWith("#")) {
					line = br.readLine();
					continue;
				} else {
					int index = line.indexOf("=");
					if (index > 0) {
						System.out.println(line.substring(0, index) + "="
								+ line.substring(index + 1, line.length()));
						String paramName = line.substring(0, index);
						String paramValue = line.substring(index + 1, line.length());
						setAppParam(paramName, paramValue, appConfig);
					}
					line = br.readLine();
				}
			} while (line != null);

		} catch (FileNotFoundException e) {
			System.out.println("Bad path to config file!");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("IOException during reading config file!");
			e.printStackTrace();
		} finally {
			closeReader(br);
		}

	}

	private static void closeReader(Reader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}
	
	private static void setAppParam(String paramName, String paramValue, AppConfig appConfig) {
		switch(paramName) {
			case "AppPath" :
				appConfig.appPath = paramValue;
				break;
			case "MySQLConnectorPath" :
				appConfig.mySQLConnectorPath = paramValue;
				break;
			case "MsgfPath" :
				appConfig.msgfPath = paramValue;
				break;
			case "PepnovoPath" :
				appConfig.pepnovoPath = paramValue;
				break;
			case "InputFilesPath" :
				appConfig.inputFilesPath = paramValue;
				break;
			case "NumberOfPartitions" :
				try {
					appConfig.numberOfPartitions = Integer.parseInt(paramValue);
				} catch (NumberFormatException e) {
					System.out.println("Bad value of NumberOfPartitions config parameter");
				}
				break;
			case "AppToUse" :
				appConfig.setAppToUse(paramValue);
				break;
			case "SaveToDatabase" :
				appConfig.saveToDatabase = paramValue.equals("TRUE");
				break;
		}
	}
}
