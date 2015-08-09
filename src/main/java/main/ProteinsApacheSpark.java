package main;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import database.DatabaseTool;

public class ProteinsApacheSpark {
		
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		AppConfig appConfig = new AppConfig(args);
		DatabaseTool databaseTool = new DatabaseTool();
		AppTool appTool = new AppTool();
		
		//creating JavaSparkContext
		SparkConf conf = appTool.getSparkConf(appConfig);
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		//setting job start time
		databaseTool.saveJobStart(appConfig);
		
		//compute
		JavaRDD<String> output = appTool.compute(appConfig, sparkContext);
		//save computations end time
		databaseTool.setComputationsEndTime(appConfig);
		//saving to database
		databaseTool.saveOutputToDatabase(appConfig, output);
		//setting job end time
		databaseTool.setJobEndTime(appConfig);
		//closing Spark context 
		sparkContext.close();
	}
}
