package main;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import database.PepnovoDatabaseSaveFunction;

public class SimpleApp {
		
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		AppConfig appConfig = new AppConfig(args);
		
		SparkConf conf = new SparkConf().setAppName("Simple Application")
				.set("spark.rdd.compress", "true")
				.set("spark.storage.memoryFraction", "1")
				.set("spark.core.connection.ack.wait.timeout", "600")
				//.set("spark.core.connection.auth.wait.timeout","3600")
				.set("spark.akka.frameSize", "50")
				//.set("spark.driver.maxResultSize", "250m")
				//.set("spark.driver.extraLibraryPath", appConfig.mySQLConnectorPath)
				;
		//JavaSparkContext.jarOfClass(SimpleApp.class)
		conf.setJars(new String[]{appConfig.appPath, appConfig.mySQLConnectorPath});
		JavaSparkContext sc = new JavaSparkContext(conf);
		//sc.addJar(appConfig.mySQLConnectorPath);
		
/*		DatabaseTool dateDatabaseTool = new DatabaseTool();
		dateDatabaseTool.saveJobStart(appConfig);*/
		
		JavaPairRDD<String,String> pepnovoFiles = sc.wholeTextFiles(appConfig.inputFilesPath, appConfig.numberOfPartitions);
		JavaRDD<String> output =  pepnovoFiles.pipe(appConfig.getBashScriptPath());
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
		String actualDate = format1.format(cal.getTime());
		output.saveAsTextFile("/outputFiles/" + actualDate + "/" + appConfig.outputFileName); //coalesce(1,true) repartition(1) 
		
		if(appConfig.saveToDatabase) {
			output.foreachPartition(new PepnovoDatabaseSaveFunction());
		}
		
		sc.close();
	}
}
