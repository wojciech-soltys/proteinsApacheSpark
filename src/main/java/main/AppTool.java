package main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AppTool {
	
	public AppTool() {
		
	}
	
	public SparkConf getSparkConf(AppConfig appConfig) {
		SparkConf sparkConf = new SparkConf().setAppName("Proteins Apache Spark")
				.set("spark.rdd.compress", "true")
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.core.connection.ack.wait.timeout", "6000")
				//.set("spark.core.connection.auth.wait.timeout","3600")
				.set("spark.akka.frameSize", "50")
				//.set("spark.driver.extraLibraryPath", appConfig.mySQLConnectorPath)
				;
/*		ArrayList<String> jars = new ArrayList<String>(Arrays.asList(JavaSparkContext.jarOfClass(SimpleApp.class)));
		//jars.add(appConfig.mySQLConnectorPath);
		String[] jarsArray = new String[jars.size()];
		jars.toArray( jarsArray );
		sparkConf.setJars(jarsArray);*/
		return sparkConf;
	}
	
	public JavaRDD<String> compute(AppConfig appConfig, JavaSparkContext sparkContext) {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String jobStartDate = dateFormat.format(new Date(appConfig.startJobMillis));
		
		JavaPairRDD<String,String> inputFiles = sparkContext.wholeTextFiles(appConfig.inputFilesPath, appConfig.numberOfPartitions);
		JavaRDD<String> output =  inputFiles.pipe(appConfig.getBashScriptPath());
		output.saveAsTextFile("/outputFiles/" + jobStartDate + "/" + appConfig.jobId + "/" + appConfig.outputFileName); //coalesce(1,true) repartition(1)
		return output;
	}
}
