import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
		
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		checkNumberOfParameters(args);
		AppConfig appConfig = new AppConfig(args);
		
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<String,String> pepnovoFiles = sc.wholeTextFiles(appConfig.pathToInputFiles, 4).cache();
		JavaRDD<String> output =  pepnovoFiles.pipe(appConfig.bashScriptLocation);
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
		String actualDate = format1.format(cal.getTime());
		output.saveAsTextFile("/user/root/" + actualDate + "/" + appConfig.outputFileName); //coalesce(1,true) repartition(1) 
		
		sc.close();
	}
	
	private static void checkNumberOfParameters(String[] args) {
		if(args.length < 2) {
			System.out.println("Too few arguments");
			System.exit(1);
		} else if (args.length > 2) {
			System.out.println("Too many arguments");
			System.exit(1);
		}
	}
}
