import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		String logFile = "D:/spark-1.3.0-bin-hadoop2.4/README.md";
		String pepnovoFile = "D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/thesmallestinputfile.mgf";
		SparkConf conf = new SparkConf().setAppName("Simple Application")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> logData = sc.textFile(logFile).cache();
		JavaRDD<String> pepnovoData = sc.textFile(pepnovoFile,10).cache();
		
		JavaPairRDD<String,String> pepnovoFiles = sc.wholeTextFiles("D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/files",3).cache();
		

/*		for(String partition : pepnovoData.pipe("D:/readline2.bat").collect()) {
			System.out.println(partition.toString());
		}*/
		
		
/*		for(String partition : pepnovoFiles.pipe("D:/OneDrive/Studia/Magisterka/apps/readlines.bat").collect()) {
			System.out.println(partition.toString());
		}*/
		
		for(String partition : pepnovoFiles.pipe("D:/OneDrive/Studia/Magisterka/apps/readline2.bat").collect()) {
			System.out.println(partition.toString());
		}

	}
}
