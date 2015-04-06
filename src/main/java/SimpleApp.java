import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		if(args.length < 1) {
			System.out.println("Nie przekazano parametru");
			System.exit(1);
		}
		final String pathToInputFiles = args[0]; //"/user/root/pepnovo3/";
		
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//String pepnovoFile = folderPath + "biginputfile.mgf";
		//FileDivider.divideFile(pepnovoFile, "1MB", false);
		JavaPairRDD<String,String> pepnovoFiles = sc.wholeTextFiles(pathToInputFiles, 4).cache();
		
		long startTime = System.currentTimeMillis();
		pepnovoFiles.pipe("/home/ec2-user/read.sh").collect();
		System.out.println("Computations takes " + (System.currentTimeMillis()- startTime) + "ms");
		pepnovoFiles.saveAsTextFile(pathToInputFiles + "testoutput1.txt"); //coalesce(1,true) repartition(1)
		
		sc.close();

	}
}
