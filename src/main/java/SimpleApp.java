import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "d:\\winutil\\");
		
		if(args.length < 2) {
			System.out.println("Too few arguments");
			System.exit(1);
		} else if (args.length > 2) {
			System.out.println("Too many arguments");
			System.exit(1);
		}
		final String programToUse = args[0];
		String bashScriptLocation = "";
		if(!programToUse.equals("pepnovo3") && !programToUse.equals("msgf")) {
			System.out.println("Bad program to use");
			System.exit(1);
		} else if (programToUse.equals("pepnovo3")) {
			bashScriptLocation = "/home/ec2-user/read.sh";
		} else {
			bashScriptLocation = "/home/ec2-user/msgf/msgf.sh";
		}
		
		final String pathToInputFiles = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*String pepnovoFile = "/home/ec2-user/test/smallinputfile.mgf";
		FileDivider.divideFile(pepnovoFile, "256KB", false);*/
		JavaPairRDD<String,String> pepnovoFiles = sc.wholeTextFiles(pathToInputFiles, 4).cache();
		
		long startTime = System.currentTimeMillis();
		pepnovoFiles.pipe(bashScriptLocation).collect();
		System.out.println("Computations takes " + (System.currentTimeMillis()- startTime) + "ms");
		pepnovoFiles.saveAsTextFile(pathToInputFiles + "testoutput.tsv");
		
		//pepnovoFiles.saveAsTextFile(pathToInputFiles + "testoutput1.txt"); //coalesce(1,true) repartition(1)
		
		sc.close();

	}
}
