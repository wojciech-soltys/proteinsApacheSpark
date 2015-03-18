import org.apache.spark.SparkConf;
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
		
/*		int i = 0;
		for(String partition : pepnovoData.collect()) { //pipe("D:/test.bat")
			System.out.println(i + ":");
			System.out.println(partition.toString());
			System.out.println("============");
			i++;
		}*/
		
		
		for(String partition : pepnovoData.pipe("D:/readline2.bat").collect()) {
			System.out.println(partition.toString());
		}
		
		
/*		for(String partition : pepnovoData.collect()) {
			System.out.println("============");
			System.out.println(partition.toString());
			System.out.println("============");
		}*/
		
		
		
/*		for (int i = 0; i < pepnovoData.pipe("D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/Pepnovo.exe -file smallinputfile.mgf -model CID_IT_TRYP").collect().size();i++) {
			System.out.println(pepnovoData.take(i).toString());
		}*/
		
		
		
/*
	    long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		*/
		

		/*
		 * String[] cmd = {
		 * "D:\\spark-1.3.0-bin-hadoop2.4\\pepnovo3\\Pepnovo.exe -file smallinputfile.mgf -model CID_IT_TRYP > testoutput.txt"
		 * }; Process p; try { p = Runtime.getRuntime().exec(cmd); p.waitFor();
		 * } catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

/*		try {
			Process proc = Runtime
					.getRuntime()
					.exec("D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/Pepnovo.exe -file smallinputfile.mgf -model CID_IT_TRYP",
							null,
							new File("D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/"));
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(
					proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new InputStreamReader(
					proc.getErrorStream()));

			// read the output from the command
			System.out.println("Here is the standard output of the command:\n");
			String s = null;
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			}

			// read any errors from the attempted command
			System.out
					.println("Here is the standard error of the command (if any):\n");
			while ((s = stdError.readLine()) != null) {
				System.out.println(s);
			}
			
			pepnovoData.pipe("D:/spark-1.3.0-bin-hadoop2.4/pepnovo3/Pepnovo.exe");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	*/

	}
}
