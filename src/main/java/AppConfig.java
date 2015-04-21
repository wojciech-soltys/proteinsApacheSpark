
public class AppConfig {
	public ProgramsToUse programToUse;
	public String bashScriptLocation;
	public String outputFileName;
	public String pathToInputFiles;
	
	public AppConfig(String[] args) {
		final String paramProgram = args[0];
		if(!paramProgram.equals("pepnovo3") && !paramProgram.equals("msgf")) {
			System.out.println("Bad program to use");
			System.exit(1);
		} else if (paramProgram.equals("pepnovo3")) {
			programToUse = ProgramsToUse.PEPNOVO3;
			bashScriptLocation = "/home/ec2-user/read.sh";
			outputFileName = "pepnovo3_output.txt";
		} else {
			programToUse = ProgramsToUse.MSGFPLUS;
			bashScriptLocation = "/home/ec2-user/msgf/msgf.sh";
			outputFileName = "msgfPlus_output.tsv";
		}
		
		pathToInputFiles = args[1];
	}
	
}
