
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;


public class FileDivider {
	
	/**Maksymalny rozmiar pliku (max block size in HDFS) **/
	public static final long MAX_FILE_SIZE = 64 * 1024 * 1024;
	
	public static int divideFile(String inputFilePath, String maxSizeString, boolean addHeader) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			long maxSize = calcMaxSize(maxSizeString);
			if(maxSize > MAX_FILE_SIZE) {
				maxSize = MAX_FILE_SIZE;
			}

			int fileCount = 0;
			File file = createNewFile(inputFilePath, fileCount);
			
			br = new BufferedReader(new FileReader(inputFilePath));
			String header = getHeader(br);
			bw = createNewBufferedWriter(file, addHeader, header);
			
			String line = br.readLine();
			String ions = "\r\n";
			do {
				while (line != null && !line.equals("BEGIN IONS")) {
					line = br.readLine();
				}
				while (line != null && !line.contains("END IONS")) {
					ions += line + "\r\n";
					line = br.readLine();
				}
				ions += line + "\r\n" + "\r\n";
				line = br.readLine();
				
				if((file.length() + ions.length()) > maxSize) {
					fileCount++;
					file = createNewFile(inputFilePath, fileCount);
					bw.close();
					bw = createNewBufferedWriter(file, addHeader, header);
					ions = "\r\n" + ions;
				} 
				
				bw.write(ions);
				ions = "";	
				
			} while (line != null);
				
			return fileCount;
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeReader(br);
			closeWriter(bw);
		}
		return 0;
	}
	
	private static String getHeader(BufferedReader br) throws IOException {
		String header = "";
		String line = br.readLine();
		while (line != null && line.contains("#")) {
			header += line + "\r\n";
			line = br.readLine();
		}
		return header;
	}
	
	private static long calcMaxSize(String maxSize) {
		String unit = maxSize.substring(maxSize.length() - 2);
		String sizeString = maxSize.substring(0, maxSize.length() - 2);
		try {
			Integer size = Integer.parseInt(sizeString);
			if (unit.equals("KB")) {
				return size * 1024;
			} else if (unit.equals("MB")) {
				return size * 1024 * 1024;
			}		
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return MAX_FILE_SIZE;
	}
	
	private static void closeReader (Reader reader) {
	  if ( reader != null ) {
	    try {
	    	reader.close();
	    }
	    catch ( IOException ioe ) {
	      ioe.printStackTrace();
	    }
	  }
	}
	
	private static void closeWriter (Writer writer) {
		  if ( writer != null ) {
		    try {
		    	writer.close();
		    }
		    catch ( IOException ioe ) {
		      ioe.printStackTrace();
		    }
		  }
		}
	
	private static File createNewFile(String inputFilePath, int fileCount) throws IOException {
		
		String dirPath = inputFilePath.substring(0, inputFilePath.lastIndexOf('.')) + "/";
		String filePath = dirPath + inputFilePath.substring(inputFilePath.lastIndexOf('/'), inputFilePath.lastIndexOf('.'))
				+ "_" + fileCount + ".mgf";
		File file = new File(dirPath);
		file.mkdirs();
		file = new File(filePath);
		file.createNewFile();
		return file;
	}
	
	private static BufferedWriter createNewBufferedWriter(File file, boolean addHeader, String header) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));
		if(addHeader) {
			bw.append(header);
		}
		return bw;
	}
	
}
