package utils;
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
		
		System.out.println("FileDivider.divideFile - start");
		System.out.println("FileDivider.divideFile - max file size: " + maxSizeString);
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			long maxSize = calcMaxSize(maxSizeString);
			if(maxSize > MAX_FILE_SIZE) {
				maxSize = MAX_FILE_SIZE;
			}

			int fileCount = 0;
			File file = createNewFile(inputFilePath, fileCount, true);
			
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
					file = createNewFile(inputFilePath, fileCount, false);
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
	
	private static File createNewFile(String inputFilePath, int fileCount, boolean isFirstCreation) throws IOException {
		
		String dirPath = getOutputPath(inputFilePath);
		String filePath = dirPath + inputFilePath.substring(inputFilePath.lastIndexOf('/') + 1, inputFilePath.lastIndexOf('.'))
				+ "_" + fileCount + ".mgf";
		File file = new File(dirPath);
		if(isFirstCreation) {
			deleteFolder(file);
			System.out.println("FileDivider.createNewFile - deleted folder: " + dirPath);
		}
		file.mkdirs();
		file = new File(filePath);
		file.createNewFile();
		System.out.println("FileDivider.createNewFile - created file: " + filePath);
		return file;
	}
	
	private static BufferedWriter createNewBufferedWriter(File file, boolean addHeader, String header) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));
		if(addHeader) {
			bw.append(header);
		}
		return bw;
	}
	
	public static String getOutputPath(String inputFilePath) {
		return inputFilePath.substring(0, inputFilePath.lastIndexOf('.')) + "/";
	}
	
	public static void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) { //some JVMs return null for empty dirs
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	    folder.delete();
	}
	
}
