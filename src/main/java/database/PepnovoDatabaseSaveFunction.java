package database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;

import java.math.BigDecimal;

import com.mysql.jdbc.Statement;

public class PepnovoDatabaseSaveFunction implements VoidFunction<Iterator<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7039277486852158360L;

	public void call(Iterator<String> it) {
		
		int scanId = 0;
		while (it.hasNext()) {
			String line = it.next();
			if(line.startsWith(">>")) {
				if(it.hasNext()) {
					String secondLine = it.next();
					if(secondLine.startsWith("#Index")) {
						scanId = addScan(line, null);
					} else if (secondLine.startsWith("#")) {
						scanId = addScan(line, secondLine);
					}
					
					while(it.hasNext()) {
						String scanDetails = it.next();
						if(scanDetails.length() == 0) {
							break;
						} else {
							addScanDetails(scanDetails, scanId);
						}
					}
				}
			}
		}
	}
	
	private int addScan(String headerLine, String additionalInfo) {
		Connection connect = null;
		PreparedStatement preparedStatement = null;
		int scanId = 0;
		
		try {
			DatabaseTool databaseTool = new DatabaseTool();
			connect = databaseTool.getConnection();
	
			preparedStatement = connect
					.prepareStatement("insert into  proteins.ScansPepnovo(ScansPepnovoId, JobId, ScanNumber, RT, Raw, SQS, AdditionalInfo) "
							+ "values (default, ?, ?, ?, ?, ?, ?)",
							Statement.RETURN_GENERATED_KEYS);
			/* TODO - dodac job ID */
			preparedStatement.setInt(1, 0);
			preparedStatement.setInt(2, getScanNumber(headerLine));
			preparedStatement.setBigDecimal(3, getRT(headerLine));
			preparedStatement.setString(4, getRaw(headerLine));
			preparedStatement.setBigDecimal(5, getSQS(headerLine));
			if(additionalInfo != null) {
				preparedStatement.setString(6, additionalInfo.substring(1));
			} else {
				preparedStatement.setString(6, null);
			}
			preparedStatement.executeUpdate();
			
			
			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	scanId = generatedKeys.getInt(1);
	            }
	        }
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connect.close();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return scanId;
	}
	
	private int getScanNumber(String headerLine) {
		return Integer.parseInt(headerLine.substring(headerLine.indexOf("Scan") + 5, headerLine.indexOf("(rt") - 1));
	}
	
	private BigDecimal getRT(String headerLine) {
		return new BigDecimal(headerLine.substring(headerLine.indexOf("(rt=") + 4, headerLine.indexOf("[") - 2));
	}
	
	private String getRaw(String headerLine) {
		return headerLine.substring(headerLine.indexOf("[") + 1, headerLine.indexOf("]"));
	}
	
	private BigDecimal getSQS(String headerLine) {
		BigDecimal SQS = null;
		if(headerLine.indexOf("SQS") > -1 ) {
			SQS = new BigDecimal(headerLine.substring(headerLine.indexOf("SQS") + 4, headerLine.lastIndexOf(")")));
		} 
		return SQS;
	}
	
	private void addScanDetails(String detailsLine, int scanId) {
		
		if(scanId == 0) {
			System.out.println("ScanId is eqaul 0. Return.");
			return;
		}
		String[] detailsFields = detailsLine.split("\t");
		if(detailsFields.length != 8) {
			System.out.println("Wrong number of fields in line.");
		}
		
		Connection connect = null;
		PreparedStatement preparedStatement = null;
		
		try {
			
			DatabaseTool databaseTool = new DatabaseTool();
			connect = databaseTool.getConnection();
			
			//(ScanDetailsPepnovoId, ScanId, Index, RnkScr, PnvScr, NGap, CGap, MH, Charge, Sequence)"
			preparedStatement = connect
					.prepareStatement( "insert into  proteins.ScanDetailsPepnovo "
							+ "values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
							Statement.RETURN_GENERATED_KEYS);

			preparedStatement.setInt(1, scanId);
			preparedStatement.setInt(2, Integer.parseInt(detailsFields[0]));
			preparedStatement.setBigDecimal(3, new BigDecimal(detailsFields[1]));
			preparedStatement.setBigDecimal(4, new BigDecimal(detailsFields[2]));
			preparedStatement.setBigDecimal(5, new BigDecimal(detailsFields[3]));
			preparedStatement.setBigDecimal(6, new BigDecimal(detailsFields[4]));
			preparedStatement.setBigDecimal(7, new BigDecimal(detailsFields[5]));
			preparedStatement.setBigDecimal(8, new BigDecimal(detailsFields[6]));
			preparedStatement.setString(9, detailsFields[7]);
			preparedStatement.executeUpdate();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connect.close();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
