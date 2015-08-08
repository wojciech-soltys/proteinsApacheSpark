package database;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;

public class MSGFPlusDatabaseSaveFunction implements VoidFunction<Iterator<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7039277486852158360L;

	public void call(Iterator<String> it) {
		
		while (it.hasNext()) {		
			String line = it.next();
			if(line == null || line.startsWith("#")) {
				continue;
			}
			addScan(line);
		}
	}
	
	private void addScan(String line) {
		Connection connect = null;
		PreparedStatement preparedStatement = null;
		
		try {
			
			String[] detailsFields = line.split("\t");
			if(detailsFields.length != 15) {
				System.out.println("Wrong number of fields in line.");
				return;
			}
			
			DatabaseTool databaseTool = new DatabaseTool();
			connect = databaseTool.getConnection();
	
			//ScansMSGFPlusId, JobId, SpecID, ScanNum, Title, FragMethod, Precursor, IsotopeError, PrecursorError, Charge, Peptide, Protein, DeNovoScore, MSGFScore, SpecEValue, EValue
			preparedStatement = connect
					.prepareStatement("insert into  proteins.ScansMSGFPlus "
							+ "values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			
			
			/* TODO - dodac job ID */
			preparedStatement.setInt(1, 0);
			preparedStatement.setInt(2, new Integer(detailsFields[1].substring(detailsFields[1].indexOf("=") + 1)));
			preparedStatement.setInt(3, new Integer(detailsFields[2]));
			preparedStatement.setString(4, detailsFields[3]);
			preparedStatement.setString(5, detailsFields[4]);
			preparedStatement.setBigDecimal(6, new BigDecimal(detailsFields[5]));
			preparedStatement.setInt(7, new Integer(detailsFields[6]));
			preparedStatement.setBigDecimal(8, new BigDecimal(detailsFields[7]));
			preparedStatement.setInt(9, new Integer(detailsFields[8]));
			preparedStatement.setString(10, detailsFields[9]);
			preparedStatement.setString(11, detailsFields[10]);
			preparedStatement.setInt(12, new Integer(detailsFields[11]));
			preparedStatement.setInt(13, new Integer(detailsFields[12]));
			preparedStatement.setBigDecimal(14, new BigDecimal(detailsFields[13]));
			preparedStatement.setBigDecimal(15, new BigDecimal(detailsFields[14]));
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
