package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import utils.AppsToUse;
import main.AppConfig;

import com.mysql.jdbc.Statement;

public class DatabaseTool {
	
	
	public Connection getConnection () throws SQLException, ClassNotFoundException{
		Class.forName("com.mysql.jdbc.Driver");
		return DriverManager.getConnection("jdbc:mysql://"
				+ "proteins-database.cgr4a9metqrx.us-west-2.rds.amazonaws.com" + "/"
				+ "proteins", "wojtala6", "wojtala2");
	}
	
	public void saveJobStart(AppConfig appConfig) {
		
		Connection connect = null;
		PreparedStatement preparedStatement = null;
		
		try {
			//Class.forName("com.mysql.jdbc.Driver");
			connect = getConnection();

			preparedStatement = connect
					.prepareStatement("INSERT INTO proteins.jobs(job_id,program_name,start_time) values (default, ?, ?)",
							Statement.RETURN_GENERATED_KEYS);

			preparedStatement.setString(1, appConfig.getProgramName());
			preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			preparedStatement.executeUpdate();
			
			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                System.out.println(generatedKeys.getLong(1));
	                appConfig.jobId = generatedKeys.getLong(1);
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
	}
	
	public VoidFunction<Iterator<String>> getDatabaseSaveFunction(AppsToUse appToUse) {
		switch (appToUse) {
			case PEPNOVO :
				return new PepnovoDatabaseSaveFunction();
			case MSGFPLUS :
				return new MSGFPlusDatabaseSaveFunction();
			default :
				return null;
		}
	}
	
	public void saveOutputToDatabase(AppConfig appConfig, JavaRDD<String> output) {
		if(appConfig.saveToDatabase) {
			VoidFunction<Iterator<String>> databaseSaveFunction = getDatabaseSaveFunction(appConfig.appToUse);
			if (databaseSaveFunction != null) {
				output.foreachPartition(databaseSaveFunction);
			}
		}
	}
}
