package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;

import main.AppConfig;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import com.mysql.jdbc.Statement;

public class DatabaseTool {
	
	
	public Connection getConnection () throws SQLException, ClassNotFoundException{
		Class.forName("com.mysql.jdbc.Driver");
		return DriverManager.getConnection("jdbc:mysql://"
				+ "proteins-database.cgr4a9metqrx.us-west-2.rds.amazonaws.com" + "/"
				+ "proteins", "wojtala6", "wojtala2");
	}
	
	public void saveJobStart(AppConfig appConfig) {
		
		if(appConfig.saveToDatabase) {
			Connection connect = null;
			PreparedStatement preparedStatement = null;
			
			try {
				connect = getConnection();
	
				preparedStatement = connect
						.prepareStatement("INSERT INTO proteins.Jobs(JobsId,ProgramName,StartTime) values (default, ?, ?)",
								Statement.RETURN_GENERATED_KEYS);
	
				preparedStatement.setString(1, appConfig.getProgramName());
				preparedStatement.setTimestamp(2, new Timestamp(appConfig.startJobMillis));
				preparedStatement.executeUpdate();
				
				try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
		            if (generatedKeys.next()) {
		                System.out.println("!!!!!!!!!!! JOB ID: " + generatedKeys.getInt(1) + " REMEMBER THIS FOR GETTING RESULTS !!!!!!!!!!!");
		                appConfig.jobId = generatedKeys.getInt(1);
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
	}
	
	public void setComputationsEndTime(AppConfig appConfig) {
		long computationsEndMillis = System.currentTimeMillis();
		System.out.println("Computations took: " + (computationsEndMillis - appConfig.startJobMillis)/1000 + "s");
		
		if(appConfig.saveToDatabase) {
			Connection connect = null;
			PreparedStatement preparedStatement = null;
			
			try {
				connect = getConnection();
	
				preparedStatement = connect
						.prepareStatement("UPDATE proteins.Jobs SET ComputationsEndTime = ? WHERE JobsId = ?");
	
				preparedStatement.setTimestamp(1, new Timestamp(computationsEndMillis));
				preparedStatement.setInt(2, appConfig.jobId);
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
	
	public void setJobEndTime(AppConfig appConfig) {
		long jobEndMillis = System.currentTimeMillis();
		System.out.println("Computations with saving to database took: " + (jobEndMillis - appConfig.startJobMillis)/1000 + "s");
		if(appConfig.saveToDatabase) {
			Connection connect = null;
			PreparedStatement preparedStatement = null;
			
			try {
				connect = getConnection();
	
				preparedStatement = connect
						.prepareStatement("UPDATE proteins.Jobs SET EndTime = ? WHERE JobsId = ?");
	
				preparedStatement.setTimestamp(1, new Timestamp(jobEndMillis));
				preparedStatement.setInt(2, appConfig.jobId);
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
	
	public VoidFunction<Iterator<String>> getDatabaseSaveFunction(AppConfig appConfig) {
		switch (appConfig.appToUse) {
			case PEPNOVO :
				return new PepnovoDatabaseSaveFunction(appConfig.jobId);
			case MSGFPLUS :
				return new MSGFPlusDatabaseSaveFunction(appConfig.jobId);
			default :
				return null;
		}
	}
	
	public void saveOutputToDatabase(AppConfig appConfig, JavaRDD<String> output) {
		if(appConfig.saveToDatabase) {
			VoidFunction<Iterator<String>> databaseSaveFunction = getDatabaseSaveFunction(appConfig);
			if (databaseSaveFunction != null) {
				output.foreachPartition(databaseSaveFunction);
			}
		}
	}
}
