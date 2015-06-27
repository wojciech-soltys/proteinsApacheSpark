package database;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;

public class DatabaseSaveFunction implements VoidFunction<Iterator<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7039277486852158360L;

	public void call(Iterator<String> it) {
		Connection connect = null;
		PreparedStatement preparedStatement = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://"
					+ "proteins.cgr4a9metqrx.us-west-2.rds.amazonaws.com" + "/"
					+ "proteins", "wojtala6", "wojtala2");

			preparedStatement = connect
					.prepareStatement("insert into  proteins.test values (default, ?)");

			while (it.hasNext()) {
				String outputElement = it.next();
				preparedStatement.setString(1, "" + outputElement.length());
				preparedStatement.executeUpdate();
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
