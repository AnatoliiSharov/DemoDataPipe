package com.example.sharov.anatoliy.flink.conf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnector{
	private Connection connection;
	private ConfParams conf;
//TODO APPLY THIS CLASS IN PROJECT AFTER TESTS
	public DatabaseConnector() throws SQLException {
		this.conf = new ConfParams();
		this.connection = DriverManager.getConnection(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword());
	}

	public Connection getConnection() {
		return connection;
	}

	public void closeConnection() throws SQLException {

		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}
	
}
