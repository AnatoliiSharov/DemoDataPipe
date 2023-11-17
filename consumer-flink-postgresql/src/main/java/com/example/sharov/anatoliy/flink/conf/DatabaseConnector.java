package com.example.sharov.anatoliy.flink.conf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnector{
	private Connection connection;
	private ConfParams conf;
	
//TODO APPLY THIS CLASS IN PROJECT AFTER TESTS
	public DatabaseConnector(){
		this.conf = new ConfParams();
	}

	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword());
	}
	
	public Connection getConnection(Properties prop) throws SQLException {
		return DriverManager.getConnection(prop.getProperty("url"), prop.getProperty("username"), prop.getProperty("password"));
	}

	public void closeConnection() throws SQLException {

		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}
	
}
