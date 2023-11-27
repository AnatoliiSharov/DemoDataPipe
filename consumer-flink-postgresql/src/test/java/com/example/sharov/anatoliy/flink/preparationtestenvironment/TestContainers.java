package com.example.sharov.anatoliy.flink.preparationtestenvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;

public class TestContainers {
	public static final String INIT_DATABASE = "\"CREATE TABLE IF NOT EXISTS stories(id CHARACTER VARYING(100) NOT NULL UNIQUE, title CHARACTER VARYING(189819) NOT NULL, url CHARACTER VARYING(189819) NOT NULL, site CHARACTER VARYING(189819) NOT NULL, time TIMESTAMP NOT NULL, favicon_url CHARACTER VARYING(189819) NOT NULL, description CHARACTER VARYING(189819))"; 
	
	private PostgreSQLContainer<?> postgresContainer;
		
	public void createTestPostgresContainer() throws SQLException {
		postgresContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.3"));
		postgresContainer.withDatabaseName("stories").withInitScript("database/init_database.sql");
		postgresContainer.start();
		Awaitility.await().atMost(Duration.ofSeconds(2)).until(postgresContainer::isRunning);
	}
	
	public void initTestScript(String sqlscript) {
		postgresContainer.withInitScript(sqlscript);
	}
	
	public void stopTestPostgresContainer() {
		postgresContainer.stop();;
	}
	
	public Properties getProperties() {
		Properties testProp = new Properties(); 

		testProp.setProperty("url", postgresContainer.getJdbcUrl());
		testProp.setProperty("username", postgresContainer.getUsername());
		testProp.setProperty("password", postgresContainer.getPassword());
		return testProp;
	}
	
}
