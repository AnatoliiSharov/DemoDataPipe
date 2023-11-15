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

public class TestContainers {
	public static final String INIT_DATABASE = "\"CREATE TABLE IF NOT EXISTS stories(id CHARACTER VARYING(100) NOT NULL UNIQUE, title CHARACTER VARYING(189819) NOT NULL, url CHARACTER VARYING(189819) NOT NULL, site CHARACTER VARYING(189819) NOT NULL, time TIMESTAMP NOT NULL, favicon_url CHARACTER VARYING(189819) NOT NULL, description CHARACTER VARYING(189819))"; 
	
	private PostgreSQLContainer<?> postgresContainer;
	private Connection connection;
	private ConfParams conf;
	
	public Connection createTestPostgresContainer() throws SQLException {
		postgresContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.3"));
		postgresContainer.withInitScript("database/init_database.sql");
		conf = new ConfParams();
		Startables.deepStart(Stream.of(postgresContainer)).join();
		Awaitility.await().atMost(Duration.ofSeconds(2)).until(postgresContainer::isRunning);
		Properties testProp = new Properties(); 
		
		testProp.setProperty(conf.getDatabaseUrl(), postgresContainer.getJdbcUrl());
		testProp.setProperty(conf.getUsername(), postgresContainer.getUsername());
		testProp.setProperty(conf.getPassword(), postgresContainer.getPassword());
		connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(), postgresContainer.getPassword());
		return connection;
	}
	
}
