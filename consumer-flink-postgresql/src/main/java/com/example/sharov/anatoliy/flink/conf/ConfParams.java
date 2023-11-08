package com.example.sharov.anatoliy.flink.conf;

import java.io.Serializable;

public class ConfParams implements Serializable{
	private static final long serialVersionUID = 4965623907148713671L;
	
	public static final String DEFAULT_TOPIC = "my-topic";
	public static final String DEFAULT_KAFKA_GROUP = "mygroup";
	public static final String DEFAULT_BOOTSTAP_SERVERS = "broker:9092";

	public static final String DEFAULT_URL = "jdbc:postgresql://database:5432/stories";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String DEFAULT_DATABASE_USER = "crawler";
	public static final String DEFAULT_DATABASE_PASSWORD = "1111";
	
	
	private String topic;
	private String kafkaGroup;
	private String databaseUrl;
	private String username;
	private String password;
	private String bootstrapServers;
	private String sqlDriver;
	
	public ConfParams() {
		super();
		this.topic = System.getenv("KAFKA_TOPIC") != null 
				? System.getenv("KAFKA_TOPIC") 
						: DEFAULT_TOPIC;
		this.kafkaGroup = System.getenv("KAFKA_FLINK_GROUP") != null 
				? System.getenv("KAFKA_FLINK_GROUP")
						: DEFAULT_KAFKA_GROUP;
		this.databaseUrl = System.getenv("DATABASE_URL") != null 
				? System.getenv("DATABASE_URL")
						: DEFAULT_URL;
		this.username = System.getenv("DATABASE_USER") != null 
				? System.getenv("DATABASE_USER") 
						: DEFAULT_DATABASE_USER;
		this.password = System.getenv("DATABASE_PASSWORD") != null 
				? System.getenv("DATABASE_PASSWORD")
						: DEFAULT_DATABASE_PASSWORD;
		this.bootstrapServers = (System.getenv("KAFKA_BROKER_HOST") != null&&System.getenv("KAFKA_BROKER_PORT") != null)
			? System.getenv("KAFKA_BROKER_HOST") + ":" + System.getenv("KAFKA_BROKER_PORT")
				: DEFAULT_BOOTSTAP_SERVERS;
		this.sqlDriver = SQL_DRIVER;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public String getKafkaGroup() {
		return kafkaGroup;
	}
	
	public String getDatabaseUrl() {
		return databaseUrl;
	}
	
	public String getUsername() {
		return username;
	}
	
	public String getPassword() {
		return password;
	}
	
	public String getBootstrapServers() {
		return bootstrapServers;
	}
	
	public String getSqlDriver() {
		return sqlDriver;
	}
	
}
