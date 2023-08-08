package com.example.sharov.anatoliy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InspectionUtil {
	private static final Logger LOG = LoggerFactory.getLogger(InspectionUtil.class);

	static public final String TABLE_EXISTENCE_CHECK = "SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = ?)";

	public void waitForDatabaceAccessibility(String url, String tableName, int hoverTime) throws InterruptedException {

		while (!checkDataBaseAvailability(url, tableName)) {
			LOG.info("waiting cause by checking of postgres table do not pass");
			Thread.sleep(hoverTime);
		}
	}

	public boolean checkDataBaseAvailability(String jdbcUrl, String tableName) {
		LOG.debug("InspectionUtil.checkDataBaseAvailability get start");
		boolean result = false;

		try (Connection connection = DriverManager.getConnection(jdbcUrl);
				PreparedStatement statement = connection.prepareStatement(TABLE_EXISTENCE_CHECK)) {
			LOG.debug("InspectionUtil.checkDataBaseAvailability do get connection successfully to db with jdbcUrl = {}", jdbcUrl);
			statement.setString(1, tableName);

			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					LOG.debug("InspectionUtil.checkDataBaseAvailability check exitence of table successfully with tableName = {}",
							tableName);
					result = resultSet.getBoolean(1);
				}
			}
		} catch (SQLException e) {
			LOG.debug("check of InspectionUtil.checkDataBaseAvailability do not pass with jdbcUrl = {} tableName = {}", jdbcUrl,
					tableName);
			e.printStackTrace();
		}
		return result;
	}

	public void waitForTopicAvailability(String topicName, String bootstrapServers, int hoverTime) throws InterruptedException {
		
		while (!checkTopicAvailability(topicName, bootstrapServers)) {
			LOG.info("waiting cause by checking of kafka topic do not pass");
			Thread.sleep(hoverTime);
		}
	}

	public boolean checkTopicAvailability(String topicName, String bootstrapServers) throws InterruptedException {
		LOG.debug("InspectionUtil.checkTopicAvailability get start");
		boolean topicAvailable = false;
		Properties adminProps = new Properties();

		adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		try (AdminClient adminClient = AdminClient.create(adminProps)) {
			LOG.debug("InspectionUtil.checkTopicAvailability get AdminClient successfully with bootstrapServers = {}", bootstrapServers);
			DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
			Map<String, KafkaFuture<TopicDescription>> topicDescriptions = topicsResult.topicNameValues();
			KafkaFuture<TopicDescription> topicDescription = topicDescriptions.get(topicName);

			try {
				topicDescription.get();
				LOG.debug("InspectionUtil.checkTopicAvailability get topicDescription.get() successfully with topicName = {}", topicName);
				topicAvailable = true;
			} catch (InterruptedException | ExecutionException e) {
				LOG.debug("check of InspectionUtil.checkTopicAvailability do not pass with topicName = {} bootstrapServers = {}", topicName,
						bootstrapServers);
				e.printStackTrace();
			}
		}
		return topicAvailable;
	}

}
