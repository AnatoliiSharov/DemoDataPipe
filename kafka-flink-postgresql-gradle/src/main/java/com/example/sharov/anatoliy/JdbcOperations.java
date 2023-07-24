package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.COLOMN_OF_NUMBER;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.SELECT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static com.example.sharov.anatoliy.DataStreamJob.SQL_DRIVER;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcOperations {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcOperations.class);

	public int lookForNuberWord(String word) throws SQLException {
		LOG.debug("JdbcOperations start lookForNuberWord with word = {}", word);
		int number = 0;
		try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
				PreparedStatement ps = connect.prepareStatement(SELECT_SQL_QUERY);) {
			LOG.debug("JdbcOperations get connection lookForNuberWord with word = {}", word);

			ps.setString(1, word);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				LOG.debug("JdbcOperations found result lookForNuberWord with word = {}, count = {}", word,
						resultSet.getInt(COLOMN_OF_NUMBER));
				number = resultSet.getInt(COLOMN_OF_NUMBER);
			} else {
				LOG.debug("JdbcOperations not found result lookForNuberWord with word = {}", word);
			}
		} catch (SQLException e) {
			LOG.error("JdbcOperations has errors lookForNuberWord with word = {}", word);
		}
		return number++;

	}

	public void putResult(Tuple2<String, Integer> value, String sqlQuery) {
		LOG.debug("JdbcOperations start putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
		int count = value.f1;
		String word = value.f0;

		try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
				PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
			LOG.debug("JdbcOperations get connection putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
			statement.setString(1, word);
			statement.setInt(2, count);
			statement.executeUpdate();
		} catch (SQLException e) {
			LOG.error("JdbcOperations has errors putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
		}
	}

	public static JdbcExecutionOptions jdbcExecutionOptions() {
		return JdbcExecutionOptions.builder().withBatchIntervalMs(200) // optional: default = 0, meaning no time-based
																		// execution is done
				.withBatchSize(1000) // optional: default = 5000 values
				.withMaxRetries(5) // optional: default = 3
				.build();
	}

	public static JdbcConnectionOptions jdbcConnectionOptions() {
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(URL).withDriverName(SQL_DRIVER)
				.withUsername(USERNAME).withPassword(PASSWORD).build();
	}

}
