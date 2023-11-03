package com.example.sharov.anatoliy.flink;

import static com.example.sharov.anatoliy.flink.DataStreamJob.INSERT_STORIES;
import static com.example.sharov.anatoliy.flink.DataStreamJob.SELECT_ID_FROM_STORIES;
import static com.example.sharov.anatoliy.flink.DataStreamJob.SQL_DRIVER;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;


import org.postgresql.Driver;

class DataStreamJobTestSourceFromElementPostgresContainer {
	@SuppressWarnings("rawtypes")
	static transient PostgreSQLContainer postgresContainer;
/*
	@SuppressWarnings("rawtypes")
	@BeforeEach
	public void prepareTestContainers() throws SQLException, InterruptedException, ExecutionException {
		postgresContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:15.3"));
		Startables.deepStart(Stream.of(postgresContainer)).join();

		Thread.sleep(3000);

		Properties postgresProps = new Properties();
		postgresProps.put(URL, postgresContainer.getJdbcUrl());
		postgresProps.put(USERNAME, postgresContainer.getUsername());
		postgresProps.put(PASSWORD, postgresContainer.getPassword());
		postgresProps.put("counted_words", postgresContainer.getDatabaseName());
		postgresProps.put("driver", Driver.class.getName());

		try (Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),
				postgresContainer.getUsername(), postgresContainer.getPassword());
				Statement st = connection.createStatement();) {
			st.execute(
					"CREATE TABLE counted_words(word_id SERIAL PRIMARY KEY  NOT NULL, word CHARACTER VARYING(189819) UNIQUE NOT NULL, number INTEGER NOT NULL);");
			st.execute("INSERT INTO counted_words (word, number) VALUES ('word2', 200);");
		}
	}

	@Test
	void test() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().disableClosureCleaner();

		DataStream<String> dataStream = env.fromElements("word1", "word2", "word3", "word4", "word1");

		DataStream<CountedWordPojo> dataFirstMidStream = dataStream.map((word) -> {
			int number = 0;

			try (Connection connect = DriverManager.getConnection(postgresContainer.getJdbcUrl(),
					postgresContainer.getUsername(), postgresContainer.getPassword());
					PreparedStatement ps = connect.prepareStatement(SELECT_NEWS_HASH_CODE);) {

				ps.setString(1, word);
				ResultSet resultSet = ps.executeQuery();

				if (resultSet.next()) {
					number = resultSet.getInt(COLOMN_OF_NUMBER);
				} else {
					number = 0;
				}
			} catch (SQLException e) {
			}
			return new CountedWordPojo(word, number + 1);
		});

		dataFirstMidStream.filter(countedWord -> new NewWordsFilter().filter(countedWord))
				.addSink(JdbcSink.sink(INSERT_NEWS,

						(statement, countedWord) -> {
							statement.setString(1, countedWord.getWord());
							statement.setInt(2, countedWord.getNumber());
						}, jdbcExecutionOptions(), jdbcConnectionOptions()));

		dataFirstMidStream.filter(countedWord -> !new NewWordsFilter().filter(countedWord))
				.addSink(JdbcSink.sink(UPDATE_SQL_QUERY, (statement, countedWord) -> {
					statement.setString(2, countedWord.getWord());
					statement.setInt(1, countedWord.getNumber());
				}, jdbcExecutionOptions(), jdbcConnectionOptions()));

		env.execute("Flink Java API Skeleton");
		
		Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),
				postgresContainer.getUsername(), postgresContainer.getPassword());
		PreparedStatement statement = connection.prepareStatement("SELECT word, number FROM counted_words;");
		ResultSet resultSet = statement.executeQuery();
		
		List<CountedWordPojo> actual = new ArrayList<>();
		List<CountedWordPojo> expected = Arrays.asList(new CountedWordPojo("word1", 2), new CountedWordPojo("word2", 201), new CountedWordPojo("word3", 1), new CountedWordPojo("word4", 1));
	
		while(resultSet.next()) {
			actual.add(new CountedWordPojo(resultSet.getString("word"), resultSet.getInt("number")));
		}
		Collections.sort(expected, compare(expected));
		Collections.sort(actual, compare(actual));
		assertEquals(expected, actual);
	}

	private Comparator compare(List<CountedWordPojo> list) {
		return new Comparator<CountedWordPojo>() {

			@Override
			public int compare(CountedWordPojo o1, CountedWordPojo o2) {
				return o1.getWord().compareTo(o2.getWord());
			}
		};
	}

	public static JdbcExecutionOptions jdbcExecutionOptions() {
		return JdbcExecutionOptions.builder().withBatchIntervalMs(200) // optional: default = 0, meaning no time-based
				.withBatchIntervalMs(200) // optional: default = 0, meaning no time-based execution is done
				.withBatchSize(1000) // optional: default = 5000 values
				.withMaxRetries(5) // optional: default = 3
				.build();
	}

	public static JdbcConnectionOptions jdbcConnectionOptions() {
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(postgresContainer.getJdbcUrl())
				.withDriverName(SQL_DRIVER)
				.withUsername(postgresContainer.getUsername())
				.withPassword(postgresContainer.getPassword())
				.build();
	}
*/

}
