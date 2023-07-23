package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.UPDATE_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.INSERT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static com.example.sharov.anatoliy.DataStreamJob.SQL_DRIVER;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class DataStreamJobTest {
	/*
	@SuppressWarnings("rawtypes")
	private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15.3-alpine");

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(2).setNumberTaskManagers(1)
					.build());

	@BeforeEach
	private void startContainer() {
		postgres.start();

		String sql = "CREATE DATABASE counted_words;" + "CREATE TABLE counted_words("
				+ "word_id SERIAL PRIMARY KEY  NOT NULL, " + "word CHARACTER VARYING(189819) UNIQUE NOT NULL, "
				+ "number INTEGER NOT NULL" + ");" + "INSERT INTO counted_words (word, number) VALUES ('word2', '2');";
	}

	@Test
	void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setParallelism(1);
		DataStream<String> source = env.fromElements("word1","word2", "word3", "word1");
		
		DataStream<Tuple2<String, Integer>> dataFirstMidStream = source.map(new LengthWordsDefinition());
		DataStream<Tuple2<String, Integer>> newWordStream = dataFirstMidStream.filter(new NewWordsFilter());

		newWordStream.addSink(JdbcSink.sink(
				UPDATE_SQL_QUERY,
				(statement, tuple) -> {
                	String word = tuple.f0;
                	int number = tuple.f1;
                	
                    statement.setString(1, word);
                    statement.setInt(2, number);
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions()
        ));

		dataFirstMidStream.filter(new OldWordsFilter()).addSink(
                JdbcSink.sink(
                		INSERT_SQL_QUERY,
                        (statement, tuple) -> {
                        	String word = tuple.f0;
                        	int number = tuple.f1;
                        	
                            statement.setString(1, word);
                            statement.setInt(2, number);
                        },
                        jdbcExecutionOptions(),
                        jdbcConnectionOptions()
                ));

		env.execute();
		
		
		
		List<ResultPojo> actual = new ArrayList();
		
		Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		PreparedStatement statement = connection.prepareStatement("SELECT word, number FROM counted_words;");
		ResultSet resultSet = statement.executeQuery();
		
		while(resultSet.next()) {
			actual.add(new ResultPojo(resultSet.getString("word"), resultSet.getInt("number")));
		}
		
		List<ResultPojo> expected = Arrays.asList(new ResultPojo("word1", 1));
		
		assertEquals(expected, actual);
		
		
	}

	private static JdbcExecutionOptions jdbcExecutionOptions() {
		return JdbcExecutionOptions.builder()
		        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
		        .withBatchSize(1000)                  // optional: default = 5000 values
		        .withMaxRetries(5)                    // optional: default = 3 
		.build();
	}

	private static JdbcConnectionOptions jdbcConnectionOptions() {
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(URL)
                .withDriverName(SQL_DRIVER)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .build();
	}
	*/
}
