package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static com.example.sharov.anatoliy.DataStreamJob.UPDATE_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.INSERT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.SQL_DRIVER;
import static com.example.sharov.anatoliy.DataStreamJob.COLOMN_OF_NUMBER;
import static com.example.sharov.anatoliy.DataStreamJob.COLOMN_OF_WORD;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

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
    static PostgreSQLContainer postgresContainer;
    
    @BeforeEach
    public void prepareTestContainers() throws SQLException, InterruptedException, ExecutionException {
        postgresContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:15.3"));
        Startables.deepStart(Stream.of(postgresContainer)).join();

    	Thread.sleep(3000);
    	
    	Properties postgresProps = new Properties();
        postgresProps.put(USERNAME, postgresContainer.getUsername());
        postgresProps.put(PASSWORD, postgresContainer.getPassword());
        postgresProps.put("counted_words", postgresContainer.getDatabaseName());
        postgresProps.put("driver", Driver.class.getName());
    
        try (Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),postgresContainer.getUsername(),postgresContainer.getPassword());
                Statement st = connection.createStatement();) {
        	st.execute("DROP TABLE IF EXISTS counted_words;");
            st.execute("CREATE TABLE counted_words(word_id SERIAL PRIMARY KEY  NOT NULL, word CHARACTER VARYING(189819) UNIQUE NOT NULL, number INTEGER NOT NULL);");
            st.execute("INSERT INTO counted_words (word, number) VALUES ('word1', 200);");
    	}
    }
    
	@Test
	void test() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();		
		
		env.getConfig().disableClosureCleaner();
		
		DataStream<String> dataStream = env.fromElements("word1", "word2", "word3", "word4", "word5");
		
		DataStream<Tuple2<String, Integer>> dataFirstMidStream = dataStream.map(new WordsExistingCheck());
		
		DataStream<Tuple2<String, Integer>> newWordStream = dataFirstMidStream.filter(new NewWordsFilter());
		DataStream<Tuple2<String, Integer>> oldWordStream = dataFirstMidStream.filter(new OldWordsFilter());
        		
        oldWordStream.addSink(
                JdbcSink.sink(
                        UPDATE_SQL_QUERY,
                        (statement, tuple) -> {
                            String word = tuple.f0;
                            int number = tuple.f1;

                            statement.setString(2, word);
                            statement.setInt(1, number);
                        },
                        jdbcExecutionOptions(),
                        jdbcConnectionOptions()
                ));
		
        newWordStream.addSink(
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



        env.execute("Flink Java API Skeleton");
		
        Thread.sleep(5000);
        
		List<ResultPojo> actual = new ArrayList<>();
		List<ResultPojo> expected = Arrays.asList(new ResultPojo("word1", 1));
		
		Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),postgresContainer.getUsername(),postgresContainer.getPassword());
        Statement st = connection.createStatement();
		PreparedStatement resultStatement = connection.prepareStatement("SELECT word, number FROM counted_words;");
		ResultSet resultSet = resultStatement.executeQuery();
		
		while(resultSet.next()) {
			actual.add(new ResultPojo(resultSet.getString("word"), resultSet.getInt("number")));
		}
		assertEquals(expected, actual);
	}
	
	public static JdbcExecutionOptions jdbcExecutionOptions() {
		return JdbcExecutionOptions.builder()
		        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
		        .withBatchSize(1000)                  // optional: default = 5000 values
		        .withMaxRetries(5)                    // optional: default = 3 
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
	
}
