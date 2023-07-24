/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.sharov.anatoliy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.SQL_DRIVER;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the tutorials and
 * examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class DataStreamJob {
	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);
	private static final JdbcOperations op = new JdbcOperations() ;
	
	
	public static final String INPUT_TOPIC = "gate_of_word";
	public static final String KAFKA_GROUP = "possession_of_pipeline";
	public static final String BOOTSTAP_SERVERS = "localhost:9092";
	public static final String URL = "jdbc:postgresql://localhost:5432/counted_words";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String USERNAME = "postgres";
	public static final String PASSWORD = "1111";
	public static final String NAME_OF_STREAM = "Kafka Source";
	public static final String COLOMN_OF_NUMBER = "number";
	public static final String COLOMN_OF_WORD = "word";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";
	public static final String SELECT_SQL_QUERY = "SELECT * FROM counted_words WHERE word = ?";
	public static final String INSERT_SQL_QUERY = "INSERT INTO counted_words (word, number) VALUES (?, ?)";
	public static final String UPDATE_SQL_QUERY = "UPDATE counted_words SET number = ? WHERE word = ?";

	
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(BOOTSTAP_SERVERS)
				.setTopics(INPUT_TOPIC)
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
				.setUnbounded(OffsetsInitializer.latest()).build();
		LOG.debug("DataStreamJob get source from Kafka");
		DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);
		LOG.debug("DataStreamJob get kafkaStream");
		DataStream<Tuple2<String, Integer>> dataFirstMidStream = kafkaStream.map(new LengthWordsDefinition());
		LOG.debug("DataStreamJob get LengthWordsDefinition");
		DataStream<Tuple2<String, Integer>> newWordStream = dataFirstMidStream.filter(new NewWordsFilter());

		newWordStream.addSink(JdbcSink.sink(
				UPDATE_SQL_QUERY,
				(statement, tuple) -> {
					LOG.debug("DataStreamJob addition Sink for newWordStrim with tuple = {}", tuple);
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
                        	LOG.debug("DataStreamJob addition Sink for notNewWordStrim with tuple = {}", tuple);
                        	String word = tuple.f0;
                        	int number = tuple.f1;
                        	
                            statement.setString(2, word);
                            statement.setInt(1, number);
                        },
                        jdbcExecutionOptions(),
                        jdbcConnectionOptions()
                ));

		env.execute("Flink Java API Skeleton");
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
                .withUrl(URL)
                .withDriverName(SQL_DRIVER)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .build();
	}

}
