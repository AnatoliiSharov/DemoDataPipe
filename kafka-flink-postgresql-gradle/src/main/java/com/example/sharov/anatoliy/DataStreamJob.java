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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
	public final static String INPUT_TOPIC = "gate_of_word";
	public final static String KAFKA_GROUP = "possession_of_pipeline";
	public final static String BOOTSTAP_SERVERS = "localhost:9092";
	public final static String URL = "jdbc:postgresql://localhost:5432/counted_words";
	public final static String SQL_DRIVER = "org.postgresql.Driver";

	public final static String USERNAME = "postgres";
	public final static String PASSWORD = "1111";
	public final static String NAME_OF_STREAM = "Kafka Source";
	public final static String COLOMN_OF_RESULT = "number";
	public final static String NAME_OF_FLINK_JOB = "Flink Job";
	public final static String SELECT_SQL_QUERY = "SELECT * FROM counted_words WHERE word = ?";
	public final static String INSERT_SQL_QUERY = "INSERT INTO counted_words (word, number) VALUES (?, ?)";
	public final static String UPDATE_SQL_QUERY = "UPDATE counted_words SET number = ? WHERE word = ?";

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(BOOTSTAP_SERVERS)
				.setTopics(INPUT_TOPIC)
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
				.setUnbounded(OffsetsInitializer.latest()).build();

		DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);

		DataStream<Tuple2<String, Integer>> dataFirstMidStream = kafkaStream
				.map(new MapFunction<String, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> map(String word) throws Exception {
						int count = 0;
						try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
								PreparedStatement ps = connect.prepareStatement(SELECT_SQL_QUERY);) {
							ps.setString(1, word);
							ResultSet resultSet = ps.executeQuery();

							if (resultSet.next()) {
								count = resultSet.getInt(COLOMN_OF_RESULT);
							}
						}
						return new Tuple2<>(word, count + 1);
					}
				});

		DataStream<Tuple2<String, Integer>> newWordStrim = dataFirstMidStream
				.filter(new FilterFunction<Tuple2<String, Integer>>() {

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f1 != 1;
					}
				});

		DataStream<Tuple2<String, Integer>> oldWordStrim = dataFirstMidStream
				.filter(new FilterFunction<Tuple2<String, Integer>>() {

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f1 == 1;
					}
				});

		newWordStrim.addSink(JdbcSink.sink(INSERT_SQL_QUERY, (statement, value) -> {
			int count = value.f1;
			String word = value.f0;
			statement.setString(1, word);
			statement.setInt(2, count);
		}, JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(URL).withDriverName(SQL_DRIVER)
						.withUsername(USERNAME).withPassword(PASSWORD).build()));

		oldWordStrim.addSink(JdbcSink.sink(UPDATE_SQL_QUERY, (statement, value) -> {
			int count = value.f1;
			String word = value.f0;
			statement.setString(1, word);
			statement.setInt(2, count);
		}, JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(URL).withDriverName(SQL_DRIVER)
						.withUsername(USERNAME).withPassword(PASSWORD).build()));

		env.execute("Flink Java API Skeleton");
	}
}
