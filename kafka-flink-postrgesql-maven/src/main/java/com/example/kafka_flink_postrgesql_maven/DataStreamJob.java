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

package com.example.kafka_flink_postrgesql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
	public final static String INPUT_TOPIC = "gate_of_word";
	public final static String KAFKA_GROUP = "possession_of_pipeline";
	public final static String BOOTSTAP_SERVERS = "localhost:9092";
	public final static String URL = "jdbc:postgresql://localhost:5432/db_counted_word";
	public final static String USERNAME = "postgres";
	public final static String PASSWORD = "1111";
	public final static String NAME_OF_STREAM = "Kafka Source";
	public final static String COLOMN_OF_RESULT = "count";
	public final static String NAME_OF_FLINK_JOB = "Flink Job";
	public final static String SELECT_SQL_QUERY = "SELECT * FROM mytable WHERE id = ?";
	public final static String INSERT_SQL_QUERY = "INSERT INTO table (count, word) VALUES (?, ?)";
	public final static String UPDATE_SQL_QUERY = "UPDATE table SET count = ? WHERE word = ?";
	
	
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(BOOTSTAP_SERVERS)
				.setTopics(INPUT_TOPIC)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();
		
		DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);

		DataStream<Tuple2<String, Integer>> dataFirstMidStream = kafkaStream
				.map(new MapFunction<String, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> map(String word) throws Exception {
						String url = URL;
						String username = USERNAME;
						String password = PASSWORD;
						int count = 0;

						try (Connection connect = DriverManager.getConnection(url, username, password);
								PreparedStatement ps = connect
										.prepareStatement(SELECT_SQL_QUERY);) {
							ps.setString(1, word);
							ResultSet resultSet = ps.executeQuery();

							if (resultSet.next()) {
								count = resultSet.getInt(COLOMN_OF_RESULT);
							}
						}
						return new Tuple2<>(word, count+1);
					}
				});

		dataFirstMidStream.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
			private PreparedStatement insertStatement;
			private PreparedStatement updateStatement;
			private Connection connection;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				String url = URL;
				String username = USERNAME;
				String password = PASSWORD;
				Connection connection = DriverManager.getConnection(url, username, password);
				String insertQuery = INSERT_SQL_QUERY;
				insertStatement = connection.prepareStatement(insertQuery);
				String updateQuery = UPDATE_SQL_QUERY;
				updateStatement = connection.prepareStatement(updateQuery);
			}

			@Override
			public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
				int count = value.f1;
				String word = value.f0;

				if (count == 0) {
					insertStatement.setString(1, word);
					insertStatement.setInt(2, count);
					insertStatement.executeUpdate();
				} else {
					updateStatement.setString(1, word);
					updateStatement.setInt(2, count);
					updateStatement.executeUpdate();
				}
			}

			@Override
			public void close() throws Exception {
				super.close();

				if (insertStatement != null) {
					insertStatement.close();
				}

				if (updateStatement != null) {
					updateStatement.close();
				}

				if (connection != null) {
					connection.close();
				}
			}
		});
		
		env.execute("Flink Java API Skeleton");
	}
}
