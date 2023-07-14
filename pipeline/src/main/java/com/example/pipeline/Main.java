package com.example.pipeline;

import java.sql.Connection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.common.serialization.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Main {
	public final static String URL = "jdbc:postgresql://localhost:5432/database";
	public final static String USERNAME = "username";
	public final static String PASSWORD = "password";

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("localhost:9092")
				.setTopics("input-topic").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();

		DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

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
										.prepareStatement("SELECT * FROM mytable WHERE id = ?");) {
							ps.setString(1, word);
							ResultSet resultSet = ps.executeQuery();

							if (resultSet.next()) {
								count = resultSet.getInt("count");
							}
						}
						return new Tuple2<>(word, count);
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
				String insertQuery = "INSERT INTO table (count, word) VALUES (?, ?)";
				insertStatement = connection.prepareStatement(insertQuery);
				String updateQuery = "UPDATE table SET count = ? WHERE word = ?";
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

		env.execute("Flink Job");
	}
}