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

package com.example.sharov.anatoliy.flink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.DataStreamJob;
import com.example.sharov.anatoliy.flink.protobuf.NewsProtos.News;
import com.twitter.chill.protobuf.ProtobufSerializer;

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

	public static final int HOVER_TIME = 6000;
	public static final String DEFAULT_TOPIC = "protobuf-topic/butnot";
	public static final String DEFAULT_KAFKA_GROUP = "mygroup";
	public static final String DEFAULT_BOOTSTAP_SERVERS = "broker:9092";
	
	
	public static final String DEFAULT_URL = "jdbc:postgresql://database:5432/newses";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String DEFAULT_DATABASE_USER = "crawler/butnot";
	public static final String DEFAULT_DATABASE_PASSWORD = "1111/butnot";
	public static final String NAME_OF_STREAM = "Kafka Source";
	
	public static final String COLOMN_OF_TITLE = "title";
	public static final String COLOMN_OF_BODY = "text";
	public static final String COLOMN_OF_LINK = "link";
	public static final String COLOMN_OF_TAG = "tag";
	
	public static final String CHECKING_TABLE_NAME = "newses";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";
	public static final String SELECT_NEWS_HASH_CODE = "SELECT * FROM news WHERE hash_code = ?";
	public static final String FETCH_NEW_ID = "SELECT nextval('newses_id_seq')";
	public static final String INSERT_NEWS = "INSERT INTO newses (id, title, text, link, hash_news) VALUES (?, ?, ?, ?, ?)";
	public static final String INSERT_TAGS = "INSERT INTO tags (id_news, tag) VALUES (?, ?)";
	
	
	private	static String topic = System.getenv("KAFKA_TOPIC") != null ? System.getenv("KAFKA_TOPIC") : DEFAULT_TOPIC;
	
	private	static String kafkaGroup = System.getenv("KAFKA_FLINK_GROUP") != null ? System.getenv("KAFKA_FLINK_GROUP") : DEFAULT_KAFKA_GROUP;
	private	static String databaseUrl = System.getenv("DATABASE_URL") != null ? System.getenv("DATABASE_URL") : DEFAULT_URL;
	private	static String username = System.getenv("DATABASE_USER") != null ? System.getenv("DATABASE_USER") : DEFAULT_DATABASE_USER;
	private	static String password = System.getenv("DATABASE_PASSWORD") != null ? System.getenv("DATABASE_PASSWORD") : DEFAULT_DATABASE_PASSWORD;
	
	private	static String bootstrapServers = (System.getenv("KAFKA_BROKER_HOST")!= null && System.getenv("KAFKA_BROKER_PORT")!= null) ?
		System.getenv("KAFKA_BROKER_HOST") + ":" + System.getenv("KAFKA_BROKER_PORT") : DEFAULT_BOOTSTAP_SERVERS;
	
	/*		
	private	static String topic = DEFAULT_TOPIC;
	private	static String kafkaGroup = DEFAULT_KAFKA_GROUP;
	private	static String bootstrapServers = DEFAULT_BOOTSTAP_SERVERS;
	private	static String databaseUrl = DEFAULT_URL;
	private	static String username = DEFAULT_DATABASE_USERNAME;
	private	static String password = DEFAULT_DATABASE_PASSWORD;
		*/
	
	public static void main(String[] args) throws Exception {
		
		
		DataStreamJob dataStreamJob = new DataStreamJob();
		KafkaSource<News> source = KafkaSource.<News>builder().setBootstrapServers(bootstrapServers)
					.setTopics(topic)
					.setGroupId(kafkaGroup)
					.setValueOnlyDeserializer(new CustomProtobufDeserializer())
					.setUnbounded(OffsetsInitializer.latest())
					.build();

		dataStreamJob.processData(source);
	    }
	
	@SuppressWarnings("serial")
	public void processData(KafkaSource<News> source) throws Exception {
		InspectionUtil inspectionUtil = new InspectionUtil();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addDefaultKryoSerializer(News.class, ProtobufSerializer.class);
		inspectionUtil.waitForDatabaceAccessibility(databaseUrl,username, password, CHECKING_TABLE_NAME, HOVER_TIME);
		inspectionUtil.waitForTopicAvailability(topic, bootstrapServers, HOVER_TIME);
		DataStream<News> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);
		DataStream<ParsedNews> dataFirstMidStream = kafkaStream.map(new MapFunction<News, ParsedNews>() {

			@Override
			public ParsedNews map(News message) throws Exception {
				ParsedNews result = new ParsedNews();
				
				result.setTitle(message.getTitle());
				result.setBody(message.getBody());
				result.setLink(message.getLink());
				result.setTags(message.getTagsList());
				return result;
			}
		});
		dataFirstMidStream.filter(new FilterFunction<ParsedNews>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unlikely-arg-type")
			@Override
			public boolean filter(ParsedNews value) throws Exception {
				Boolean result = true;
				
				try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
						PreparedStatement ps = connect.prepareStatement(SELECT_NEWS_HASH_CODE);) {
					ps.setInt(1, value.hashCode());
					ResultSet resultSet = ps.executeQuery();

					if (resultSet.next()) {
						result = !value.equals(resultSet.getString(COLOMN_OF_BODY));
					}
				} catch (SQLException e) {
				}
				return result;
			}
		})
		.map((news)->{
			ParsedNews result = news;
			try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
					PreparedStatement ps = connect.prepareStatement(FETCH_NEW_ID)){
				ResultSet resultSet = ps.executeQuery();

				if(resultSet.next()) {
					result.setId(resultSet.getLong("nextval"));
				}else {
					result.setId(2L);
				}
			}catch(SQLException e) {
			}
			return result;
		})
				.addSink(JdbcSink.sink(INSERT_NEWS,
						(statement, parsedWord) -> {
							statement.setLong(1, parsedWord.getId());
							statement.setString(2, parsedWord.getTitle());
							statement.setString(3, parsedWord.getBody());
							statement.setString(4, parsedWord.getLink());
							statement.setInt(5, parsedWord.hashCode());
						}, jdbcExecutionOptions(), jdbcConnectionOptions()));

		env.execute("MyFlink");
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
				.withUrl(databaseUrl)
				.withDriverName(SQL_DRIVER)
				.withUsername(username)
				.withPassword(password)
				.build();
	}

}