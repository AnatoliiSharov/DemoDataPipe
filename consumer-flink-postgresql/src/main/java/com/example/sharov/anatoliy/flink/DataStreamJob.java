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
import java.sql.Timestamp;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.DataStreamJob;
import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;
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
	public static final String DEFAULT_TOPIC = "my-topic";
	public static final String DEFAULT_KAFKA_GROUP = "mygroup";
	public static final String DEFAULT_BOOTSTAP_SERVERS = "broker:9092";

	public static final String DEFAULT_URL = "jdbc:postgresql://database:5432/stories";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String DEFAULT_DATABASE_USER = "crawler";
	public static final String DEFAULT_DATABASE_PASSWORD = "1111";
	public static final String NAME_OF_STREAM = "Kafka Source";

	public static final String CHECKING_TABLE_NAME = "stories";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";
	public static final String SELECT_ID_FROM_STORIES = "SELECT * FROM stories WHERE id = ?";
	public static final String SELECT_ID_FROM_TAGS = "SELECT * FROM stories WHERE tag = ?";
	public static final String SELECT_ID_FROM_SIMILAR_STORIES = "SELECT * FROM stories WHERE similar_story = ?";
	/*
	 * public static final String FETCH_NEW_TAG_ID =
	 * "SELECT nextval('tags_id_seq')"; public static final String
	 * FETCH_NEW_SIMILAR_STORY_ID = "SELECT nextval('similar_stories_id_seq')";
	 */
	public static final String INSERT_STORIES = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String INSERT_TAGS = "INSERT INTO tags (tag) VALUES (?) RETURNING id";
	public static final String INSERT_STORIES_TAGS = "INSERT INTO stories_tags (id, story_id, tag_id) VALUES (?, ?, ?)";
	public static final String INSERT_SIMILAR_STORIES = "INSERT INTO similar_stories (id, similar_story) VALUES (?, ?)ls";
	public static final String INSERT_STORIES_SIMILAR_STORIES = "INSERT INTO stories_similar_stories (id, story_id, similar_story_id) VALUES (?, ?, ?)";

	private static String topic = System.getenv("KAFKA_TOPIC") != null ? System.getenv("KAFKA_TOPIC") : DEFAULT_TOPIC;
	private static String kafkaGroup = System.getenv("KAFKA_FLINK_GROUP") != null ? System.getenv("KAFKA_FLINK_GROUP")
			: DEFAULT_KAFKA_GROUP;
	private static String databaseUrl = System.getenv("DATABASE_URL") != null ? System.getenv("DATABASE_URL")
			: DEFAULT_URL;
	private static String username = System.getenv("DATABASE_USER") != null ? System.getenv("DATABASE_USER")
			: DEFAULT_DATABASE_USER;
	private static String password = System.getenv("DATABASE_PASSWORD") != null ? System.getenv("DATABASE_PASSWORD")
			: DEFAULT_DATABASE_PASSWORD;

	private static String bootstrapServers = (System.getenv("KAFKA_BROKER_HOST") != null
			&& System.getenv("KAFKA_BROKER_PORT") != null)
					? System.getenv("KAFKA_BROKER_HOST") + ":" + System.getenv("KAFKA_BROKER_PORT")
					: DEFAULT_BOOTSTAP_SERVERS;

	public static void main(String[] args) throws Exception {

		DataStreamJob dataStreamJob = new DataStreamJob();
		KafkaSource<Story> source = KafkaSource.<Story>builder().setBootstrapServers(bootstrapServers).setTopics(topic)
				.setGroupId(kafkaGroup).setValueOnlyDeserializer(new CustomProtobufDeserializer())
				.setUnbounded(OffsetsInitializer.latest()).build();

		dataStreamJob.processData(source);
	}

	@SuppressWarnings("serial")
	public void processData(KafkaSource<Story> source) throws Exception {
		InspectionUtil inspectionUtil = new InspectionUtil();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addDefaultKryoSerializer(Story.class, ProtobufSerializer.class);
		inspectionUtil.waitForDatabaceAccessibility(databaseUrl, username, password, CHECKING_TABLE_NAME, HOVER_TIME);
		inspectionUtil.waitForTopicAvailability(topic, bootstrapServers, HOVER_TIME);

		DataStream<Story> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);

		DataStream<StoryPojo> newStoriesFromKafkaStream = kafkaStream.map(new MapFunction<Story, StoryPojo>() {

			@Override
			public StoryPojo map(Story message) throws Exception {
				return new StoryPojo().parseFromMessageNews(message);
			}
		}).filter(new FilterFunction<StoryPojo>() {
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unlikely-arg-type")
			@Override
			public boolean filter(StoryPojo value) throws Exception {
				Boolean result = true;

				try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
						PreparedStatement psLook = connect.prepareStatement(SELECT_ID_FROM_STORIES);) {
					psLook.setString(1, value.getId());
					ResultSet resultSetLook = psLook.executeQuery();

					if (resultSetLook.next()) {
						result = false;
					} else {

						try (PreparedStatement psPut = connect.prepareStatement(INSERT_STORIES);) {
							psPut.setString(1, value.getId());
							psPut.setString(2, value.getTitle());
							psPut.setString(3, value.getUrl());
							psPut.setString(4, value.getSite());
							psPut.setTimestamp(5, null);
							psPut.setString(6, value.getFavicon_url());
							psPut.setString(7, value.getDescription());
							psPut.executeQuery();
						}
					}
				} catch (SQLException e) {
					LOG.debug("for find by id = {} from stories threw SQLException e = {}", value.getId(), e);
				}
				return result;
			}
		});

		DataStream<Tuple3<Long, String, String>> storyTagsStream = newStoriesFromKafkaStream
				.flatMap(new FlatMapFunction<StoryPojo, Tuple3<Long, String, String>>() {
					@Override
					public void flatMap(StoryPojo story, Collector<Tuple3<Long, String, String>> out) throws Exception {
						for (String tag : story.getTags()) {
							Long result = null;

							try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
									PreparedStatement psCheck = connect.prepareStatement(SELECT_ID_FROM_TAGS);
									PreparedStatement psInsert = connect.prepareStatement(INSERT_TAGS)) {
								psCheck.setString(1, tag);
								ResultSet rsCheck = psCheck.executeQuery();
								if (rsCheck.next()) {
									result = rsCheck.getLong(1);
								} else {
									psInsert.setString(1, tag);
									psInsert.executeUpdate();
									ResultSet rsInsert = psInsert.getGeneratedKeys();

									if (rsInsert.next()) {
										result = rsInsert.getLong(1);
									}
								}
							} catch (SQLException e) {
								LOG.debug("SQLException e = {} for check or insert tag  = {} from story ={} ", e, tag, story);
							}
							out.collect(new Tuple3<>(result, story.getId(), tag));
						}
					}
				});

		storyTagsStream.addSink(JdbcSink.sink(INSERT_STORIES_TAGS, (statement, value) -> {
			statement.setString(2, value.f1);
			statement.setLong(3, value.f0);

		}, jdbcExecutionOptions(), jdbcConnectionOptions()));

		newStoriesFromKafkaStream.addSink(JdbcSink.sink(INSERT_STORIES, (statement, story) -> {
			statement.setString(1, story.getId());
			statement.setString(2, story.getTitle());
			statement.setString(3, story.getUrl());
			statement.setString(4, story.getSite());
			statement.setTimestamp(5, story.getTime());
			statement.setString(6, story.getFavicon_url());
			statement.setString(7, story.getDescription());
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
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl).withDriverName(SQL_DRIVER)
				.withUsername(username).withPassword(password).build();
	}

}