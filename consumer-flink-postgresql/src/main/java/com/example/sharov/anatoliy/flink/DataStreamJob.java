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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
import org.apache.flink.shaded.guava30.com.google.common.base.Optional;
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
	public static final String FETCH_TAG_ID = "SELECT nextval('tag_id_seq')";
	public static final String FETCH_SIMILAR_STORY_ID = "SELECT nextval('similar_story_id_seq')";
	public static final String SELECT_ID_FROM_SIMILAR_STORIES = "SELECT * FROM stories WHERE similar_story = ?";
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

		DataStream<StoryFlink> newStories = kafkaStream.map(new MapFunction<Story, StoryFlink>() {

			@Override
			public StoryFlink map(Story message) throws Exception {
				return new StoryFlink().parseFromMessageNews(message);
			}
		}).filter(new FilterFunction<StoryFlink>() {
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unlikely-arg-type")
			@Override
			public boolean filter(StoryFlink value) throws Exception {
				Boolean result = true;

				try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
						PreparedStatement psCheck = connect.prepareStatement(SELECT_ID_FROM_STORIES);) {
					psCheck.setString(1, value.getId());
					ResultSet resultSetLook = psCheck.executeQuery();

					if (resultSetLook.next()) {
						result = false;
					}
				} catch (SQLException e) {
					LOG.debug("for find by id = {} from stories threw SQLException e = {}", value.getId(), e);
				}
				return result;
			}
		});

		DataStream<StoryFlink> checkedTagsStream =
		newStories.map(new MapFunction<StoryFlink, StoryFlink>() {

			@Override
			public StoryFlink map(StoryFlink story) throws Exception {
				List<Tuple2<Long, String>>result = story.getTags();
				
				result.replaceAll(tupleTag -> {
					
					if(tupleTag.f0 != -1) {
						String tag = tupleTag.f1;
						
						try (Connection connect = DriverManager.getConnection(databaseUrl, username, password);
								PreparedStatement psCheck = connect.prepareStatement(SELECT_ID_FROM_TAGS);
								PreparedStatement psNewId = connect.prepareStatement(FETCH_TAG_ID)) {
							psCheck.setString(1, tag);
							ResultSet rsCheck = psCheck.executeQuery();

							if (!rsCheck.next()) {
								ResultSet rsNewId = psNewId.executeQuery();

								if (rsNewId.next()) {
									tupleTag.of(rsNewId.getLong("nextval"), tag);
								}
							}
						} catch (SQLException e) {
							LOG.debug("SQLException e = {} for check or insert tag  = {} from story ={} ", e, tag,
									story);
						}
					}
					return tupleTag;
					
				});
				story.setTags(result);
				return story;	
			}
		});
			
			

		
//public static final String INSERT_STORIES = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";		
		SinkFunction<StoryFlink>  sinkStory = JdbcSink.sink(INSERT_STORIES, (statement, story)
				  -> { 
					  String[] row = mapToRowForStoryTable(story);
					  for(int i = 0; i < row.length; i++) {
					  statement.setString(i+1, row[i]);
					  }
				  },
				  jdbcExecutionOptions(), jdbcConnectionOptions());
		
		SinkFunction<StoryFlink>  sinkTag = JdbcSink.sink(INSERT_TAGS, (statement, story)
				  -> { 
					  for(Tuple2<Long, String> tupleTag : story.getTags()) {
						  
						  if(tupleTag.f0 != -1 || tupleTag.f0 != 0 || tupleTag.f0 != null) {
							  statement.setLong(1, tupleTag.f0);
							  statement.setString(1, tupleTag.f1);
						  }
					  }
				  },
				  jdbcExecutionOptions(), jdbcConnectionOptions());
		SinkFunction<StoryFlink>  sinkStoryAndTag = JdbcSink.sink(INSERT_TAGS, (statement, story)
				-> { 
					for(Tuple2<Long, String> tupleTag : story.getTags()) {
						
						if(tupleTag.f0 != -1 || tupleTag.f0 != 0 || tupleTag.f0 != null) {
							statement.setLong(1, tupleTag.f0);
							statement.setString(1, tupleTag.f1);
						}
					}
				},
				jdbcExecutionOptions(), jdbcConnectionOptions());
		
		
		checkedTagsStream.addSink(sinkStory);
		checkedTagsStream.addSink(sinkTag);
				  env.execute("MyFlink");
	}

				  
	
	private String[] mapToRowForStoryTable(StoryFlink story) {
		return new String[] {
				story.getId(),
				story.getTitle(),
				story.getUrl(),
				story.getSite(),
				story.getTime().toString(),
				story.getFavicon_url(),
				story.getDescription(),
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
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl).withDriverName(SQL_DRIVER)
				.withUsername(username).withPassword(password).build();
	}

}