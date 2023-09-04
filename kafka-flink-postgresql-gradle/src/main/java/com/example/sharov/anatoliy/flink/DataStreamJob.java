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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.protobuf.NewsProtos;
import com.example.sharov.anatoliy.flink.protobuf.NewsProtos.News;

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

	public static final int HOVER_TIME = 3000;
	public static final String TOPIC = "mytopic";
	public static final String KAFKA_GROUP = "possession_of_pipeline";
	public static final String BOOTSTAP_SERVERS = "broker:29092";
	public static final String URL = "jdbc:postgresql://database:5432/counted_words";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String USERNAME = "postgres";
	public static final String PASSWORD = "1111";
	public static final String NAME_OF_STREAM = "Kafka Source";
	
	public static final String COLOMN_OF_TITLE = "title";
	public static final String COLOMN_OF_BODY = "body";
	public static final String COLOMN_OF_LINK = "link";
	public static final String COLOMN_OF_TEGS = "tegs";
	
	public static final String TABLE_NAME = "counted_words";
	public static final String COLOMN_OF_WORD = "word";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";
	public static final String SELECT_NEWS_HASH_CODE = "SELECT * FROM news WHERE hash_code = ?";
	public static final String INSERT_NEWS = "INSERT INTO newses (title, body, link, hash_code) VALUES (?, ?, ?)";
	public static final String RETURNING_ID_NEWS = "RETURNING id_column";
	public static final String INSERT_TEGS = "INSERT INTO tegs (id_news, tegs) VALUES (?, ?)";
	
	public static void main(String[] args) throws Exception {
	       DataStreamJob dataStreamJob = new DataStreamJob();
		KafkaSource<NewsProtos.News> source = KafkaSource.<NewsProtos.News>builder().setBootstrapServers(BOOTSTAP_SERVERS)
					.setTopics(TOPIC)
					.setDeserializer((KafkaRecordDeserializationSchema<NewsProtos.News>) new KafkaNewsDesrializer())
					.setUnbounded(OffsetsInitializer.latest()).build();
	       
			
	       
	       LOG.debug("DataStreamJob get source from Kafka");
	       dataStreamJob.processData(source);
	    }
	
	@SuppressWarnings("serial")
	public void processData(KafkaSource<NewsProtos.News> source) throws Exception {
		InspectionUtil inspectionUtil = new InspectionUtil();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.getConfig().registerTypeWithKryoSerializer(NewsProtos.News.class, ProtobufSerializer.class);
		inspectionUtil.waitForDatabaceAccessibility(URL,USERNAME, PASSWORD, TABLE_NAME, HOVER_TIME);
		inspectionUtil.waitForTopicAvailability(TOPIC, BOOTSTAP_SERVERS, HOVER_TIME);
		LOG.info("DataStreamJob finished to wait Kafka and Postgres");
		DataStream<NewsProtos.News> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);
		
		DataStream<ParsedNews> jobStream = kafkaStream.map(e -> parseByteToParsedNews(e));
		DataStream<ParsedNews> streamWithoutDoubles = jobStream.filter(new FilterFunction<ParsedNews>() {
			@Override
			public boolean filter(ParsedNews parsedNews) throws Exception {
				Boolean result = false;
				
				try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
						PreparedStatement ps = connect.prepareStatement(SELECT_NEWS_HASH_CODE);) {
					ps.setInt(1, parsedNews.hashCode());
					ResultSet resultSet = ps.executeQuery();

					if (resultSet.next()) {
						result = parsedNews.getBody().equals(resultSet.getString(COLOMN_OF_BODY));
					}
				} catch (SQLException e) {
				}
				return result;
			}
		});
		
		DataStream<ParsedNews> newsesStreamWithNewsId = streamWithoutDoubles.map(news -> {
			
			try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
					PreparedStatement ps = connect.prepareStatement("SELECT nextval('news_id_seq')")){
				
				ResultSet resultSet = ps.executeQuery();
				if(resultSet.next()) {
					news.setId(resultSet.getLong("id"));
				}
			}catch(SQLException e) {
			}
			return news;
		});
		
		DataStream<Tuple2<Long, String>> tegsStreamWithNewsId = newsesStreamWithNewsId.flatMap(new FlatMapFunction<ParsedNews, Tuple2<Long, String>>(){

			@Override
			public void flatMap(ParsedNews news, Collector<Tuple2<Long, String>> out) throws Exception {
				for(String tag : news.getTegs()) {
	        		out.collect(new Tuple2<>(news.getId(), tag));
	        	}				
			}
        	 
        });
		
		newsesStreamWithNewsId.addSink(JdbcSink.sink(INSERT_NEWS,

						(statement, parsedNews) -> {
							
							statement.setLong(1, parsedNews.getId());
							statement.setString(2, parsedNews.getTitle());
							statement.setString(3, parsedNews.getBody());
							statement.setString(4, parsedNews.getLink());
							statement.setInt(5, parsedNews.hashCode());
						}, jdbcExecutionOptions(), jdbcConnectionOptions())).name("NewsWithoutTagsJdbcSink").setParallelism(1);
        
		tegsStreamWithNewsId.addSink(JdbcSink.sink(INSERT_TEGS,

        		(statement, tuple) -> {
					statement.setLong(1, tuple.f0);
					statement.setString(2, tuple.f1);
				}, jdbcExecutionOptions(), jdbcConnectionOptions())).name("TagsJdbcSink");

        env.execute("MyFlink");
			}

	private ParsedNews parseByteToParsedNews(News messageNews) {
		
		ParsedNews parsedNews = new ParsedNews();
		parsedNews.setTitle(messageNews.getTitle());
		parsedNews.setBody(messageNews.getBody());
		parsedNews.setLink(messageNews.getLink());
		parsedNews.setTegs(messageNews.getTegsList());
		return parsedNews;
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
				.withUrl(URL)
				.withDriverName(SQL_DRIVER)
				.withUsername(USERNAME)
				.withPassword(PASSWORD)
				.build();
	}

}
