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

package com.example.sharov.anatoliy.flinkexam;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flinkexam.protobuf.NewsProtos;
import com.example.sharov.anatoliy.flinkexam.protobuf.NewsProtos.News;
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

	public static final int HOVER_TIME = 3000;
	public static final String TOPIC = "mytopic";
	public static final String KAFKA_GROUP = "my-group";
	public static final String BOOTSTAP_SERVERS = "localhost:9092";
	public static final String URL = "jdbc:postgresql://database:5432/newses";
	public static final String SQL_DRIVER = "org.postgresql.Driver";

	public static final String USERNAME = "cawler";
	public static final String PASSWORD = "1111";
	public static final String NAME_OF_STREAM = "Kafka Source";
	
	public static final String COLOMN_OF_TITLE = "title";
	public static final String COLOMN_OF_BODY = "text";
	public static final String COLOMN_OF_LINK = "link";
	public static final String COLOMN_OF_TAGS = "tags";
	
	public static final String TABLE_NAME = "newses";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";
	public static final String SELECT_NEWS_HASH_CODE = "SELECT * FROM news WHERE hash_code = ?";
	public static final String FETCH_NEW_ID = "SELECT nextval('newses_id_seq')";
	public static final String INSERT_NEWS = "INSERT INTO newses (id, title, text, link, hash_news) VALUES (?, ?, ?, ?)";
	public static final String INSERT_TEGS = "INSERT INTO tegs (id_news, teg) VALUES (?, ?)";
	
	public static void main(String[] args) throws Exception {
		
		DataStreamJob dataStreamJob = new DataStreamJob();
	     
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "");
		properties.setProperty("group.id", "");
		
			
		KafkaSource<News> source = KafkaSource.<News>builder().setBootstrapServers(BOOTSTAP_SERVERS)
					.setTopics(TOPIC)
				    .setValueOnlyDeserializer(new CustomKafkaNewsDesrializationSchema())
					.build();
	       
	       dataStreamJob.processData(source);
	    }
	
	public void processData(KafkaSource<News> source) throws Exception {
		InspectionUtil inspectionUtil = new InspectionUtil();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		inspectionUtil.waitForTopicAvailability(TOPIC, BOOTSTAP_SERVERS, HOVER_TIME);

		DataStream<NewsProtos.News> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);
		env.getConfig().registerTypeWithKryoSerializer(NewsProtos.News.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(ParsedNews.class, ProtobufSerializer.class);
		DataStream<ParsedNews> jobStream = kafkaStream.map(e -> parseToParsedNews(e));
		
		jobStream.print();
        env.execute("MyFlink");
			}

	private ParsedNews parseToParsedNews(News messageNews) {
		ParsedNews parsedNews = new ParsedNews();

		parsedNews.setTitle(messageNews.getTitle());
		parsedNews.setBody(messageNews.getBody());
		parsedNews.setLink(messageNews.getLink());
		parsedNews.setTegs(messageNews.getTagsList());
		return parsedNews;
	}

}
