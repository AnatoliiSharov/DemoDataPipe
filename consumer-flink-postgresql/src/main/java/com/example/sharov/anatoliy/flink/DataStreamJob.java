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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.DataStreamJob;
import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.CustomProtobufDeserializer;
import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.conf.InspectionUtil;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;
import com.example.sharov.anatoliy.flink.conf.StoryMessageParser;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.process.DataSink;
import com.example.sharov.anatoliy.flink.process.NewStoriesFilter;
import com.example.sharov.anatoliy.flink.process.TegIdHandler;
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
	public static final String NAME_OF_STREAM = "Kafka Source";
	public static final String CHECKING_TABLE_NAME = "stories";
	public static final String NAME_OF_FLINK_JOB = "Flink Job";

	public static void main(String[] args) throws Exception {
		ConfParams conf = new ConfParams();

		DataStreamJob dataStreamJob = new DataStreamJob();
		KafkaSource<Story> source = KafkaSource.<Story>builder().setBootstrapServers(conf.getBootstrapServers())
				.setTopics(conf.getTopic()).setGroupId(conf.getKafkaGroup())
				.setValueOnlyDeserializer(new CustomProtobufDeserializer()).setUnbounded(OffsetsInitializer.latest())
				.build();

		dataStreamJob.processData(source);
	}

	@SuppressWarnings("serial")
	public void processData(KafkaSource<Story> source) throws Exception {
		ConfParams conf = new ConfParams();
		InspectionUtil inspectionUtil = new InspectionUtil();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addDefaultKryoSerializer(Story.class, ProtobufSerializer.class);
		inspectionUtil.waitForDatabaceAccessibility(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword(),
				CHECKING_TABLE_NAME, HOVER_TIME);
		inspectionUtil.waitForTopicAvailability(conf.getTopic(), conf.getBootstrapServers(), HOVER_TIME);

		LOG.info("DataStreamJob job process started");
		
		DataStream<Story> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), NAME_OF_STREAM);
		DataStream<StoryPojo> newStories = kafkaStream.map(new StoryMessageParser()).filter(new NewStoriesFilter());
		DataStream<StoryPojo> checkedTagsStream = newStories.map(new TegIdHandler());

		checkedTagsStream.addSink(new DataSink());
		env.execute("MyFlink");
	}

	/*
	public static JdbcExecutionOptions jdbcExecutionOptions() {
		return JdbcExecutionOptions.builder().withBatchIntervalMs(200) // optional: default = 0, meaning no time-based
				.withBatchIntervalMs(200) // optional: default = 0, meaning no time-based execution is done
				.withBatchSize(1000) // optional: default = 5000 values
				.withMaxRetries(5) // optional: default = 3
				.build();
	}

	public static JdbcConnectionOptions jdbcConnectionOptions() {
		ConfParams conf = new ConfParams();
		return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(conf.getBootstrapServers())
				.withDriverName(conf.getSqlDriver()).withUsername(conf.getUsername()).withPassword(conf.getPassword())
				.build();
	}
*/
	
}