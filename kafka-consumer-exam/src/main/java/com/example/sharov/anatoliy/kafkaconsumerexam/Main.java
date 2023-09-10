package com.example.sharov.anatoliy.kafkaconsumerexam;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.kafkaconsumerexam.protobuf.NewsProtos;
import com.example.sharov.anatoliy.kafkaconsumerexam.protobuf.NewsProtos.News;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	public static final String INPUT_TOPIC = "mytopic";
	public static final String BOOTSTAP_SERVERS = "localhost:9092";
	public static final String ASK = "all";

	public static void main(String[] args) throws Exception {

		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTAP_SERVERS);
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());//KafkaProtobufDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
System.out.println(1);
		Consumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerProperties);
		
		consumer.subscribe(Arrays.asList(INPUT_TOPIC));
		consumer.seekToBeginning(consumer.assignment());
		ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
		
		for (ConsumerRecord<String, byte[]> record : records) {
		    System.out.println(parsedToPojoNews(News.parseFrom(record.value())));
		}
	}

	private static ParsedNews parsedToPojoNews(News message) {
		ParsedNews result = new ParsedNews();
		
		result.setTitle(message.getTitle());
		result.setBody(message.getBody());
		result.setLink(message.getLink());
		result.setTags(message.getTagsList());
		return result;
	}
	
}
