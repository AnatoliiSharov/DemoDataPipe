package com.example.sharov.anatoliy.simpleserialize.producer;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.simpleserialize.producer.protobuf.NewsProtos;


public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	public static final String INSTANCE = "Title_1|BodyOfNews_1|excample.site_1|teg1, teg2\nTitle_2|BodyOfNews_2|excample.site_1|teg1, teg2\n"
			+ "Title_3|BodyOfNews_3|excample.site_2|teg1, teg3\n"
			+ "Title_4|BodyOfNews_4|excample.site_1|teg1, teg4\n"
			+ "Title_5|BodyOfNews_5|excample.site_1|teg5, teg10";

	public static final String DEFAULT_TOPIC = "protobuf-data";
	public static final String DEFAULT_HOST = "broker"; 
	public static final String DEFAULT_PORT = "9092";
	public static final String ASK = "all";

	public static void main(String[] args) {
		
		/*
		String topicName = System.getenv("KAFKA_TOPIC") != null ? System.getenv("KAFKA_TOPIC") : DEFAULT_TOPIC;
		String hostName = System.getenv("KAFKA_HOST") != null ? System.getenv("KAFKA_HOST") : DEFAULT_HOST;
		String portNumber = System.getenv("DEFAULT_PORT") != null ? System.getenv("DEFAULT_PORT") : DEFAULT_PORT;
		String bootstrapServers = hostName + ":" + portNumber;
		*/
		
		String topicName = DEFAULT_TOPIC;
		String hostName = DEFAULT_HOST;
		String portNumber = DEFAULT_PORT;
		String bootstrapServers = hostName + ":" + portNumber;
		
		Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		Instance instance = new Instance();
		@SuppressWarnings("resource")
		Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
		List<ParsedNews> bunchOfNews = instance.generate(INSTANCE);
		for (ParsedNews parsedNews : bunchOfNews) {
			
			NewsProtos.News messageNews = NewsProtos.News.newBuilder()
					.setTitle(parsedNews.getTitle())
					.setBody(parsedNews.getBody())
					.setLink(parsedNews.getLink())
					.addAllTags(parsedNews.getTags())
					.build();
			
			producer.send(new ProducerRecord<String, byte[]>(topicName, messageNews.toByteArray()));
			System.out.println("Messages ok to " + bootstrapServers + " with " + topicName);
		}
			producer.close();
	}
	
}
