package com.example.sharov.anatoliy.producer;

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

import com.example.sharov.anatoliy.producer.protobuf.StoryProtos;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static final String INSTANCE = "id_1|title_1|url_1|site_1|1111111111|favicon_url_1|teg1, teg2|similar_stories_1, similar_stories_2|description\n"
			+ "id_2|title_2|url_1|site_1|1111111111|favicon_url_2|teg3|similar_stories_3|description2\n"
			+ "id_3|title_3|url_1|site_1|1111111111|favicon_url_3|teg3|similar_stories_3|description3\n"
			+ "id_4|title_4|url_1|site_1|1111111111|favicon_url_4| teg1, teg3|similar_stories_1, similar_stories_3|description4";

	public static final String DEFAULT_TOPIC = "my-topic";
	public static final String DEFAULT_HOST = "localhost";
	public static final String DEFAULT_PORT = "9092";
	public static final String ASK = "all";

	public static void main(String[] args) {

		String topicName = System.getenv("KAFKA_TOPIC") != null ? System.getenv("KAFKA_TOPIC") : DEFAULT_TOPIC;
		String hostName = System.getenv("KAFKA_BROKER_HOST") != null ? System.getenv("KAFKA_BROKER_HOST")
				: DEFAULT_HOST;
		String portNumber = System.getenv("KAFKA_BROKER_PORT") != null ? System.getenv("KAFKA_BROKER_PORT")
				: DEFAULT_PORT;
		String bootstrapServers = hostName + ":" + portNumber;

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		Instance instance = new Instance();
		Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
		List<StoryPojo> bunchOfStories = instance.generate(INSTANCE);

		for (StoryPojo parsedStory : bunchOfStories) {
			StoryProtos.Story message = StoryProtos.Story.newBuilder()
					.setId(parsedStory.getId())
					.setTitle(parsedStory.getTitle())
					.setUrl(parsedStory.getUrl())
					.setSite(parsedStory.getSite())
					.setTime(parsedStory.getTime())
					.setFaviconUrl(parsedStory.getFavicon_url())
					.addAllTags(parsedStory.getTags())
					.addAllSimilarStories(parsedStory.getSimilar_stories())
					.setUrl(parsedStory.getUrl()).build();
			producer.send(new ProducerRecord<String, byte[]>(topicName, message.toByteArray()));
			System.out.println("Messages " + message.toString() + " ok to " + bootstrapServers + " with " + topicName);
		}
		producer.close();

		try {
			Thread.sleep(600000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
