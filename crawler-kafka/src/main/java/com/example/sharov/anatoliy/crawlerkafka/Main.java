package com.example.sharov.anatoliy.crawlerkafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static final String INPUT_TOPIC = "gate_of_word";
	public static final String BOOTSTAP_SERVERS = "localhost:9092";
	public static final String ASK = "all";

	public static void main(String[] args) throws Exception {
	

		Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


		Producer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>(INPUT_TOPIC, Integer.toString(i)));
		System.out.println("Message ok");
		producer.close();
	}

}
