package com.example.sharov.anatoliy.crawlerkafkasimulator;

import com.example.sharov.anatoliy.crawlerkafkasimulator.protobuf.NewsProtos;

import java.util.List;
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
	
	public static final String INSTANCE = "Title_1|BodyOfNews_1|excample.site_1|excample.site_1/Title_1|teg1, teg2|1999-01-08 13:21\n"
			+ "Title_2|BodyOfNews_2|excample.site_1|excample.site_1/Title_2|teg1, teg2|2000-01-08 13:21\n"
			+ "Title_3|BodyOfNews_3|excample.site_2|excample.site_2/Title_3|teg1, teg3|2001-01-08 13:21\n"
			+ "Title_4|BodyOfNews_4|excample.site_1|excample.site_1/Title_4|teg1, teg4|2002-01-08 13:21\n"
			+ "Title_5|BodyOfNews_5|excample.site_1|excample.site_1/Title_5|teg5, teg2|2003-01-08 13:21";

	public static final String INPUT_TOPIC = "gate_of_word";
	public static final String BOOTSTAP_SERVERS = "localhost:9092";
	public static final String ASK = "all";

	public static void main(String[] args) throws Exception {


		Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		Instance instance = new Instance();
		@SuppressWarnings("resource")
		Producer<String, NewsProtos.News> producer = new KafkaProducer<String, NewsProtos.News>(properties);
		List<ParsedNews> bunchOfNews = instance.generate(INSTANCE);
		
		for (ParsedNews parsedNews : bunchOfNews) {
			NewsProtos.News messageNews = NewsProtos.News.newBuilder()
					.setTitle(parsedNews.getTitle())
					.setBody(parsedNews.getBody())
					.setLink(parsedNews.getLink())
					.addAllTegs(parsedNews.getTegs())
					.build();
			producer.send(new ProducerRecord<String, NewsProtos.News>(INPUT_TOPIC, messageNews));
			System.out.println("Messages ok");
		}
	}
	
}
