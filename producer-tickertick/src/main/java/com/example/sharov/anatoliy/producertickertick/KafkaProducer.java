package com.example.sharov.anatoliy.producertickertick;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
@Service
public class KafkaProducer {
	
	private final String topic;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public KafkaProducer(String topic) {
		super();
		this.topic = topic;
	}



	public void sendMessage(String msg) {
	    kafkaTemplate.send(topic, msg);
	}
	
}
