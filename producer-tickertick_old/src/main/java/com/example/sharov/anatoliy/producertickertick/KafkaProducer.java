package com.example.sharov.anatoliy.producertickertick;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaProducer {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String msg) {
	    //kafkaTemplate.send(topicName, msg);
	}
	
}
