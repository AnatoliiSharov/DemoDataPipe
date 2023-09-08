package com.example.sharov.anatoliy.flinkexam;


import org.apache.kafka.common.serialization.Deserializer;

import com.example.sharov.anatoliy.flinkexam.protobuf.NewsProtos;
import com.google.protobuf.Message;

public class KafkaProtobufNewsDeserializer <News extends Message> implements Deserializer<NewsProtos.News> {

	private final Class<NewsProtos.News> message;

    public KafkaProtobufNewsDeserializer(Class<NewsProtos.News> message) {
        this.message = message;
    }
	
	@SuppressWarnings("unchecked")
	@Override
	public NewsProtos.News deserialize(String topic, byte[] data) {
        try {
        	//TODO here is a reflection, see at later
        	/*
        	Class clazz = Class.forName("News");
            Method parseFromMethod = clazz.getDeclaredMethod("parseFrom", payload.getClass());
            Object protobufResult = parseFromMethod.invoke(null, payload);
			*/
        	return (NewsProtos.News) message.getDeclaredMethod("parseFrom", byte[].class).invoke(null, data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Protobuf message", e);
        }
    }

}
