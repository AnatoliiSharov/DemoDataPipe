package com.example.sharov.anatoliy.flink;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.example.sharov.anatoliy.flink.protobuf.NewsProtos.News;

public class CustomProtobufKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<News>{

	@Override
	public TypeInformation<News> getProducedType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<News> out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
