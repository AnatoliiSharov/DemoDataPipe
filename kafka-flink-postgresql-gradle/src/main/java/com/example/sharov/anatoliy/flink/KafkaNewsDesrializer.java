package com.example.sharov.anatoliy.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.example.sharov.anatoliy.flink.protobuf.NewsProtos;

@SuppressWarnings("serial")
public class KafkaNewsDesrializer implements KafkaDeserializationSchema<NewsProtos.News>{

	@Override
	public TypeInformation<NewsProtos.News> getProducedType() {
		return TypeInformation.of(NewsProtos.News.class);
	}

	@Override
	public boolean isEndOfStream(NewsProtos.News nextElement) {
		// return nextElement == null //mark end of stream
		return false;
	}

	@Override
	public NewsProtos.News deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		// TODO Auto-generated method stub
		return NewsProtos.News.parseFrom(record.value());
	}

}
