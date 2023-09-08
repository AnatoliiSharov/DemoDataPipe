package com.example.sharov.anatoliy.flinkexam;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.zookeeper3.io.netty.handler.codec.MessageAggregator;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.example.sharov.anatoliy.flinkexam.protobuf.NewsProtos.News;

@SuppressWarnings("serial")
public class CustomKafkaNewsDesrializationSchema implements DeserializationSchema<News>{
	
	@Override
	public TypeInformation<News> getProducedType() {
		// TODO Auto-generated method stub
		return TypeInformation.of(News.class);
	}

/*
	@Override
	public News deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		return News.parseFrom(record.value());
	}
*/
	@Override
	public boolean isEndOfStream(News nextElement) {
		// stream without end
		return false;
	}


	@Override
	public News deserialize(byte[] message) throws IOException {
		// TODO Auto-generated method stub
		return News.parseFrom(message);
	}

}
