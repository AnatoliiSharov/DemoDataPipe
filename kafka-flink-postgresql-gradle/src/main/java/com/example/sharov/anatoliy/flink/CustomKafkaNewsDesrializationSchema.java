package com.example.sharov.anatoliy.flink;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.example.sharov.anatoliy.flink.protobuf.NewsProtos.News;

@SuppressWarnings("serial")
public class CustomKafkaNewsDesrializationSchema implements DeserializationSchema<News>{
	
	@Override
	public TypeInformation<News> getProducedType() {
		return TypeInformation.of(News.class);
	}

	@Override
	public boolean isEndOfStream(News nextElement) {
		// stream without end
		return false;
	}


	@Override
	public News deserialize(byte[] message) throws IOException {
		return News.parseFrom(message);
	}

}
