package com.example.sharov.anatoliy.simpleserialize.flink;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.example.sharov.anatoliy.simpleserialize.flink.protobuf.NewsProtos.News;

@SuppressWarnings("serial")
public class CustomProtobufDeserializer implements DeserializationSchema<News>{

	@Override
	public TypeInformation<News> getProducedType() {
		return TypeInformation.of(News.class);
	}

	@Override
	public News deserialize(byte[] message) throws IOException {
		return News.parseFrom(message);
	}

	@Override
	public boolean isEndOfStream(News nextElement) {
		return false;
	}

}
