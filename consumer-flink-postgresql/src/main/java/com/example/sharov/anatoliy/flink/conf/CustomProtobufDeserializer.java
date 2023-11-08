package com.example.sharov.anatoliy.flink.conf;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;;

@SuppressWarnings("serial")
public class CustomProtobufDeserializer implements DeserializationSchema<Story>{

	@Override
	public TypeInformation<Story> getProducedType() {
		return TypeInformation.of(Story.class);
	}

	@Override
	public Story deserialize(byte[] message) throws IOException {
		return Story.parseFrom(message);
	}

	@Override
	public boolean isEndOfStream(Story nextElement) {
		return false;
	}

}