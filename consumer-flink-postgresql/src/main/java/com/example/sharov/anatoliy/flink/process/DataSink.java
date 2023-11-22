package com.example.sharov.anatoliy.flink.process;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.service.Serv;
import com.example.sharov.anatoliy.flink.service.ServImpl;

public class DataSink implements SinkFunction<StoryPojo>{
	private static final Logger LOG = LoggerFactory.getLogger(DataSink.class);
	private static final long serialVersionUID = 4122202811797203992L;
	
	private Serv service;
	
	public DataSink() {
		this.service = new ServImpl();
	}

	@Override
	public void invoke(StoryPojo value, Context context) throws Exception {
		service.load(value);
		SinkFunction.super.invoke(value, context);
	}

}
