package com.example.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FileSink implements SinkFunction<Tuple2<String, Integer>> {

	public FileSink(ParameterTool params) {
		// TODO Auto-generated constructor stub
	}

	
	
}
