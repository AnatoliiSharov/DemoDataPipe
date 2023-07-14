package com.example.pipeline;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ListSource implements SourceFunction<List<String>> {

	private static final List<String> INPUT_LIST = Arrays.asList("Hello", "World", "Apache", "Flink");

	private volatile boolean running = true;

	public void initialize(SourceContext<String> sourceContext) {
	}

	@Override
	public void run(SourceContext<List<String>> sourceContext) throws Exception {
		while (running) {
			sourceContext.collect(INPUT_LIST);
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

}
