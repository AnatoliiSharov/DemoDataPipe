package com.example.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				
        List<String> inputList = Arrays.asList("Hello", "World", "Apache", "Flink");
        DataStream<String> inputStream = env.fromCollection(inputList);

        DataStream<Tuple2<String, Integer>> outputStream = inputStream
                .flatMap(new WordCountFlatMap());

        outputStream.print();

        env.execute("Flink Job");
    }

    public static class WordCountFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.toLowerCase().split("\\s+");

            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

}
