package com.example.sharov.anatoliy;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class NewWordsFilterTest {

	@Test
	void test()  throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> testData = env.fromElements(
                new Tuple2<>("word1", 1),
                new Tuple2<>("word2", 3),
                new Tuple2<>("word3", 1),
                new Tuple2<>("word1", 2)
        );

        DataStream<Tuple2<String, Integer>> filteredStream = testData.filter(new NewWordsFilter());

        List<Tuple2<String, Integer>> result = filteredStream.executeAndCollect(4);

        List<Tuple2<String, Integer>> expected = Arrays.asList(
                new Tuple2<>("word1", 1),
                new Tuple2<>("word3", 1)
        );

        assertEquals(expected, result);
    }

}
