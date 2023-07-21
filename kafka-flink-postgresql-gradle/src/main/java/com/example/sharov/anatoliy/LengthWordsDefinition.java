package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class LengthWordsDefinition  implements MapFunction<String, Tuple2<String, Integer>>{

	@Override
	public Tuple2<String, Integer> map(String word) throws Exception {
		return new Tuple2<String, Integer>(word, word.length());
	}

}
