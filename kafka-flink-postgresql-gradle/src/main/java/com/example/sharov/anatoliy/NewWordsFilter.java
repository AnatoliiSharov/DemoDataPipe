package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class NewWordsFilter implements FilterFunction<Tuple2<String, Integer>> {

	@Override
	public boolean filter(Tuple2<String, Integer> value) throws Exception {
		return value.f1 != 1;
	}

}
