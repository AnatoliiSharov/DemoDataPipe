package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class OldWordsFilter implements FilterFunction<Tuple2<String, Integer>> {
		private FilterFunction<Tuple2<String, Integer>> newWorldFilter = new NewWordsFilter();

		@Override
		public boolean filter(Tuple2<String, Integer> value) throws Exception {
			return !newWorldFilter.filter(value);
		}

}
