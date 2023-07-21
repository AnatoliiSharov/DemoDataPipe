package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OldWordsFilter implements FilterFunction<Tuple2<String, Integer>> {
	private static final Logger LOG = LoggerFactory.getLogger(NewWordsFilter.class);
	private FilterFunction<Tuple2<String, Integer>> newWorldFilter = new NewWordsFilter();

	@Override
	public boolean filter(Tuple2<String, Integer> value) throws Exception {
		LOG.debug("NewWordsFilter start filter with value = {}", value);
		return !newWorldFilter.filter(value);
	}

}
