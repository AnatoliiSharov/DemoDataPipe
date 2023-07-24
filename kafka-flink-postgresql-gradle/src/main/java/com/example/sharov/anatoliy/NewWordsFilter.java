package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewWordsFilter implements FilterFunction<CountedWordPojo> {
	private static final Logger LOG = LoggerFactory.getLogger(NewWordsFilter.class);

	@Override
	public boolean filter(CountedWordPojo countedWord) throws Exception {
		LOG.debug("NewWordsFilter start filter with countedWord = {}", countedWord);
		return countedWord.getNumber() == 1;
	}

}
