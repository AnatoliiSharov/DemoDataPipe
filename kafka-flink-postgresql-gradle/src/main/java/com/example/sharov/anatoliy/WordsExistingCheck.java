package com.example.sharov.anatoliy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordsExistingCheck implements MapFunction<String, Tuple2<String, Integer>>{
	private JdbcOperations jdbcOperations;
	
	public WordsExistingCheck() {
		this.jdbcOperations = new JdbcOperations();
	}

	@Override
	public Tuple2<String, Integer> map(String word) throws Exception {
		return new Tuple2<>(word, jdbcOperations.lookForNuberWord(word));
	}

}


















