package com.example.sharov.anatoliy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.SELECT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.COLOMN_OF_RESULT;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordsExistingCheck implements MapFunction<String, Tuple2<String, Integer>>{

	@Override
	public Tuple2<String, Integer> map(String word) throws Exception {
		int count = 0;
		try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
				PreparedStatement ps = connect.prepareStatement(SELECT_SQL_QUERY);) {
			ps.setString(1, word);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				count = resultSet.getInt(COLOMN_OF_RESULT);
			}
		}
		return new Tuple2<>(word, count + 1);
	}

}


















