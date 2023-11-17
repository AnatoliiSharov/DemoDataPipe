package com.example.sharov.anatoliy.flink.repository.parser;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple3;

public class TupleParser implements Parser<Tuple3<Long, String, Long>> {

	@Override
	public Tuple3<Long, String, Long> apply(ResultSet rs) throws SQLException {
		return new Tuple3<>(rs.getLong(1), rs.getString(2), rs.getLong(3));
	}

}
