package com.example.sharov.anatoliy.flink.repository.parser;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;

public class SimilarStoryParser implements Parser<SimilarStoryPojo> {

	@Override
	public SimilarStoryPojo apply(ResultSet rs) throws SQLException {
		return new SimilarStoryPojo(rs.getLong(1), rs.getString(2));
	}

}
