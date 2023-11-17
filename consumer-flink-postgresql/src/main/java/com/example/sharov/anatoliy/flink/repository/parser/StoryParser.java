package com.example.sharov.anatoliy.flink.repository.parser;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;

public class StoryParser implements Parser<StoryPojo> {

	@Override
	public StoryPojo apply(ResultSet rs) throws SQLException {
		return new StoryPojo.Builder()
				.id(rs.getString(1))
				.title(rs.getString(2))
				.url(rs.getString(3))
				.site(rs.getString(4))
				.time(rs.getTimestamp(5))
				.faviconUrl(rs.getString(6))
				.description(rs.getString(7))
				.build();
	}

}
