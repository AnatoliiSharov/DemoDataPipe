package com.example.sharov.anatoliy.flink.repository.parser;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.TagPojo;

public class TagParser implements Parser<TagPojo> {

	@Override
	public TagPojo apply(ResultSet rs) throws SQLException {
		return new TagPojo(rs.getLong(1), rs.getString(2));
	}

}
