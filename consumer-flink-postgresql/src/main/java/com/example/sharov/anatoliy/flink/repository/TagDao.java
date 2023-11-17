package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.TagPojo;

public interface TagDao {

	public boolean checkByTag(Connection connection, String tag);
	
	public boolean checkById(Connection connection, Long tagId);
	
	public Long retrieveNewTagFutureId(Connection connection, String tag);
	
	public TagPojo retrieveByTag(Connection connection, String tag);
	
	public TagPojo saveTag(Connection connection, TagPojo tag);
}
