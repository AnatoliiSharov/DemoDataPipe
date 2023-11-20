package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.TagPojo;

public interface TagDao {

	public boolean checkByTag(Connection connection, String tag) throws SQLException;
	
	public boolean checkById(Connection connection, Long tagId) throws SQLException;
	
	public Optional<Long> retrieveNewTagFutureId(Connection connection, String tag) throws SQLException;
	
	public Optional<TagPojo> retrieveByTag(Connection connection, String tag) throws SQLException;
	
	public void saveTag(Connection connection, TagPojo tag) throws SQLException;
	
}
