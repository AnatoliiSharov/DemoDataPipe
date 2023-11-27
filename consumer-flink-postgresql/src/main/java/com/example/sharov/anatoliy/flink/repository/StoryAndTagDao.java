package com.example.sharov.anatoliy.flink.repository;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple3;

public interface StoryAndTagDao extends Serializable{

	public boolean check(Connection connection, String storyId, Long tagId) throws SQLException;
	
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long tagId) throws SQLException;
	
}
