package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple3;

public interface StoryAndSimilarStoryDao {

	public boolean check(Connection connection, String storyId, Long similarStoryId) throws SQLException;
	
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long similarStoryId) throws SQLException;
	
}
