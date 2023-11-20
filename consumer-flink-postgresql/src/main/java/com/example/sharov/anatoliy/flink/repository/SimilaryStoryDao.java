package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;

public interface SimilaryStoryDao {

	public boolean checkBySimilarStory(Connection connection, String similarStory) throws SQLException;
	
	public boolean checkById(Connection connection, Long similarStoryId) throws SQLException;
	
	public Optional<Long> retrieveFutureId(Connection connection, String similarStory) throws SQLException;
	
	public Optional<SimilarStoryPojo> retrieveBySimilarStory(Connection connection, String similarStory) throws SQLException;
	
	public void save(Connection connection, SimilarStoryPojo similarStory) throws SQLException;
	
}
