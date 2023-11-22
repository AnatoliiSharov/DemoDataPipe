package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;

public interface SimilaryStoryDao {

	public boolean check(Connection connection, String similarStory) throws SQLException;
	
	public boolean check(Connection connection, Long similarStoryId) throws SQLException;
	
	public Optional<SimilarStoryPojo> findFutureId(Connection connection, String similarStory) throws SQLException;
	
	public Optional<SimilarStoryPojo> find(Connection connection, String similarStory) throws SQLException;
	
	public void save(Connection connection, SimilarStoryPojo similarStory) throws SQLException;
	
}
