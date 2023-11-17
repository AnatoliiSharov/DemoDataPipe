package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;

public interface SimilaryStoryDao {

	public boolean checkBySimilarStory(Connection connection, String similarStory);
	
	public boolean checkById(Connection connection, Long similarStoryId);
	
	public Long retrieveFutureId(Connection connection, String similarStory);
	
	public SimilarStoryPojo retrieveBySimilarStory(Connection connection, String similarStory);
	
	public SimilarStoryPojo save(Connection connection, SimilarStoryPojo similarStory);
	
}
