package com.example.sharov.anatoliy.flink.repository;

import org.apache.flink.api.java.tuple.Tuple3;

public interface StoryAndSimilarStoryDao {

	public boolean check(String storyId, Long similarStoryId);
	
	public Tuple3<Long, String, Long> save(String storyId, Long similarStoryId);
	
}
