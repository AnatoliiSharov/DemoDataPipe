package com.example.sharov.anatoliy.flink.repository;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;

public interface StoryDao {

	public boolean checkById(String storyId);
	
	public StoryPojo save(StoryPojo story);
	
}
