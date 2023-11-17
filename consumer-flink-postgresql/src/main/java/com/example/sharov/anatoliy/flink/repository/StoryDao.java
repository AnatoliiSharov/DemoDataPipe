package com.example.sharov.anatoliy.flink.repository;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;

public interface StoryDao {

	public boolean checkById(Connection connection, String storyId);
	
	public StoryPojo save(Connection connection, StoryPojo story);
	
}
