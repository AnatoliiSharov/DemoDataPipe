package com.example.sharov.anatoliy.flink.repository;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;

public interface StoryDao extends Serializable{

	public boolean checkById(Connection connection, String storyId) throws SQLException;
	
	public StoryPojo save(Connection connection, StoryPojo story) throws SQLException;
	
}
