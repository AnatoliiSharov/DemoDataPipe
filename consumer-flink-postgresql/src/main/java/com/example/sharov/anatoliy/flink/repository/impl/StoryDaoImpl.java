package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.repository.StoryDao;

public class StoryDaoImpl implements StoryDao {

	@Override
	public boolean checkById(Connection connection, String storyId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public StoryPojo save(Connection connection, StoryPojo story) {
		// TODO Auto-generated method stub
		return null;
	}

}
