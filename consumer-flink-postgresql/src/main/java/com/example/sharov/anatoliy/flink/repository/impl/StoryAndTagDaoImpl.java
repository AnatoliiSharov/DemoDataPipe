package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;

import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;

public class StoryAndTagDaoImpl implements StoryAndTagDao {

	@Override
	public boolean check(Connection connection, String storyId, Long tagId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long tagId) {
		// TODO Auto-generated method stub
		return null;
	}

}
