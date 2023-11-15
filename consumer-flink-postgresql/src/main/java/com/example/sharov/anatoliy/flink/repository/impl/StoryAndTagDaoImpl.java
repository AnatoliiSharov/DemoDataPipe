package com.example.sharov.anatoliy.flink.repository.impl;

import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;

public class StoryAndTagDaoImpl implements StoryAndTagDao{

	@Override
	public boolean check(String storyId, Long tagId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Tuple3<Long, String, Long> save(String storyId, Long tagId) {
		// TODO Auto-generated method stub
		return null;
	}

}
