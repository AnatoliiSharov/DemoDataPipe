package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;

public class SimilarStoryDaoImpl implements SimilaryStoryDao {

	@Override
	public boolean checkBySimilarStory(Connection connection, String similarStory) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean checkById(Connection connection, Long similarStoryId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Long retrieveFutureId(Connection connection, String similarStory) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SimilarStoryPojo retrieveBySimilarStory(Connection connection, String similarStory) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SimilarStoryPojo save(Connection connection, SimilarStoryPojo similarStory) {
		// TODO Auto-generated method stub
		return null;
	}

}
