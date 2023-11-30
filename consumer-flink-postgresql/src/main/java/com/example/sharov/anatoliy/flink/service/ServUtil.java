package com.example.sharov.anatoliy.flink.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.TagDao;
import com.example.sharov.anatoliy.flink.repository.impl.SimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndSimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndTagDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.TagDaoImpl;

public class ServUtil implements Serializable{
	private static final long serialVersionUID = -4587633043735696752L;
	private static final Logger LOG = LoggerFactory.getLogger(ServUtil.class);
	
	private SimilarStoryDao similarStoryDao;
	private TagDao tagDao;
	private StoryAndSimilarStoryDao storyAndSimilarStoryDao;
	private StoryAndTagDao storyAndTagDao;

	public ServUtil() {
		super();
		this.similarStoryDao = new SimilarStoryDaoImpl();
		this.tagDao = new TagDaoImpl();
		this.storyAndSimilarStoryDao = new StoryAndSimilarStoryDaoImpl();
		this.storyAndTagDao = new StoryAndTagDaoImpl();
	}

	public void attachSimilarStory(Connection connection, StoryPojo value) throws SQLException {
		LOG.debug("ServUtil.attachSimilarStory have started with value = {}", value);
		for (SimilarStoryPojo each : value.getSimilarStories()) {

			if (!similarStoryDao.check(connection, each.getSimilarStory())) {
				similarStoryDao.save(connection, each);
				storyAndSimilarStoryDao.save(connection, value.getId(), each.getId());
			} else {
			storyAndSimilarStoryDao.save(connection, value.getId(), each.getId());
			}
		}
	}

	public void attachTags(Connection connection, StoryPojo value) throws SQLException {
		LOG.debug("ServUtil.attachTags have started with value = {}", value);
		
		for (TagPojo each : value.getTags()) {

			if (!tagDao.check(connection, each.getTag())) {
				tagDao.save(connection, each);
				storyAndTagDao.save(connection, value.getId(), each.getId());
			} else {
			storyAndTagDao.save(connection, value.getId(), each.getId());
			}
		}
	}
	
}
