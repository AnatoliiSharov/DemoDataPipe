package com.example.sharov.anatoliy.flink.service;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.conf.TransactionUtil;
import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;
import com.example.sharov.anatoliy.flink.repository.TagDao;
import com.example.sharov.anatoliy.flink.repository.impl.SimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndSimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndTagDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.TagDaoImpl;

public class ServImpl implements Serv {
	private static final long serialVersionUID = -6878807514291380091L;
	private static final Logger LOG = LoggerFactory.getLogger(ServImpl.class);

	private TransactionUtil transactionUtil;
	private DatabaseConnector connector;
	private TagDao tagDao;
	private SimilarStoryDao similarStoryDao;
	private StoryDao storyDao;
	private StoryAndTagDao storyAndTagDao;
	private StoryAndSimilarStoryDao storyAndSimilarStoryDao;
	private ServUtil servUtil;

	public ServImpl() {
		super();
		this.transactionUtil = new TransactionUtil();
		this.connector = new DatabaseConnector();
		this.tagDao = new TagDaoImpl();
		this.similarStoryDao = new SimilarStoryDaoImpl();
		this.storyDao = new StoryDaoImpl();
		this.storyAndTagDao = new StoryAndTagDaoImpl();
		this.storyAndSimilarStoryDao = new StoryAndSimilarStoryDaoImpl();
		this.servUtil = new ServUtil();
	}

	@Override
	public boolean checkStoryAlreadyExist(StoryPojo value) throws SQLException {
		LOG.debug("ServImpl.checkStoryAlreadyExist where value = {}", value);
		return storyDao.checkById(connector.getConnection(), value.getId());
	}

	@Override
	public TagPojo fillTagId(String value) throws IllegalStateException, SQLException {
		LOG.debug("ServImpl.fillTagId where value = {}", value);
		return transactionUtil.goReturningTransaction(connector, (connection -> {

			if (tagDao.check(connection, value)) {
				return tagDao.find(connection, value);
			}
			return tagDao.findWithFutureId(connection, value);
		})).orElseThrow(() -> new IllegalStateException("tagDao.find get wrong value with String = " + value));
	}

	@Override
	public SimilarStoryPojo fillSimilarStoryId(String value) throws IllegalStateException, SQLException {
		LOG.debug("ServImpl.fillSimilarStoryId where value = {}", value);
		return transactionUtil.goReturningTransaction(connector, (connection -> {

			if (similarStoryDao.check(connection, value)) {
				return similarStoryDao.find(connection, value);
			}
			return similarStoryDao.findFutureId(connection, value);
		})).orElseThrow(() -> new IllegalStateException("similarStoryDao.find get wrong value with String = " + value));
	}

	@Override
	public void load(StoryPojo value) throws SQLException {
		LOG.debug("ServImpl.load where value = {}", value);
		Connection connection = connector.getConnection();
		
			storyDao.save(connection, value);
			servUtil.attachTags(connection, value);
			servUtil.attachSimilarStory(connection, value);
	}

	public void attachSimilarStory(Connection connection, StoryPojo value) throws SQLException {
		LOG.debug("ServImpl.attachSimilarStory where value = {}", value);
		
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
		LOG.debug("ServImpl.attachTags where value = {}", value);
		
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
