package com.example.sharov.anatoliy.flink.service;

import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.conf.TransactionUtil;
import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;
import com.example.sharov.anatoliy.flink.repository.TagDao;
import com.example.sharov.anatoliy.flink.repository.impl.SimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndSimilarStoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryAndTagDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.StoryDaoImpl;
import com.example.sharov.anatoliy.flink.repository.impl.TagDaoImpl;

public class ServImpl implements Serv{
	private static final long serialVersionUID = -6878807514291380091L;
	
	private TransactionUtil transactionUtil;
	private DatabaseConnector connector;
	private TagDao tagDao;
	private SimilaryStoryDao similarStoryDao;
	private StoryDao storyDao;
	private StoryAndTagDao storyAndTagDao;
	private StoryAndSimilarStoryDao storyAndSimilarStoryDao;

	public ServImpl() {
		super();
		this.transactionUtil = new TransactionUtil();
		this.connector = new DatabaseConnector();
		this.tagDao = new TagDaoImpl();
		this.similarStoryDao = new SimilarStoryDaoImpl();
		this.storyDao = new StoryDaoImpl();
		this.storyAndTagDao = new StoryAndTagDaoImpl();
		this.storyAndSimilarStoryDao = new StoryAndSimilarStoryDaoImpl();
	}
	
	@Override
	public boolean checkStoryAlreadyExist(StoryPojo value) throws SQLException {
		return storyDao.checkById(connector.getConnection(), value.getId());
	}



	@Override
	public TagPojo fillId(TagPojo value) throws IllegalStateException, SQLException {
		String tag = value.getTag();

		if (value.getTag() != null && value.getTag().length() >= 0) {
			return transactionUtil.transaction(connector, (connection -> {

				if (tagDao.check(connection, tag)) {
					return tagDao.find(connection, tag);
				}
				return tagDao.findWithFutureId(connection, tag);
			})).orElseThrow(() -> new IllegalStateException("tagDao.find get wrong value with String = " + tag));
		}
		return value;
	}

	@Override
	public SimilarStoryPojo fillId(SimilarStoryPojo value) throws IllegalStateException, SQLException {
		String similarStory = value.getSimilarStory();

		if (value.getSimilarStory() != null && value.getSimilarStory().length() > 0) {
			return transactionUtil.transaction(connector, (connection -> {

				if (similarStoryDao.check(connection, similarStory)) {
					return similarStoryDao.find(connection, similarStory);
				}
				return similarStoryDao.findFutureId(connection, similarStory);
			})).orElseThrow(() -> new IllegalStateException(
					"similarStoryDao.find get wrong value with String = " + similarStory));
		}
		return value;
	}

	@Override
	public StoryPojo load(StoryPojo value) throws IllegalStateException, SQLException {
		return transactionUtil.transaction(connector, (connection -> {
			try {
			StoryPojo result = value;
			
			storyDao.save(connection, value);

			for (TagPojo each : value.getTags()) {

				if (each != null && each.getTag() != null && tagDao.check(connection, each.getTag())) {
					tagDao.save(connection, each);
					storyAndTagDao.save(connection, value.getId(), each.getId());
				}
			}

			for (SimilarStoryPojo each : value.getSimilarStories()) {

				if (each != null && each.getSimilarStory() != null
						&& similarStoryDao.check(connection, each.getSimilarStory())) {
					similarStoryDao.save(connection, each);
					storyAndSimilarStoryDao.save(connection, value.getId(), each.getId());
				}
			}
			return Optional.of(result);
			} catch(SQLException e) {
				e.printStackTrace();
				connection.rollback();
				return Optional.empty();
			}
		})).orElseThrow(() -> new IllegalStateException("tagDao.load get wrong value with StoryPojo = " + value));
	}

}
