package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;

public class StoryAndSimilarStoryDaoImpl implements StoryAndSimilarStoryDao {
	private static final long serialVersionUID = -6734669774713716424L;
	private static final Logger LOG = LoggerFactory.getLogger(StoryAndSimilarStoryDaoImpl.class);
	
	public static final String SELECT_BY_STORYID_AND_TAGID = "SELECT * FROM stories_similar_stories WHERE story_id = ? and similar_story_id = ?";
	public static final String INSERT_STORIES_TAGS = "INSERT INTO stories_similar_stories (story_id, similar_story_id) VALUES (? , ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String storyId, Long similarStoryId) throws SQLException {
		LOG.debug("StoryAndSimilarStoryDaoImpl.check have started with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
		
		if (storyId != null && similarStoryId != null && similarStoryId > 0) {
			
			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORYID_AND_TAGID)) {
				LOG.debug("StoryAndSimilarStoryDaoImpl.check get PreparedStatement with SELECT_BY_STORYID_AND_TAGID = {}", SELECT_BY_STORYID_AND_TAGID);
				ps.setString(1, storyId);
				ps.setLong(2, similarStoryId);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("StoryAndSimilarStoryDaoImpl.check finished successfully with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
					return rs.next();
				}
			}
		}
		LOG.debug("StoryAndSimilarStoryDaoImpl.check did not pass with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + similarStoryId);
	}

	@Override
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long similarStoryId)
			throws SQLException {
		LOG.debug("StoryAndSimilarStoryDaoImpl.save have started with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
		
		if (storyId != null && similarStoryId != null && similarStoryId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_TAGS,
					Statement.RETURN_GENERATED_KEYS)) {
				LOG.debug("StoryAndSimilarStoryDaoImpl.save get PreparedStatement with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
				ps.setString(1, storyId);
				ps.setLong(2, similarStoryId);

				if (ps.executeUpdate() != 1) {
					LOG.debug("StoryAndSimilarStoryDaoImpl.save throw Unable to save with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
					throw new SQLException("Unable to save " + storyId + similarStoryId);
				}
				try (ResultSet rs = ps.getGeneratedKeys()) {
					LOG.debug("StoryAndSimilarStoryDaoImpl.save get ResultSet with storyId = {}, similarStoryId = {}", storyId, similarStoryId);

					if (!rs.next()) {
						LOG.debug("StoryAndSimilarStoryDaoImpl.save throw Unable to retrieve id with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
						throw new SQLException("Unable to retrieve id");
					}
					LOG.debug("StoryAndSimilarStoryDaoImpl.save finished successfully with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
					return new Tuple3<>(rs.getLong(1), storyId, similarStoryId);
				}
			}
		}
		LOG.debug("StoryAndSimilarStoryDaoImpl.save did not pass null check with storyId = {}, similarStoryId = {}", storyId, similarStoryId);
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + similarStoryId);
	}

}
