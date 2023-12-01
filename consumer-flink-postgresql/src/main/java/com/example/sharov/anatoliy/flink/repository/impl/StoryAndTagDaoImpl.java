package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;

public class StoryAndTagDaoImpl implements StoryAndTagDao {
	private static final long serialVersionUID = -4146437248260911151L;
	private static final Logger LOG = LoggerFactory.getLogger(StoryAndTagDaoImpl.class);
	
	public static final String SELECT_BY_STORYID_AND_TAGID = "SELECT * FROM stories_tags WHERE story_id = ? and tag_id = ?";
	public static final String INSERT_STORIES_TAGS = "INSERT INTO stories_tags (story_id, tag_id) VALUES (? , ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String storyId, Long tagId) throws SQLException {
		LOG.debug("StoryAndTagDaoImpl.check have started with storyId = {}, tagId = {}", storyId, tagId);
		if (storyId != null && tagId != null && tagId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORYID_AND_TAGID)) {
				LOG.debug("StoryAndTagDaoImpl.check get PreparedStatement with SELECT_BY_STORYID_AND_TAGID = {}", SELECT_BY_STORYID_AND_TAGID);
				ps.setString(1, storyId);
				ps.setLong(2, tagId);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("StoryAndTagDaoImpl.check finished successfully with storyId = {}, tagId = {}", storyId, tagId);
					return rs.next();
				}
			}
		}
		LOG.debug("StoryAndTagDaoImpl.check did not pass with storyId = {}, tagId = {}", storyId, tagId);
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + tagId);
	}

	@Override
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long tagId) throws SQLException {
		LOG.debug("StoryAndTagDaoImpl.save have started with storyId = {}, tagId = {}", storyId, tagId);
				
		if (storyId != null && tagId != null && tagId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_TAGS,
					Statement.RETURN_GENERATED_KEYS)) {
				LOG.debug("StoryAndTagDaoImpl.save get PreparedStatement  with INSERT_STORIES_TAGS = {}", INSERT_STORIES_TAGS);
				ps.setString(1, storyId);
				ps.setLong(2, tagId);

				if (ps.executeUpdate() != 1) {
					LOG.debug("StoryAndTagDaoImpl.save throw Unable to save with storyId = {}, tagId = {}", storyId, tagId);
					throw new SQLException("Unable to save " + storyId + tagId);
				}
				
				try (ResultSet rs = ps.getGeneratedKeys()) {
					LOG.debug("StoryAndTagDaoImpl.save get ResultSet rs = ps.getGeneratedKeys()");

					if (!rs.next()) {
						LOG.debug("StoryAndTagDaoImpl.save throw  Unable to retrieve id with storyId = {}, tagId = {}", storyId, tagId);
						throw new SQLException("Unable to retrieve id");
					}
					LOG.debug("StoryAndTagDaoImpl.save finished successfully with storyId = {}, tagId = {}", storyId, tagId);
					return new Tuple3<>(rs.getLong(1), storyId, tagId);
				}
			}
		}
		LOG.debug("StoryAndTagDaoImpl.save did not pass null check with storyId = {}, tagId = {}", storyId, tagId);
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + tagId);
	}

}
