package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;

public class StoryAndSimilarStoryDaoImpl implements StoryAndSimilarStoryDao {

	public static final String SELECT_BY_STORYID_AND_TAGID = "SELECT * FROM stories_similar_stories WHERE story_id = ? and similar_story_id = ?";
	public static final String INSERT_STORIES_TAGS = "INSERT INTO stories_similar_stories (story_id, similar_story_id) VALUES (? , ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String storyId, Long similarStoryId) throws SQLException {

		if (storyId != null && similarStoryId != null && similarStoryId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORYID_AND_TAGID)) {
				ps.setString(1, storyId);
				ps.setLong(2, similarStoryId);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + similarStoryId);
	}

	@Override
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long similarStoryId)
			throws SQLException {
		if (storyId != null && similarStoryId != null && similarStoryId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_TAGS,
					Statement.RETURN_GENERATED_KEYS)) {
				ps.setString(1, storyId);
				ps.setLong(2, similarStoryId);

				if (ps.executeUpdate() != 1) {
					throw new SQLException("Unable to save " + storyId + similarStoryId);
				}
				try (ResultSet rs = ps.getGeneratedKeys()) {

					if (!rs.next()) {
						throw new SQLException("Unable to retrieve id");
					}
					return new Tuple3<>(rs.getLong(1), storyId, similarStoryId);
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + similarStoryId);
	}

}
