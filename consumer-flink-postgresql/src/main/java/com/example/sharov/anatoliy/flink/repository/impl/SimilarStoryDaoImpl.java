package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;

public class SimilarStoryDaoImpl implements SimilaryStoryDao {
	public static final String SELECT_BY_SIMILAR_STORY = "SELECT * FROM similar_stories WHERE similar_story = ?";
	public static final String SELECT_BY_ID = "SELECT * FROM similar_stories WHERE id = ?";
	public static final String FETCH_SIMILAR_STORY_ID = "SELECT nextval('similar_stories_id_seq')";
	public static final String INSERT_SIMILAR_STORY = "INSERT INTO similar_stories (id, similar_story) VALUES (?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String similarStory) throws SQLException {

		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_SIMILAR_STORY)) {
				ps.setString(1, similarStory);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public boolean check(Connection connection, Long similarStoryId) throws SQLException {

		if (similarStoryId != null && similarStoryId >= 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_ID)) {
				ps.setLong(1, similarStoryId);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + similarStoryId);
	}

	@Override
	public Optional<SimilarStoryPojo> findFutureId(Connection connection, String similarStory) throws SQLException {

		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(FETCH_SIMILAR_STORY_ID)) {

				try (ResultSet rs = ps.executeQuery()) {

					if (rs.next()) {
						return Optional.of(new SimilarStoryPojo(rs.getLong("nextval"), similarStory));
					}
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public Optional<SimilarStoryPojo> find(Connection connection, String similarStory)
			throws SQLException {

		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_SIMILAR_STORY)) {
				ps.setString(1, similarStory);

				try (ResultSet rs = ps.executeQuery()) {

					if (rs.next()) {
						return Optional.of(new SimilarStoryPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public void save(Connection connection, SimilarStoryPojo similarStory) throws SQLException {

		if (similarStory != null && similarStory.getId() != null && similarStory.getId() > 0
				&& similarStory.getSimilarStory().length() > 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_SIMILAR_STORY)) {
				ps.setLong(1, similarStory.getId());
				ps.setString(2, similarStory.getSimilarStory());

				if (ps.executeUpdate() != 1) {
					throw new SQLException("Unable to save tag" + similarStory);
				}
			}
		} else {
			throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
		}
	}

}
