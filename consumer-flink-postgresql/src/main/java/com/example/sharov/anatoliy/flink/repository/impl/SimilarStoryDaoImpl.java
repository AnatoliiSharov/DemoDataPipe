package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;

public class SimilarStoryDaoImpl implements SimilaryStoryDao {
	public static final String SELECT_BY_TAG = "SELECT * FROM similar_stories WHERE similar_story = ?";
	public static final String SELECT_BY_ID = "SELECT * FROM similar_stories WHERE id = ?";
	public static final String FETCH_TAG_ID = "SELECT nextval('tag_id_seq')";
	public static final String INSERT_TAG = "INSERT INTO similar_stories (id, tag) VALUES (?, ?)";
	
	public static final String BAD_PARAMETER = "Bad parameter ";
	
	@Override
	public boolean checkBySimilarStory(Connection connection, String similarStory) throws SQLException {
		
		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
				ps.setString(1, similarStory);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public boolean checkById(Connection connection, Long similarStoryId) throws SQLException {
		
		if (similarStoryId != null && similarStoryId > 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_ID)) {
				ps.setLong(1, similarStoryId);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		return false;
	}

	@Override
	public Optional<Long> retrieveFutureId(Connection connection, String similarStory) throws SQLException {

		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(FETCH_TAG_ID)) {

				try (ResultSet rs = ps.executeQuery()) {

					if (rs.next()) {
						return Optional.of(rs.getLong("nextval"));

					}
				}
			}
		}
		return Optional.empty();
	}

	@Override
	public Optional<SimilarStoryPojo> retrieveBySimilarStory(Connection connection, String similarStory) throws SQLException {

		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
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

		if (similarStory != null && !similarStory.equals(new TagPojo()) && similarStory.getId() > 0 && similarStory.getSimilarStory().length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_TAG)) {
				ps.setLong(1, similarStory.getId());
				ps.setString(2, similarStory.getSimilarStory());

				if (ps.executeUpdate() != 1) {
					throw new SQLException("Unable to save tag" + similarStory);
				}
			}
			throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
		}
	}

}
