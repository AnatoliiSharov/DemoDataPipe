package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.repository.SimilarStoryDao;

public class SimilarStoryDaoImpl implements SimilarStoryDao {
	private static final long serialVersionUID = 6867150666574232817L;
	private static final Logger LOG = LoggerFactory.getLogger(SimilarStoryDaoImpl.class);
	
	public static final String SELECT_BY_SIMILAR_STORY = "SELECT * FROM similar_stories WHERE similar_story = ?";
	public static final String SELECT_BY_ID = "SELECT * FROM similar_stories WHERE id = ?";
	public static final String FETCH_SIMILAR_STORY_ID = "SELECT nextval('similar_stories_id_seq')";
	public static final String INSERT_SIMILAR_STORY = "INSERT INTO similar_stories (id, similar_story) VALUES (?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";
	@Override
	public boolean check(Connection connection, String similarStory) throws SQLException {
		LOG.debug("SimilarStoryDaoImpl.check have started with similarStory = {}", similarStory);
		
		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_SIMILAR_STORY)) {
				LOG.debug("SimilarStoryDaoImpl.check get PreparedStatement with SELECT_BY_SIMILAR_STORY = {}", SELECT_BY_SIMILAR_STORY);
				ps.setString(1, similarStory);
				
				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("SimilarStoryDaoImpl.check get ResultSet with similarStory = {}", similarStory);
					return rs.next();
				}
			}
		}
		LOG.debug("SimilarStoryDaoImpl.check not pass null checking with similarStory = {}", similarStory);
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public boolean check(Connection connection, Long similarStoryId) throws SQLException {
		LOG.debug("SimilarStoryDaoImpl.check have started with similarStoryId = {}", similarStoryId);
		
		if (similarStoryId != null && similarStoryId >= 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_ID)) {
				LOG.debug("SimilarStoryDaoImpl.check get PreparedStatement with SELECT_BY_ID = {}", SELECT_BY_ID);
				ps.setLong(1, similarStoryId);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("SimilarStoryDaoImpl.check get ResultSet with similarStoryId = {}", similarStoryId);
					return rs.next();
				}
			}
		}
		LOG.debug("SimilarStoryDaoImpl.check did not pass null checking with similarStoryId = {}", similarStoryId);
		throw new IllegalArgumentException(BAD_PARAMETER + similarStoryId);
	}

	@Override
	public Optional<SimilarStoryPojo> findFutureId(Connection connection, String similarStory) throws SQLException {
		LOG.debug("SimilarStoryDaoImpl.findFutureId have started with similarStory = {}", similarStory);
		
		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(FETCH_SIMILAR_STORY_ID)) {
				LOG.debug("SimilarStoryDaoImpl.findFutureId get PreparedStatement with similarStory = {}", similarStory);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("SimilarStoryDaoImpl.findFutureId get ResultSet with similarStory = {}", similarStory);

					if (rs.next()) {
						LOG.debug("SimilarStoryDaoImpl.findFutureId finished success with similarStory = {}", similarStory);
						return Optional.of(new SimilarStoryPojo(rs.getLong("nextval"), similarStory));
					}
				}
			}
		}
		LOG.debug("SimilarStoryDaoImpl.findFutureId did not pass null checking with similarStory = {}", similarStory);
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public Optional<SimilarStoryPojo> find(Connection connection, String similarStory)
			throws SQLException {
		LOG.debug("SimilarStoryDaoImpl.find have started with similarStory = {}", similarStory);
		
		if (similarStory != null && similarStory.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_SIMILAR_STORY)) {
				LOG.debug("SimilarStoryDaoImpl.find get PreparedStatement with SELECT_BY_SIMILAR_STORY = {}", SELECT_BY_SIMILAR_STORY);
				ps.setString(1, similarStory);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("SimilarStoryDaoImpl.find get ResultSet with similarStory = {}", similarStory);

					if (rs.next()) {
						LOG.debug("SimilarStoryDaoImpl.find finished success with similarStory = {}", similarStory);
						return Optional.of(new SimilarStoryPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		}
		LOG.debug("SimilarStoryDaoImpl.find did not passe null checking with similarStory = {}", similarStory);
		throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
	}

	@Override
	public void save(Connection connection, SimilarStoryPojo similarStory) throws SQLException {
		LOG.debug("SimilarStoryDaoImpl.save have started with similarStory = {}", similarStory);
		
		if (similarStory != null && similarStory.getId() != null && similarStory.getId() > 0
				&& similarStory.getSimilarStory().length() > 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_SIMILAR_STORY)) {
				LOG.debug("SimilarStoryDaoImpl.save get PreparedStatement with INSERT_SIMILAR_STORY = {}", INSERT_SIMILAR_STORY);
				ps.setLong(1, similarStory.getId());
				ps.setString(2, similarStory.getSimilarStory());

				if (ps.executeUpdate() != 1) {
					LOG.debug("SimilarStoryDaoImpl.save did not pass executeUpdate with similarStory = {}", similarStory);
					throw new SQLException("Unable to save tag" + similarStory);
				}
				LOG.debug("SimilarStoryDaoImpl.save finished success with similarStory = {}", similarStory);
			}
		} else {
			LOG.debug("SimilarStoryDaoImpl.save similarStory = {} did not pass null checking", similarStory);
			throw new IllegalArgumentException(BAD_PARAMETER + similarStory);
		}
	}

}
