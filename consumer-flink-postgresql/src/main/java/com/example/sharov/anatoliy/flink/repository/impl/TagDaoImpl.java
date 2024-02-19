package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.TagDao;

public class TagDaoImpl implements TagDao {
	private static final long serialVersionUID = 1469065703343198136L;
	private static final Logger LOG = LoggerFactory.getLogger(TagDaoImpl.class);
	
	public static final String SELECT_BY_TAG = "SELECT * FROM tags WHERE tag = ?";
	public static final String SELECT_BY_ID = "SELECT * FROM tags WHERE id = ?";
	public static final String FETCH_TAG_ID = "SELECT nextval('tags_id_seq')";
	public static final String INSERT_TAG = "INSERT INTO tags (id, tag) VALUES (?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String tag) throws SQLException {
		LOG.debug("TagDaoImpl.check have started with tag = {}", tag);
		
		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
				LOG.debug("TagDaoImpl.check get PreparedStatement with SELECT_BY_TAG = {}", SELECT_BY_TAG);
				ps.setString(1, tag);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("TagDaoImpl.check finished successfully with tag = {}", tag);
					return rs.next();
				}
			}
		}
		LOG.debug("TagDaoImpl.check did not pass null check with tag = {}", tag);
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public boolean check(Connection connection, Long tagId) throws SQLException {
		LOG.debug("TagDaoImpl.check have started with tagId = {}", tagId);

		if (tagId != null && tagId >= 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_ID)) {
				LOG.debug("TagDaoImpl.check get PreparedStatement with SELECT_BY_ID = {}", SELECT_BY_ID);
				ps.setLong(1, tagId);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("TagDaoImpl.check finish successfully with tagId = {}", tagId);
					return rs.next();
				}
			}
		}
		LOG.debug("TagDaoImpl.check did not pass with tagId = {}", tagId);
		throw new IllegalArgumentException(BAD_PARAMETER + tagId);
	}

	@Override
	public Optional<TagPojo> findWithFutureId(Connection connection, String tag) throws SQLException {
		LOG.debug("TagDaoImpl.findWithFutureId have started with tag = {}", tag);
		
		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(FETCH_TAG_ID)) {
				LOG.debug("TagDaoImpl.findWithFutureId get PreparedStatement with FETCH_TAG_ID = {}", FETCH_TAG_ID);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("TagDaoImpl.findWithFutureId get ResultSet");

					if (rs.next()) {
						LOG.debug("TagDaoImpl.findWithFutureId finished successfully with tag = {}", tag);
						return Optional.of(new TagPojo(rs.getLong("nextval"), tag));
					}
				}
			}
		}
		LOG.debug("TagDaoImpl.findWithFutureId did not pass null check with tag = {}", tag);
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public Optional<TagPojo> find(Connection connection, String tag) throws SQLException {
		LOG.debug("TagDaoImpl.find have started with tag = {}", tag);

		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
				LOG.debug("TagDaoImpl.find get PreparedStatement with SELECT_BY_TAG = {}", SELECT_BY_TAG);
				ps.setString(1, tag);

				try (ResultSet rs = ps.executeQuery()) {
					LOG.debug("TagDaoImpl.find get ResultSet");

					if (rs.next()) {
						LOG.debug("TagDaoImpl.find finished successfully with tag = {}", tag);
						return Optional.of(new TagPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		}
		LOG.debug("TagDaoImpl.find did not pass null check with tag = {}", tag);
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public void save(Connection connection, TagPojo tag) throws SQLException {
		LOG.debug("TagDaoImpl.save have started with tag = {}", tag);

		if (tag != null && !tag.equals(new TagPojo()) && tag.getId() > 0 && tag.getTag().length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_TAG)) {
				LOG.debug("TagDaoImpl.save get PreparedStatement with tag = {}", tag);
				ps.setLong(1, tag.getId());
				ps.setString(2, tag.getTag());

				if (ps.executeUpdate() != 1) {
					LOG.debug("TagDaoImpl.save have ps.executeUpdate() != 1");
					throw new SQLException("Unable to save " + tag);
				}
				LOG.debug("TagDaoImpl.save finished successfully with tag = {}", tag);
			}
		} else {
			LOG.debug("TagDaoImpl.save did not pass null check with tag = {}", tag);
			throw new IllegalArgumentException(BAD_PARAMETER + tag);
		}
	}

}