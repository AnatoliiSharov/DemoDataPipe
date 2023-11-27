package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.TagDao;

public class TagDaoImpl implements TagDao {
	private static final long serialVersionUID = 1469065703343198136L;
	public static final String SELECT_BY_TAG = "SELECT * FROM tags WHERE tag = ?";
	public static final String SELECT_BY_ID = "SELECT * FROM tags WHERE id = ?";
	public static final String FETCH_TAG_ID = "SELECT nextval('tags_id_seq')";
	public static final String INSERT_TAG = "INSERT INTO tags (id, tag) VALUES (?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean check(Connection connection, String tag) throws SQLException {

		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
				ps.setString(1, tag);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public boolean check(Connection connection, Long tagId) throws SQLException {

		if (tagId != null && tagId >= 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_ID)) {
				ps.setLong(1, tagId);

				try (ResultSet rs = ps.executeQuery()) {
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + tagId);
	}

	@Override
	public Optional<TagPojo> findWithFutureId(Connection connection, String tag) throws SQLException {

		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(FETCH_TAG_ID)) {

				try (ResultSet rs = ps.executeQuery()) {

					if (rs.next()) {
						return Optional.of(new TagPojo(rs.getLong("nextval"), tag));
					}
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public Optional<TagPojo> find(Connection connection, String tag) throws SQLException {

		if (tag != null && tag.length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_TAG)) {
				ps.setString(1, tag);

				try (ResultSet rs = ps.executeQuery()) {

					if (rs.next()) {
						return Optional.of(new TagPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + tag);
	}

	@Override
	public void save(Connection connection, TagPojo tag) throws SQLException {

		if (tag != null && !tag.equals(new TagPojo()) && tag.getId() > 0 && tag.getTag().length() != 0) {

			try (PreparedStatement ps = connection.prepareStatement(INSERT_TAG)) {
				ps.setLong(1, tag.getId());
				ps.setString(2, tag.getTag());

				if (ps.executeUpdate() != 1) {
					throw new SQLException("Unable to save " + tag);
				}
			}
		} else {
			throw new IllegalArgumentException(BAD_PARAMETER + tag);
		}
	}
}