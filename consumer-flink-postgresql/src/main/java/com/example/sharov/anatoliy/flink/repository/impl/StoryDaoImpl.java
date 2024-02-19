package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.repository.StoryDao;

public class StoryDaoImpl implements StoryDao {
	private static final long serialVersionUID = -580639298384717409L;
	private static final Logger LOG = LoggerFactory.getLogger(StoryDaoImpl.class);

	public static final String SELECT_BY_STORY_ID = "SELECT * FROM stories WHERE id = ?";
	public static final String INSERT_STORY = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean checkById(Connection connection, String storyId) throws SQLException {
		LOG.debug("StoryDaoImpl.checkById started with storyId = {}", storyId);

		try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORY_ID)) {
			LOG.debug("StoryDaoImpl.checkById get PreparedStatement with SELECT_BY_STORY_ID = {}", SELECT_BY_STORY_ID);
			ps.setString(1, storyId);

			try (ResultSet rs = ps.executeQuery()) {
				LOG.debug("StoryDaoImpl.checkById finished successfully with storyId = {}", storyId);
				return rs.next();
			}
		}
	}

	@Override
	public StoryPojo save(Connection connection, StoryPojo story) throws SQLException {
		LOG.debug("StoryDaoImpl.save started with story = {}", story);
		
		try (PreparedStatement ps = connection.prepareStatement(INSERT_STORY)) {
			LOG.debug("StoryDaoImpl.save started with story = {}", story);
			ps.setString(1, story.getId());
			ps.setString(2, story.getTitle());
			ps.setString(3, story.getUrl());
			ps.setString(4, story.getSite());
			ps.setTimestamp(5, story.getTime());
			ps.setString(6, story.getFaviconUrl());
			ps.setString(7, story.getDescription());

			if (ps.executeUpdate() != 1) {
				LOG.debug("StoryDaoImpl.save fell on checking ps.executeUpdate() != 1");
				throw new SQLException(BAD_PARAMETER + story);
			}
			LOG.debug("StoryDaoImpl.save save successfully story = {}", story);
		}
		return new StoryPojo.Builder().id(story.getId()).title(story.getTitle()).url(story.getUrl())
				.site(story.getSite()).time(story.getTime()).faviconUrl(story.getFaviconUrl())
				.description(story.getDescription()).build();
	}

}
