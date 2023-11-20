package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.repository.StoryDao;

public class StoryDaoImpl implements StoryDao {
	public static final String SELECT_BY_STORY_ID = "SELECT * FROM stories WHERE id = ?";
	public static final String INSERT_STORY = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";

	public static final String BAD_PARAMETER = "Bad parameter ";

	@Override
	public boolean checkById(Connection connection, String storyId) throws SQLException {

		if( storyId != null && storyId.length() > 0) {
			
			try(PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORY_ID)){
				ps.setString(1, storyId);
				
				try(ResultSet rs = ps.executeQuery()){
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + storyId);
	}

	@Override
	public StoryPojo save(Connection connection, StoryPojo story) throws SQLException {

		if(checkStory(story)) {
			
			try(PreparedStatement ps = connection.prepareStatement(INSERT_STORY)){
				ps.setString(1, story.getId());
				ps.setString(2, story.getTitle());
				ps.setString(3, story.getUrl());
				ps.setString(4, story.getSite());
				ps.setTimestamp(5, story.getTime());
				ps.setString(6, story.getFaviconUrl());
				ps.setString(7, story.getDescription());
				
				if(ps.executeUpdate() != 1) {
					throw new SQLException(BAD_PARAMETER + story);
				}
			}
			return new StoryPojo.Builder()
					.id(story.getId())
					.title(story.getTitle())
					.url(story.getUrl())
					.site(story.getSite())
					.time(story.getTime())
					.faviconUrl(story.getFaviconUrl())
					.description(story.getDescription())
					.build();
		}
		throw new IllegalArgumentException(BAD_PARAMETER + story);
	}

	private boolean checkStory(StoryPojo story) {
		//TODO ADD ANOTHER CHACKING IF IT NEEDS
		return story.getId() != null && story.getId().length() > 0;
	}

}
