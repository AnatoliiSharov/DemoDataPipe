package com.example.sharov.anatoliy.flink.sink;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.StoryFlink;

public class SinkLoader implements Serializable{
	private static final long serialVersionUID = 8775842477094754328L;
	private static final Logger LOG = LoggerFactory.getLogger(SinkLoader.class);
	
	public static final String CONDITION_NEW_TEG_MARK = "newTag";//TODO CHANCGE TO ENAM
	public static final String CONDITION_OLD_TEG_MARK = "oldTag";//TODO CHANCGE TO ENAM
	
	public static final String INSERT_STORIES = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String INSERT_TAGS = "INSERT INTO tags ( id, tag) VALUES ( ?, ?)";
	public static final String INSERT_STORIES_AND_TAGS = "INSERT INTO stories_tags (story_id, tag_id) VALUES (?, ?)";
	public static final String INSERT_SIMILAR_STORIES = "INSERT INTO similar_stories (id, similar_story) VALUES (?, ?)";
	public static final String INSERT_STORIES_AND_SIMILAR_STORIES = "INSERT INTO stories_similar_stories (id, story_id, similar_story_id) VALUES (?, ?, ?)";

	
	public void loadStory(Connection connection, StoryFlink story) throws SQLException {
		
		try(PreparedStatement ps = connection.prepareStatement(INSERT_STORIES)){
			ps.setString( 1, story.getId());			
			ps.setString( 2, story.getTitle());			
			ps.setString( 3, story.getUrl());			
			ps.setString( 4, story.getSite());			
			ps.setTimestamp( 5, story.getTime());			
			ps.setString( 6, story.getFavicon_url());			
			ps.setString( 7, story.getDescription());			
			ps.executeUpdate();
		} catch (SQLException e) {
			LOG.debug("SQLException for running SinkUtil.loadStory with story = {}", story);
			e.printStackTrace();
		}
	}

	public void loadTags(Connection connection, StoryFlink story) {
		
		if(!story.getTags().isEmpty()) {
		story.getTags().forEach(tupleTag -> {
			
			if(tupleTag.f1.equals(CONDITION_NEW_TEG_MARK)) {
				
				try(PreparedStatement ps = connection.prepareStatement(INSERT_TAGS)){
					ps.setLong(1, tupleTag.f0);
					ps.setString(2, tupleTag.f2);
					ps.executeUpdate();
				} catch (SQLException e) {
					LOG.debug("SQLException for running SinkUtil.loadTags with story = {} and tag = {}", story, tupleTag);
					e.printStackTrace();
				}	
			}
		});
		}
	}

	public void loadStoryAndTags(Connection connection, StoryFlink story) {
		
		if(!story.getTags().isEmpty()) {
		story.getTags().forEach(tupleTag -> {
			
			if(tupleTag.f0 != -1L) {
			
				try(PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_AND_TAGS)){
					ps.setString(1, story.getId());
					ps.setLong(2, tupleTag.f0);
				} catch (SQLException e) {
					LOG.debug("SQLException for running SinkUtil.loadTags with story = {} and tag = {}", story, tupleTag);
					e.printStackTrace();
				}
			}
		});
		}
	}

}
