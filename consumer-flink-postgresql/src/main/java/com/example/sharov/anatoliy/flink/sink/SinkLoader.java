package com.example.sharov.anatoliy.flink.sink;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.DataStreamJob;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;

public class SinkLoader implements Serializable{
	private static final long serialVersionUID = 8775842477094754328L;
	private static final Logger LOG = LoggerFactory.getLogger(SinkLoader.class);
	
	public static final String CONDITION_NEW_TEG_MARK = "newTag";//TODO CHANCGE TO ENAM
	public static final String CONDITION_OLD_TEG_MARK = "newTag";//TODO CHANCGE TO ENAM
	
	public static final String INSERT_STORIES = "INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String INSERT_TAGS = "INSERT INTO tags (tag) VALUES (?) RETURNING id";
	public static final String INSERT_STORIES_AND_TAGS = "INSERT INTO stories_tags (story_id, tag_id) VALUES (?, ?)";
	public static final String INSERT_SIMILAR_STORIES = "INSERT INTO similar_stories (id, similar_story) VALUES (?, ?)";
	public static final String INSERT_STORIES_AND_SIMILAR_STORIES = "INSERT INTO stories_similar_stories (id, story_id, similar_story_id) VALUES (?, ?, ?)";

	

	public void loadStory(Connection connection, StoryFlink story) throws SQLException {
		
		try(PreparedStatement ps = connection.prepareStatement(INSERT_STORIES)){
			String[] row = mapToRowForStoryTable(story);

			for(int i = 0; i <= row.length; i++) {
				ps.setString(i+1, row[i]);
			}
		} catch (SQLException e) {
			LOG.debug("SQLException for running SinkUtil.loadStory with story = {}", story);
			e.printStackTrace();
		}
	}
	
	public String[] mapToRowForStoryTable(StoryFlink story) {
		return new String[] {
				story.getId(),
				story.getTitle(),
				story.getUrl(),
				story.getSite(),
				story.getTime().toString(),
				story.getFavicon_url(),
				story.getDescription(),
		};
	}

	public void loadTags(Connection connection, StoryFlink story)  throws SQLException {
		story.getTags().forEach(tupleTag -> {
			
			if(tupleTag.f1.equals(CONDITION_NEW_TEG_MARK)) {
				
				try(PreparedStatement ps = connection.prepareStatement(INSERT_TAGS)){
					ps.setLong(1, tupleTag.f0);
					ps.setString(1, tupleTag.f1);
					ps.executeUpdate();
				} catch (SQLException e) {
					LOG.debug("SQLException for running SinkUtil.loadTags with story = {} and tag = {}", story, tupleTag);
					e.printStackTrace();
				}	
			}
		});
	}

	public void loadStoryAndTags(Connection connection, StoryFlink story)  throws SQLException {
		story.getTags().forEach(tupleTag -> {
			
			if(tupleTag.f0 != -1L) {
			
				try(PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_AND_TAGS)){
					ps.setString(2, story.getId());
					ps.setLong(3, tupleTag.f0);
				} catch (SQLException e) {
					LOG.debug("SQLException for running SinkUtil.loadTags with story = {} and tag = {}", story, tupleTag);
					e.printStackTrace();
				}
			}
		});
	}

}
