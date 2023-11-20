package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;

public class StoryAndTagDaoImpl implements StoryAndTagDao {
public static final String SELECT_BY_STORYID_AND_TAGID = "SELECT * FROM stories_tags WHERE story_id = ? and tag_id = ?";
public static final String INSERT_STORIES_TAGS = "INSERT INTO stories_tags (story_id, tag_id) where (? , ?)";

public static final String BAD_PARAMETER = "Bad parameter ";
	@Override
	public boolean check(Connection connection, String storyId, Long tagId) throws SQLException {
		
		if(storyId != null && tagId != null && tagId > 0) {
			
			try(PreparedStatement ps = connection.prepareStatement(SELECT_BY_STORYID_AND_TAGID)){
				
				try(ResultSet rs = ps.executeQuery()){
					return rs.next();
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + tagId);
	}

	@Override
	public Tuple3<Long, String, Long> save(Connection connection, String storyId, Long tagId) throws SQLException {

		if(storyId != null && tagId != null && tagId > 0) {
			
			try(PreparedStatement ps = connection.prepareStatement(INSERT_STORIES_TAGS)){
				ps.setString( 1, storyId);
				ps.setLong(2, tagId);
				
				if(ps.executeUpdate() != 1) {
					throw new SQLException("Unable to save " + storyId + tagId);
				}
			}
		}
		throw new IllegalArgumentException(BAD_PARAMETER + storyId + tagId);
	}

}
