package com.example.sharov.anatoliy.flink.process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;

public class NewStoriesFilter implements FilterFunction<StoryFlink>{
	public static final String SELECT_ID_FROM_STORIES = "SELECT * FROM stories WHERE id = ?";
	
	private static final long serialVersionUID = 975616519208227157L;
	private static final Logger LOG = LoggerFactory.getLogger(NewStoriesFilter.class);
	
	private ConfParams conf;
	//TODO fter tests apply DatabaseConnector with RichFilter(open and close methods)
	
	public NewStoriesFilter() {
		this.conf = new ConfParams();
	}

	@Override
	public boolean filter(StoryFlink value) throws Exception {
		Boolean result = true;

		try (Connection connection = DriverManager.getConnection(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword());
				PreparedStatement psCheck = connection.prepareStatement(SELECT_ID_FROM_STORIES);) {
			psCheck.setString(1, value.getId());
			ResultSet resultSetLook = psCheck.executeQuery();

			if (resultSetLook.next()) {
				result = false;
			}
		} catch (SQLException e) {
			LOG.debug("for find by id = {} from stories threw SQLException e = {}", value.getId(), e);
		}
		return result;
	}
	

}
