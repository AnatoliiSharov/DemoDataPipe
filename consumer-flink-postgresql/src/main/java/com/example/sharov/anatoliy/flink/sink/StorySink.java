package com.example.sharov.anatoliy.flink.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;

public class StorySink implements SinkFunction<StoryFlink>{
	private static final Logger LOG = LoggerFactory.getLogger(StorySink.class);
	private static final long serialVersionUID = 4122202811797203992L;
	private ConfParams conf; 
	private SinkLoader sinkUtil;
	
	public StorySink() {
		this.conf = new ConfParams();
		this.sinkUtil = new SinkLoader();
	}

	@Override
	public void invoke(StoryFlink value, Context context) throws Exception {
		
		try(Connection connection = DriverManager.getConnection(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword());){
			connection.setAutoCommit(false);
		
				try{
					sinkUtil.loadStory(connection, value);
					sinkUtil.loadTags(connection, value);
					sinkUtil.loadStoryAndTags(connection, value);
					connection.commit();
				}catch (SQLException e) {
					LOG.debug("StorySink SQLException with story = {}", value);
					connection.rollback();
					throw e;
				} finally {
					connection.setAutoCommit(true);
				}
		}
		//TODO SinkFunction.super.invoke(value, context); - delete after tests
	}

}
