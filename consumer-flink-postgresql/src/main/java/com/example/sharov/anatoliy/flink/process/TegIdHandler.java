package com.example.sharov.anatoliy.flink.process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;

public class TegIdHandler implements MapFunction<StoryFlink, StoryFlink>{
	public static final String FETCH_TAG_ID = "SELECT nextval('tag_id_seq')";
	public static final String SELECT_ID_FROM_TAGS = "SELECT * FROM stories WHERE tag = ?";
	
	private static final long serialVersionUID = -8655749500756635739L;
	private static final Logger LOG = LoggerFactory.getLogger(TegIdHandler.class);
	
	private ConfParams conf = new ConfParams();

	@Override
	public StoryFlink map(StoryFlink story) throws Exception {
		List<Tuple3<Long, String, String>>result = story.getTags();
		
		result.replaceAll(tupleTag -> {
			
			if(tupleTag.f0 != -1) {
				String tag = tupleTag.f1;
				
				try (Connection connect = DriverManager.getConnection(conf.getDatabaseUrl(), conf.getUsername(), conf.getPassword());
						PreparedStatement psCheck = connect.prepareStatement(SELECT_ID_FROM_TAGS);
						PreparedStatement psNewId = connect.prepareStatement(FETCH_TAG_ID)) {
					psCheck.setString(1, tag);
					ResultSet rsCheck = psCheck.executeQuery();

					if (!rsCheck.next()) {
						ResultSet rsNewId = psNewId.executeQuery();

						if (rsNewId.next()) {
							tupleTag = new Tuple3<>(rsNewId.getLong("nextval"), "newTag", tag);
						}
					}
				} catch (SQLException e) {
					LOG.debug("SQLException e = {} for check or insert tag  = {} from story ={} ", e, tag,
							story);
				}
			}
			return tupleTag;
			
		});
		story.setTags(result);
		return story;	
	}

}
