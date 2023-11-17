package com.example.sharov.anatoliy.flink.sink;

import static com.example.sharov.anatoliy.flink.conf.StoryMessageParser.CONDITION_MARK;
import static com.example.sharov.anatoliy.flink.conf.StoryMessageParser.UNABLE_ID_DATA_EXISTS;
import static com.example.sharov.anatoliy.flink.conf.StoryMessageParser.UNABLE_ID_MISSING_DATA;
import static com.example.sharov.anatoliy.flink.sink.SinkLoader.CONDITION_NEW_TEG_MARK;
import static com.example.sharov.anatoliy.flink.sink.SinkLoader.CONDITION_OLD_TEG_MARK;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
@Testcontainers
class SinkLoaderTest {
	private TestContainers testContainer;
	private Connection connection;
	private SinkLoader sinkLoader;
	private StoryFlink initialData;
	
	@BeforeEach
	public void prepareTestContaner() throws SQLException, InterruptedException{
		testContainer = new TestContainers();
		testContainer.createTestPostgresContainer();
		testContainer.initTestScript("database/init_database.sql");
		
		sinkLoader = new SinkLoader();
		initialData = new StoryFlink();
		
		Tuple3<Long, String, String> tupleTagOne = new Tuple3<>(2L, CONDITION_NEW_TEG_MARK, "tag1");
		Tuple3<Long, String, String> tupleTagTwo = new Tuple3<>(3L, CONDITION_OLD_TEG_MARK, "tag2");
		List<Tuple3<Long, String, String>> tupleTags = Arrays.asList(tupleTagOne, tupleTagTwo);
		
		Tuple3<Long, String, String> tupleSimilarStoryOne = new Tuple3<>(UNABLE_ID_DATA_EXISTS, CONDITION_NEW_TEG_MARK, "similar1");
		Tuple3<Long, String, String> tupleSimilarStoryTwo = new Tuple3<>(20L, CONDITION_OLD_TEG_MARK, "similar2");
		List<Tuple3<Long, String, String>> tupleSimilarStories = Arrays.asList(tupleSimilarStoryOne, tupleSimilarStoryTwo);
		
		initialData.setId("id");
		initialData.setTitle("title");
		initialData.setUrl("url");
		initialData.setSite("site");
		initialData.setTime(new Timestamp(Long.valueOf("11111111")));
		initialData.setFavicon_url("Favicon_Url");
		initialData.setDescription("Description");
		initialData.setTags(tupleTags);
		initialData.setSimilar_stories(tupleSimilarStories);
		

		connection = new DatabaseConnector().getConnection(testContainer.getProperties());
	}
	
	@Test
	void testLoadStory() throws SQLException {
		List<StoryFlink> actual = new ArrayList<>();
		String expected = "[StoryFlink [id=existed_story_id, title=title, url=url, site=site, time=2023-11-14 12:08:08.965057, favicon_url=favicon_url, tags=null, similar_stories=null, description=description], StoryFlink [id=id, title=title, url=url, site=site, time=1970-01-01 06:05:11.111, favicon_url=Favicon_Url, tags=null, similar_stories=null, description=Description]]";
		
		sinkLoader.loadStory(connection, initialData);
	
		try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM STORIES")){
			
			try(ResultSet rs = ps.executeQuery()){
				
				while(rs.next()) {
					StoryFlink result = new StoryFlink();
					result.setId(rs.getString(1));
					result.setTitle(rs.getString(2));
					result.setUrl(rs.getString(3));
					result.setSite(rs.getString(4));
					result.setTime(rs.getTimestamp(5));
					result.setFavicon_url(rs.getString(6));
					result.setDescription(rs.getString(7));
					actual.add(result);
				}
			}
		}
		assertEquals(expected, actual.toString());
	}
	

	@Test
	void testLoadTags() throws SQLException {
		List<Tuple2<Long, String>> actual = new ArrayList<>();
		String expected = "[(10,existed_tag), (2,tag1)]";
		
		sinkLoader.loadTags(connection, initialData);
		
		try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM tags")){
		
			try(ResultSet rs = ps.executeQuery();){
			
				while(rs.next()) {
					actual.add(Tuple2.of(rs.getLong(1), rs.getString(2)));
				}
			}
		assertEquals(expected, actual.toString());	
		}
	}

	@Test
	void testLoadStoryAndTags() throws SQLException {
		List<Tuple2<String, Long>> actual = new ArrayList<>();
		String expected = "";
		
		sinkLoader.loadStoryAndTags(connection, initialData);
	
		try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM stories_tags")){
			
			try(ResultSet rs = ps.executeQuery()){
				
				while(rs.next()) {
					actual.add(Tuple2.of(rs.getString(1), rs.getLong(2)));
				}
			}
		}
		assertEquals(expected, actual.toString());
	}

}
