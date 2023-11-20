package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;


class SimilarStoryDaoImplTest {
	SimilaryStoryDao similarStoryDao;
	static DatabaseConnector connector;
	static TestContainers testContainers;
	Connection connection;
	StoryDao storyDao;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		connector = new DatabaseConnector();
		testContainers = new TestContainers();
		testContainers.createTestPostgresContainer();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		testContainers.stopTestPostgresContainer();
	}

	@BeforeEach
	void setUp() throws Exception {
		similarStoryDao = new SimilarStoryDaoImpl();
		testContainers.initTestScript("database/init_database.sql");
		connection = connector.getConnection(testContainers.getProperties());
	}


	@ParameterizedTest
	@CsvSource({"testTagOne, true, NoTag, false"})
	void testCheckBySimilarStory(String input, boolean expected) throws SQLException {
		assertEquals(expected, similarStoryDao.checkBySimilarStory(connection, input));
	}

	@ParameterizedTest
	@CsvSource({"1, true, 100, false"})
	void testCheckById(long input, boolean expected) throws SQLException {
		assertEquals(expected, similarStoryDao.checkById(connection, input));
	}

	@Test
	void testRetrieveFutureId() throws SQLException {
		List<SimilarStoryPojo> dbTags = retrieveAllSimilarStoryPojo();
		long expectedIndex = dbTags.size();
		
		assertEquals(expectedIndex, similarStoryDao.retrieveFutureId(connection, "newTag"));
	}

	@Test
	void testRetrieveBySimilarStory() throws SQLException {
		SimilarStoryPojo expected = new SimilarStoryPojo();
		
		assertEquals(expected, similarStoryDao.retrieveBySimilarStory(connection, expected.getSimilarStory()));
	}

	@Test
	void testSaveTag() throws SQLException {
		SimilarStoryPojo input = new SimilarStoryPojo(10L, "newTag");
		List<SimilarStoryPojo> expectedBefore = retrieveAllSimilarStoryPojo();
		
		similarStoryDao.save(connection, input);
		List<SimilarStoryPojo> expectedAfter = retrieveAllSimilarStoryPojo();
		
		assertFalse(expectedBefore.contains(input));
		assertTrue(expectedAfter.contains(input));
	}

	private List<SimilarStoryPojo> retrieveAllSimilarStoryPojo() throws SQLException {
		List<SimilarStoryPojo> result = new ArrayList<SimilarStoryPojo>();
		
			try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM tags")){
			
				try(ResultSet rs = ps.executeQuery()){
				
					while(rs.next()) {
						result.add(new SimilarStoryPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		return result;
	}
}
