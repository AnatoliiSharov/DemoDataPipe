
package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
import com.example.sharov.anatoliy.flink.repository.StoryDao;

@Testcontainers
class StoryDaoImplTest {
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
		storyDao = new StoryDaoImpl();
		testContainers.initTestScript("database/init_database.sql");
		connection = connector.getConnection(testContainers.getProperties());
	}

	@ParameterizedTest
	@CsvSource({"AAAAAAAAAAAA,true", "BBBBBBBBBBBB,false"})
	void testCheckById_whenIdEqualsAAAAAAAAAAAA_shouldTrue_when(String storyId, boolean expected) throws SQLException {
		assertEquals(expected, storyDao.checkById(connection, storyId));
	}
	
	@Test
	void testSave() throws SQLException {
		List<StoryPojo> actual = new ArrayList<StoryPojo>();
		StoryPojo story = new StoryPojo.Builder()
				.id("testId")
				.title("testTitle")
				.url("testUrl")
				.site("testSite")
				.time(Timestamp.valueOf("2023-11-14 12:08:08"))
				.faviconUrl("testFaviconUrl")
				.description("testDescription")
				.build();
		storyDao.save(connection, story);
		
		try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM stories")){
			
			try(ResultSet rs = ps.executeQuery()){
				
				while(rs.next()) {
					StoryPojo result = new StoryPojo.Builder()
							.id(rs.getString(1))
							.title(rs.getString(2))
							.url(rs.getString(3))
							.site(rs.getString(4))
							.time(rs.getTimestamp(5))
							.faviconUrl(rs.getString(6))
							.description(rs.getString(7))
							.build();
					actual.add(result);
				}
			}
		}
		assertTrue(actual.contains(story));
	}
	
}
