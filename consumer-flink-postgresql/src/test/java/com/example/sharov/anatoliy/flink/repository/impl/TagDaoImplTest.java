package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
import com.example.sharov.anatoliy.flink.repository.TagDao;

class TagDaoImplTest {

	static DatabaseConnector connector;
	static TestContainers testContainers;
	Connection connection;
	TagDao tagDao;
	
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
		tagDao = new TagDaoImpl();
		testContainers.initTestScript("database/init_database.sql");
		connection = connector.getConnection(testContainers.getProperties());
	}

	@ParameterizedTest
	@CsvSource({"existed_tag, true, NoTag, false"})
	void testCheckByTag(String input, boolean expected) throws SQLException {
		assertEquals(expected, tagDao.checkByTag(connection, input));
	}

	@ParameterizedTest
	@CsvSource({"10, true, 1, false"})
	void testCheckById(long input, boolean expected) throws SQLException {
		assertEquals(expected, tagDao.checkById(connection, input));
	}

	@Test
	void testRetrieveNewTagFutureId() throws SQLException {
		List<TagPojo> dbTags = retrieveAllTags();
		long expectedIndex = dbTags.size();
		
		assertEquals(expectedIndex, tagDao.retrieveNewTagFutureId(connection, "newTag").get());
	}

	@Test
	void testRetrieveByTag() throws SQLException {
		TagPojo expected = new TagPojo();
		expected.setId(10L);
		expected.setTag("existed_tag");
		
		assertEquals(expected, tagDao.retrieveByTag(connection, expected.getTag()).get());
	}

	@Test
	void testSaveTag() throws SQLException {
		TagPojo input = new TagPojo(1L, "newTag");
		List<TagPojo> expectedBefore = retrieveAllTags();
		
		tagDao.saveTag(connection, input);
		List<TagPojo> expectedAfter = retrieveAllTags();
		
		assertFalse(expectedBefore.contains(input));
		assertTrue(expectedAfter.contains(input));
	}

	private List<TagPojo> retrieveAllTags() throws SQLException {
		List<TagPojo> result = new ArrayList<TagPojo>();
		
			try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM tags")){
			
				try(ResultSet rs = ps.executeQuery()){
				
					while(rs.next()) {
						result.add(new TagPojo(rs.getLong(1), rs.getString(2)));
					}
				}
			}
		return result;
	}

}
