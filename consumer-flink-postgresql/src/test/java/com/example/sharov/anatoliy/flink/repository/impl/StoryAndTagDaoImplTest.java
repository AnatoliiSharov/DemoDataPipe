package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PSQLException;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;

class StoryAndTagDaoImplTest {

	static DatabaseConnector connector;
	static TestContainers testContainers;
	Connection connection;
	StoryAndTagDao storyAndTagDao;
	
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
		storyAndTagDao = new StoryAndTagDaoImpl();
		testContainers.initTestScript("database/init_database.sql");
		connection = connector.getConnection(testContainers.getProperties());
	}


	@ParameterizedTest
	@CsvSource({"'10', 'AAAAAAAAAAAA', 'true', '10', 'BBBBBBBBBBBB', 'false', '10', 'AAAAAAAAAAAA', 'false'"})
	void testCheck(Long checkedTagId, String checkedStoryId, boolean expected) throws SQLException {
		assertEquals(expected, storyAndTagDao.check(connection, checkedStoryId, checkedTagId));
	}

	@ParameterizedTest
	@MethodSource("provideStringsForSaveWithResultThrow")
	void testSave_sholdTrow_whenSomeThingWrong(String inputStoryId, Long inputTagId, Class<? extends Exception> expectedException) throws SQLException {
		
		 assertThrows(expectedException, () -> storyAndTagDao.save(connection, inputStoryId, inputTagId));
	}

	private static Stream<Arguments> provideStringsForSaveWithResultThrow() {
	    return Stream.of(
	      Arguments.of("AAAAAAAAAAAA", 0L, IllegalArgumentException.class),
	      Arguments.of("AAAAAAAAAAAA", -1L, IllegalArgumentException.class),
	      Arguments.of("AAAAAAAAAAAA", 9L, PSQLException.class),
	      Arguments.of("BBBBBBBBBBBB", 1L, PSQLException.class),
	      Arguments.of("BBBBBBBBBBBB", null, IllegalArgumentException.class),
	      Arguments.of(null, 1L, IllegalArgumentException.class),
	      Arguments.of(null, null, IllegalArgumentException.class)
	    		  
	    );
	}
	
	@ParameterizedTest
	@MethodSource("provideStringsForSave")
	void testSave_sholdSuccessfully_whenStoryIdAndTagIdExisting(String inputStoryId, Long inputTagId, Tuple3<Long, String, Long> expected) throws SQLException {
		
		assertEquals(expected, storyAndTagDao.save(connection, inputStoryId, inputTagId));
	}
	
	private static Stream<Arguments> provideStringsForSave() {
		return Stream.of(
				Arguments.of("CCCCCCCCCCCC", 11L, new Tuple3<Long, String, Long>(2L, "CCCCCCCCCCCC", 11L))
				);
	}
	
}
