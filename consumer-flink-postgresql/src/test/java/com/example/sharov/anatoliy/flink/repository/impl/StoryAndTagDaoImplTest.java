package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
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
	@CsvSource({"'1', 'AAAAAAAAAAAA', 'true', '1', 'BBBBBBBBBBBB', 'false', '10', 'AAAAAAAAAAAA', 'false'"})
	void testCheck(long checkedTagId, String checkedStoryId, boolean expected) {
		assertEquals(expected, storyAndTagDao.check(connection, checkedStoryId, checkedTagId));
	}

	@ParameterizedTest
	@MethodSource("provideStringsForSave")
	void testSave(String inputStoryId, Long inputTagId, Tuple3<Long, String, Long> expected) {
		
		assertEquals(expected, storyAndTagDao.save(connection, inputStoryId, inputTagId));
	}

	private static Stream<Arguments> provideStringsForSave() {
	    return Stream.of(
	      Arguments.of("AAAAAAAAAAAA", 1L, new Tuple3<Long, String, Long>(1L, "AAAAAAAAAAAA", 1L)),
	      Arguments.of("AAAAAAAAAAAA", 0L, new Tuple3<Long, String, Long>()),
	      Arguments.of("AAAAAAAAAAAA", -1L, new Tuple3<Long, String, Long>()),
	      Arguments.of("AAAAAAAAAAAA", 9L, new Tuple3<Long, String, Long>()),
	      Arguments.of("BBBBBBBBBBBB", 1L, new Tuple3<Long, String, Long>()),
	      Arguments.of("BBBBBBBBBBBB", null, new Tuple3<Long, String, Long>()),
	      Arguments.of(null, 1L, new Tuple3<Long, String, Long>()),
	      Arguments.of(null, null, new Tuple3<Long, String, Long>())
	    		  
	    );
	}
	
}
