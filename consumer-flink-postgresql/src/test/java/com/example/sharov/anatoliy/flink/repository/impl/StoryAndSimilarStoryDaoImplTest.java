package com.example.sharov.anatoliy.flink.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PSQLException;

import com.example.sharov.anatoliy.flink.conf.DatabaseConnector;
import com.example.sharov.anatoliy.flink.preparationtestenvironment.TestContainers;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;

class StoryAndSimilarStoryDaoImplTest {

	static DatabaseConnector connector;
	static TestContainers testContainers;
	Connection connection;
	StoryAndSimilarStoryDao storyAndSimilarStoryDao;

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
		storyAndSimilarStoryDao = new StoryAndSimilarStoryDaoImpl();
		testContainers.initTestScript("database/init_database.sql");
		connection = connector.getConnection(testContainers.getProperties());
	}

	@ParameterizedTest
	@CsvSource({ "'20', 'AAAAAAAAAAAA', 'true', " + "'1', 'BBBBBBBBBBBB', 'false', " + "'1', 'AAAAAAAAAAAA', 'false'" })
	void testCheck(long checkedSimilarStoryId, String checkedStoryId, boolean expected) throws SQLException {
		assertEquals(expected, storyAndSimilarStoryDao.check(connection, checkedStoryId, checkedSimilarStoryId));
	}

	@ParameterizedTest
	@MethodSource("provideStringsForSaveWithThrowException")
	void testSave_sholdTrowException_whenWrongData(String inputStoryId, Long inputSimilarStoryId,
			Class<? extends Exception> expectedException) throws SQLException {

		assertThrows(expectedException,
				() -> storyAndSimilarStoryDao.save(connection, inputStoryId, inputSimilarStoryId));
	}

	private static Stream<Arguments> provideStringsForSaveWithThrowException() {
		return Stream.of(Arguments.of("AAAAAAAAAAAA", 0L, IllegalArgumentException.class),
				Arguments.of("AAAAAAAAAAAA", -1L, IllegalArgumentException.class),
				Arguments.of("AAAAAAAAAAAA", 9L, PSQLException.class),
				Arguments.of("BBBBBBBBBBBB", 1L, PSQLException.class),
				Arguments.of("BBBBBBBBBBBB", null, IllegalArgumentException.class),
				Arguments.of(null, 1L, IllegalArgumentException.class),
				Arguments.of(null, null, IllegalArgumentException.class));
	}

	@ParameterizedTest
	@MethodSource("provideStringsForSave")
	void testSave(String inputStoryId, Long inputSimilarStoryId, Tuple3<Long, String, Long> expected)
			throws SQLException {

		assertEquals(expected, storyAndSimilarStoryDao.save(connection, inputStoryId, inputSimilarStoryId));
	}

	private static Stream<Arguments> provideStringsForSave() {
		return Stream.of(Arguments.of("AAAAAAAAAAAA", 20L, new Tuple3<Long, String, Long>(2L, "AAAAAAAAAAAA", 20L)));
	}

}
