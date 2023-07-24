package com.example.sharov.anatoliy;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.PostgreSQLContainer;

class JdbcOperationsTest {
	@SuppressWarnings("rawtypes")
	private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15.3-alpine");
	private JdbcOperations op;

	@BeforeEach
	public void startContainer() {
		op = new JdbcOperations();
		postgres.start();
	
	String sql = "CREATE DATABASE counted_words;"
			+ "CREATE TABLE counted_words("
			+ "word_id SERIAL PRIMARY KEY  NOT NULL, "
			+ "word CHARACTER VARYING(189819) UNIQUE NOT NULL, "
			+ "number INTEGER NOT NULL"
			+ ");"
			+ "INSERT INTO counted_words (word, number) VALUES ('word2', 2);";
	}
	
	@ParameterizedTest
	@CsvSource({"word1, 1", "word2, 3"})
	void test(String word,  int expected) throws SQLException{
		int actual = op.lookForNuberWord(word);

		assertEquals(expected, actual);
	
	}

}
