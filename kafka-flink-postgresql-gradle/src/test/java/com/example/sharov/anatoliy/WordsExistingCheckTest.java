package com.example.sharov.anatoliy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class WordsExistingCheckTest {
	
	private WordsExistingCheck check;
	@Mock
	private JdbcOperations mockJdbcOp;
	
	@BeforeEach
	void setUp() {
		check = new WordsExistingCheck();
	mockJdbcOp = Mockito.mock(JdbcOperations.class);
	}

	@Test
	void test_shouldReturnOne_whenNewWord() throws Exception {
		String newWord = "newWord";
		Tuple2<String, Integer> expected = new Tuple2("newWord", 1);
		
		when(mockJdbcOp.lookForNuberWord(newWord)).thenReturn(1);
		
		Tuple2<String, Integer> actual = check.map(newWord);
		
		assertEquals(expected, actual);
	}
	
	@Test
	void test_shouldReturnMoreThanOne_whenOldWord() throws Exception {
		String newWord = "newWord";
		Tuple2<String, Integer> expected = new Tuple2("newWord", 4);
		
		when(mockJdbcOp.lookForNuberWord(newWord)).thenReturn(4);
		
		Tuple2<String, Integer> actual = check.map(newWord);
		
		assertEquals(expected, actual);
	}

}
