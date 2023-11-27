package com.example.sharov.anatoliy.flink.service;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.example.sharov.anatoliy.flink.conf.TransactionUtil;
import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;
import com.example.sharov.anatoliy.flink.repository.TagDao;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ServImplTest {
	StoryPojo input;
	@Mock
	TransactionUtil transaction;
	@Mock
	Connection connection;
	@Mock
	TagDao tagDao;
	@Mock
	SimilaryStoryDao similarStoryDao;
	@Mock
	StoryDao storyDao;
	@Mock
	StoryAndTagDao storyAndTagDao;
	@Mock
	StoryAndSimilarStoryDao storyAndSimilarStoryDao;
	@InjectMocks
	ServImpl servImpl;
	
	@BeforeEach
	void setUp() throws Exception {
		input = new StoryPojo.Builder()
				.id("id")
				.description("test")
				.faviconUrl("test")
				.site("test")
				.title("test")
				.url("test")
				.time(Timestamp.valueOf("2023-11-14 12:08:08.965057"))
				.build();
	}

	@Test
	void testFillIdTagPojo_shouldUseFutureId_whenNegativeChecking() throws SQLException {
		String input = "input";
		boolean check = false;
		TagPojo retrievedTag = new TagPojo(10L, "input");
		
		when(tagDao.check(connection, input)).thenReturn(check);
		when(tagDao.findWithFutureId(connection, input)).thenReturn(Optional.of(retrievedTag));
		when(transaction.goReturningTransaction(any(), any())).thenAnswer(invocation -> {
            TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
            return consumer.concume(connection);
        });
		
		assertEquals(retrievedTag, servImpl.fillTagId( "input"));
		
		verify(tagDao, times(1)).check(any(), anyString());
		verify(tagDao, times(1)).findWithFutureId(any(), anyString());
	}
	
	@Test
	void testFillIdTagPojo_shouldUseDatabaseId_whenPositiveChecking() throws SQLException {
		String input = "input";
		boolean check = true;
		TagPojo retrievedTag = new TagPojo(10L, "input");
		
		when(tagDao.check(connection, input)).thenReturn(check);
		when(tagDao.find(connection, input)).thenReturn(Optional.of(retrievedTag));
		when(transaction.goReturningTransaction(any(), any())).thenAnswer(invocation -> {			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
			return consumer.concume(connection);
		});
		
		assertEquals(retrievedTag, servImpl.fillTagId("input"));
		
		verify(tagDao, times(1)).check(any(), anyString());
		verify(tagDao, times(1)).find(any(), anyString());
	}
	
	@Test
	void testFillIdSimilarStoryPojo_shouldUseFutureId_whenNegativeChecking() throws SQLException {
		String input = "input";
		boolean check = false;
		SimilarStoryPojo retrievedSimilarStory = new SimilarStoryPojo(10L, "input");
		
		when(similarStoryDao.check(connection, input)).thenReturn(check);
		when(similarStoryDao.findFutureId(connection, input)).thenReturn(Optional.of(retrievedSimilarStory));
		when(transaction.goReturningTransaction(any(), any())).thenAnswer(invocation -> {
			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
			return consumer.concume(connection);
		});
		
		assertEquals(retrievedSimilarStory, servImpl.fillSimilarStoryId("input"));
		
		verify(similarStoryDao, times(1)).check(any(), anyString());
		verify(similarStoryDao, times(1)).findFutureId(connection, input);
	}
	
	@Test
	void testFillIdSimilarStoryPojo_shouldUseDatabaseId_whenPositiveChecking() throws SQLException {
		String input = "input";
		boolean check = true;
		SimilarStoryPojo retrievedSimilarStory = new SimilarStoryPojo(10L, "input");
		
		when(similarStoryDao.check(connection, input)).thenReturn(check);
		when(similarStoryDao.find(connection, input)).thenReturn(Optional.of(retrievedSimilarStory));
		when(transaction.goReturningTransaction(any(), any())).thenAnswer(invocation -> {			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
		return consumer.concume(connection);
		});
		
		assertEquals(retrievedSimilarStory, servImpl.fillSimilarStoryId("input"));
		
		verify(similarStoryDao, times(1)).check(any(), anyString());
		verify(similarStoryDao, times(1)).find(any(), anyString());
	}

	@ParameterizedTest
	@CsvSource({"existedTag, true, 0",
		"newTag, false, 1"})
	void attachTags(String tag, boolean existingTagChack, int InvocationOfSaveMethod) throws SQLException {
		input.setTags(Arrays.asList(new TagPojo(1L, tag)));
		input.setSimilarStories(Arrays.asList(new SimilarStoryPojo(1L, "similarStory")));
		
		when(tagDao.check(any(), eq(tag))).thenReturn(existingTagChack);
		doNothing().when(tagDao).save(any(), eq(new TagPojo(1L, tag)));
		when(storyAndTagDao.save(any(), eq("storyId"), eq(1L))).thenReturn(Tuple3.of(1L, "storyId", 1L));
		
		servImpl.attachTags(connection, input);
		
		verify(tagDao, times(1)).check(any(), anyString());
		verify(tagDao, times(InvocationOfSaveMethod)).save(any(), any(TagPojo.class));
		verify(storyAndTagDao, times(1)).save(any(), anyString(), anyLong());
	}
	
	@ParameterizedTest
	@CsvSource({"existedSimilarStory, true, 0",
	"newSimilarStory, false, 1"})
	void attachSimilarStoris(String similarStory, boolean existingSimilarStoryChack, int InvocationOfSaveMethod) throws SQLException {
		input.setTags(Arrays.asList(new TagPojo(1L, "tag")));
		input.setSimilarStories(Arrays.asList(new SimilarStoryPojo(1L, similarStory)));
		
		when(similarStoryDao.check(any(), eq(similarStory))).thenReturn(existingSimilarStoryChack);
		doNothing().when(similarStoryDao).save(any(), eq(new SimilarStoryPojo(1L, similarStory)));
		when(storyAndSimilarStoryDao.save(any(), eq("storyId"), eq(1L))).thenReturn(Tuple3.of(1L, "storyId", 1L));
		
		servImpl.attachSimilarStory(connection, input);
		
		verify(similarStoryDao, times(1)).check(any(), anyString());
		verify(similarStoryDao, times(InvocationOfSaveMethod)).save(any(), any(SimilarStoryPojo.class));
		verify(storyAndSimilarStoryDao, times(1)).save(any(), anyString(), anyLong());
	}
	
	@Test
	void testLoad() throws SQLException {
		StoryPojo inputWithoutTagsAndSimilarStory = mock(StoryPojo.class);
		
		doReturn(inputWithoutTagsAndSimilarStory).when(storyDao).save(any(), any(StoryPojo.class));
		//when(storyDao.save(any(), any(StoryPojo.class))).thenReturn(inputWithoutTagsAndSimilarStory);
		doNothing().when(servImpl).attachTags(any(), any(StoryPojo.class));
		doNothing().when(servImpl).attachSimilarStory(any(), any(StoryPojo.class));
		
		servImpl.load(input);
		
		verify(storyDao, times(1)).save(any(), any(StoryPojo.class));
		verify(servImpl, times(1)).attachTags(any(), any(StoryPojo.class));
		verify(servImpl, times(1)).attachSimilarStory(any(), any(StoryPojo.class));
	}

}
