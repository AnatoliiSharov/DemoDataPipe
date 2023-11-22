package com.example.sharov.anatoliy.flink.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.sharov.anatoliy.flink.conf.TransactionUtil;
import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.SimilaryStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndSimilarStoryDao;
import com.example.sharov.anatoliy.flink.repository.StoryAndTagDao;
import com.example.sharov.anatoliy.flink.repository.StoryDao;
import com.example.sharov.anatoliy.flink.repository.TagDao;
import com.ibm.icu.impl.UResource.Array;

@ExtendWith(MockitoExtension.class)
class ServImplTest {
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
	}

	@Test
	void testFillIdTagPojo_shouldUseFutureId_whenNegativeChecking() throws SQLException {
		String input = "input";
		boolean check = false;
		TagPojo retrievedTag = new TagPojo(10L, "input");
		
		when(tagDao.check(connection, input)).thenReturn(check);
		when(tagDao.findWithFutureId(connection, input)).thenReturn(Optional.of(retrievedTag));
		when(transaction.transaction(any(), any())).thenAnswer(invocation -> {
            TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
            return consumer.concume(connection);
        });
		
		assertEquals(retrievedTag, servImpl.fillId(new TagPojo(0L, "input")));
		
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
		when(transaction.transaction(any(), any())).thenAnswer(invocation -> {			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
			return consumer.concume(connection);
		});
		
		assertEquals(retrievedTag, servImpl.fillId(new TagPojo(0L, "input")));
		
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
		when(transaction.transaction(any(), any())).thenAnswer(invocation -> {
			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
			return consumer.concume(connection);
		});
		
		assertEquals(retrievedSimilarStory, servImpl.fillId(new SimilarStoryPojo(0L, "input")));
		
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
		when(transaction.transaction(any(), any())).thenAnswer(invocation -> {			TransactionUtil.ConnectionConcumer<?> consumer = invocation.getArgument(1);
		return consumer.concume(connection);
		});
		
		assertEquals(retrievedSimilarStory, servImpl.fillId(new SimilarStoryPojo(0L, "input")));
		
		verify(similarStoryDao, times(1)).check(any(), anyString());
		verify(similarStoryDao, times(1)).find(any(), anyString());
	}

	@SuppressWarnings("unchecked")
	@Test
	void testLoad() throws SQLException {
		//TODO
		/*/
		List<TagPojo> tagList = Arrays.asList(new TagPojo(1L, "oneTag"));
		List<SimilarStoryPojo> SimilarStoryList = Arrays.asList(new SimilarStoryPojo(1L, "oneSimilarStory"));
		StoryPojo input = new StoryPojo.Builder()
				.id("id")
				.tags(tagList)
				.similarStories(SimilarStoryList)
				.description("test")
				.faviconUrl("test")
				.site("test")
				.title("test")
				.url("test")
				.time(Timestamp.valueOf("2023-11-14 12:08:08.965057"))
				.build();
		
		boolean tagExisting = true;
		int tagChecking = 1;
		int tagSaving = 1;
		boolean similarStoryExisting = true;
		int similarStoryChecking = 1;
		int similarStorySaving = 1;
		
		when(storyDao.save(any(), any())).thenReturn(any());
		
		when(tagDao.check(any(), "oneTag")).thenReturn(tagExisting);
		doNothing().when(tagDao).save(any(), eq(new TagPojo(1L, "oneTag")));
		when(storyAndTagDao.save(any(), eq("id"), eq(1L))).thenReturn(Tuple3.of(1L, "id", 1L));
		
		when(similarStoryDao.check(any(), eq("oneSimilarStory"))).thenReturn(similarStoryExisting);
		doNothing().when(similarStoryDao).save(any(), eq(new SimilarStoryPojo(1L, "oneSimilarStory")));
		when(storyAndSimilarStoryDao.save(any(), eq("id"), eq(1L))).thenReturn(Tuple3.of(1L, "id", 1L));
		
		assertEquals(input, servImpl.load(input));
		
		verify(storyDao, times(1)).save(any(), any(StoryPojo.class));
		
		verify(tagDao, times(tagChecking)).check(any(), eq(anyString()));
		verify(tagDao, times(tagSaving)).save(any(), eq(any(TagPojo.class)));
		verify(storyAndTagDao, times(tagSaving)).save(any(), anyString(), anyLong());
		
		verify(similarStoryDao, times(similarStoryChecking)).check(any(), anyString());
		verify(similarStoryDao, times(similarStorySaving)).save(any(), any(SimilarStoryPojo.class));
		verify(storyAndSimilarStoryDao, times(similarStorySaving)).save(any(), anyString(), anyLong());
	*/
	}

}
