package com.example.sharov.anatoliy.flink.process;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.process.StoryMessageParser;
import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;
import com.example.sharov.anatoliy.flink.service.Serv;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StoryMessageParserTest {

	private Story input;
	private StoryPojo expected;
	@Mock
	private Serv service;
	@InjectMocks
	private StoryMessageParser parser;
	@BeforeEach
	public void prepareTest() {
		input = Story.newBuilder().setId("storyId").setTitle("storyTitle").setUrl("storyUrl").setSite("storySite")
	            .setTime("111111111").setFaviconUrl("storyFaviconUrl")
	            .setDescription("storyDescription").build();
		expected = new StoryPojo.Builder()
				.id("storyId").title("storyTitle").url("storyUrl").site("storySite")
	            .time(Timestamp.valueOf("1970-01-02 09:51:51.111")).faviconUrl("storyFaviconUrl")
	            .description("storyDescription")
				.build();
	}

	@Test
	void testMap_WhenNoExistTagAndSimilarStory_ShouldCreateSpecifyTagPojoAndSimilarStoryPojo() throws Exception {
		expected.setTags(Arrays.asList(new TagPojo(-1L, "")));
		expected.setSimilarStories(Arrays.asList(new SimilarStoryPojo(-1L, "")));
		
		when(service.fillTagId(anyString())).thenReturn(new TagPojo(1L, "firstTag"));
		when(service.fillSimilarStoryId(anyString())).thenReturn(new SimilarStoryPojo(1L, "firstSimilarStory"));
		
	    assertEquals(expected, parser.map(input)); 
	    
	    verify(service, times(0)).fillTagId(anyString());
	    verify(service, times(0)).fillSimilarStoryId(anyString());
	}
	
	@Test
	void testMap_WhenOneTagExist_ShouldCreateTagPojo() throws Exception {
		input = input.toBuilder().addTags("firstTag").build();
		expected.setTags(Arrays.asList(new TagPojo(1L, "firstTag")));
		expected.setSimilarStories(Arrays.asList(new SimilarStoryPojo(-1L, "")));
		
		when(service.fillTagId("firstTag")).thenReturn(new TagPojo(1L, "firstTag"));
		when(service.fillSimilarStoryId(anyString())).thenReturn(new SimilarStoryPojo(1L, "firstSimilarStory"));
		
		assertEquals(expected, parser.map(input));
		
		verify(service, times(1)).fillTagId(anyString());
	    verify(service, times(0)).fillSimilarStoryId(anyString());
	}
	
	@Test
	void testMap_WhenTreeTagsExist_ShouldCreateTreeTagPojo() throws Exception {
		input = input.toBuilder().addTags("firstTag").addTags("secondTag").addTags("thirdTag").build();
		expected.setTags(Arrays.asList(new TagPojo(1L, "firstTag"), new TagPojo(2L, "secondTag"), new TagPojo(3L, "thirdTag")));
		expected.setSimilarStories(Arrays.asList(new SimilarStoryPojo(-1L, "")));
		
		when(service.fillTagId("firstTag")).thenReturn(new TagPojo(1L, "firstTag"));
		when(service.fillTagId("secondTag")).thenReturn(new TagPojo(2L, "secondTag"));
		when(service.fillTagId("thirdTag")).thenReturn(new TagPojo(3L, "thirdTag"));
		when(service.fillSimilarStoryId(anyString())).thenReturn(new SimilarStoryPojo(1L, "firstSimilarStory"));
		
		assertEquals(expected, parser.map(input));
		
		verify(service, times(3)).fillTagId(anyString());
		verify(service, times(0)).fillSimilarStoryId(anyString());
	}
	
	@Test
	void testMap_WhenOneSimilarStoryExist_ShouldCreateSimilarStoryPojos() throws Exception {
		input = input.toBuilder().addSimilarStories("firstSimilarStory").build();
		expected.setSimilarStories(Arrays.asList(new SimilarStoryPojo(1L, "firstSimilarStory")));
		expected.setTags(Arrays.asList(new TagPojo(-1L, "")));
		
		when(service.fillTagId(anyString())).thenReturn(new TagPojo(1L, "firstTag"));
		when(service.fillSimilarStoryId("firstSimilarStory")).thenReturn(new SimilarStoryPojo(1L, "firstSimilarStory"));
		
		assertEquals(expected, parser.map(input));
		
		verify(service, times(0)).fillTagId(anyString());
		verify(service, times(1)).fillSimilarStoryId(anyString());
	}
	
	@Test
	void testMap_WhenTreeSimilarStoriesExist_ShouldCreateTreeSimilarStoryPojos() throws Exception {
		input = input.toBuilder().addSimilarStories("firstSimilarStory").addSimilarStories("secondSimilarStory").addSimilarStories("thirdSimilarStory").build();
		expected.setSimilarStories(Arrays.asList(new SimilarStoryPojo(1L, "firstSimilarStory"), new SimilarStoryPojo(2L, "secondSimilarStory"), new SimilarStoryPojo(3L, "thirdSimilarStory")));
		expected.setTags(Arrays.asList(new TagPojo(-1L, "")));
		
		
		when(service.fillTagId(anyString())).thenReturn(new TagPojo(1L, "firstTag"));
		when(service.fillSimilarStoryId("firstSimilarStory")).thenReturn(new SimilarStoryPojo(1L, "firstSimilarStory"));
		when(service.fillSimilarStoryId("secondSimilarStory")).thenReturn(new SimilarStoryPojo(2L, "secondSimilarStory"));
		when(service.fillSimilarStoryId("thirdSimilarStory")).thenReturn(new SimilarStoryPojo(3L, "thirdSimilarStory"));
		
		assertEquals(expected, parser.map(input));
		
		verify(service, times(0)).fillTagId(anyString());
		verify(service, times(3)).fillSimilarStoryId(anyString());
	}
	
}