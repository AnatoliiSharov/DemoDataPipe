package com.example.sharov.anatoliy.flink.conf;

import static org.mockito.Mockito.*;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;

class StoryMessageParserTest {

	private Story message;
	private StoryMessageParser parser;

	@BeforeEach
	public void prepareTest() {
		parser = new StoryMessageParser();
	}

	@Test
	void testMap_WhenAllDateExist_ShouldTupleTagsIdIsZeroAndTupleSimilarStoryIdIsZero() throws Exception {
		message = Story.newBuilder().setId("storyId").setTitle("storyTitle").setUrl("storyUrl").setSite("storySite")
				.setTime("111111111").setFaviconUrl("storyFaviconUrl").addTags("firstTag").addTags("secondTag")
				.addSimilarStories("firstSimilarStories").addSimilarStories("secondSimilarStories")
				.setDescription("storyDescription").build();
		String expected = "StoryFlink [id=storyId, title=storyTitle, url=storyUrl, site=storySite, time=1970-01-02 09:51:51.111, favicon_url=storyFaviconUrl, tags=[(0,incoming,firstTag), (0,incoming,secondTag)], similar_stories=[(0,incoming,firstSimilarStories), (0,incoming,secondSimilarStories)], description=storyDescription]";
		
		assertEquals(expected, parser.map(message).toString());
	}
	
	@Test
	void testMap_WhenTagsIsAbsent_ShouldListTupleTagsIdIsEmptyAndTupleSimilarStoryIdIsZero() throws Exception {
		message = Story.newBuilder().setId("storyId").setTitle("storyTitle").setUrl("storyUrl").setSite("storySite")
				.setTime("111111111").setFaviconUrl("storyFaviconUrl")
				.addAllTags(new ArrayList<>())
				.addSimilarStories("firstSimilarStories").addSimilarStories("secondSimilarStories")
				.setDescription("storyDescription").build();
		String expected = "StoryFlink [id=storyId, title=storyTitle, url=storyUrl, site=storySite, time=1970-01-02 09:51:51.111, favicon_url=storyFaviconUrl, tags=[], similar_stories=[(0,incoming,firstSimilarStories), (0,incoming,secondSimilarStories)], description=storyDescription]";
		
		assertEquals(expected, parser.map(message).toString());
	}
	
	@Test
	void testMap_WhenTagsIsAbsent_ShouldTupleTagsStoryIdIsZeroAndListTupleSimilarStoryIdIsEmpty() throws Exception {
		message = Story.newBuilder().setId("storyId").setTitle("storyTitle").setUrl("storyUrl").setSite("storySite")
				.setTime("111111111").setFaviconUrl("storyFaviconUrl")
				.addAllTags(new ArrayList<>())
				.addTags("firstTag").addTags("secondTag")
				.setDescription("storyDescription").build();
		String expected = "StoryFlink [id=storyId, title=storyTitle, url=storyUrl, site=storySite, time=1970-01-02 09:51:51.111, favicon_url=storyFaviconUrl, tags=[(0,incoming,firstTag), (0,incoming,secondTag)], similar_stories=[], description=storyDescription]";
		
		assertEquals(expected, parser.map(message).toString());
	}

}
