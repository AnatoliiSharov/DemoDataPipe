package com.example.sharov.anatoliy.flink.process;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;
import com.example.sharov.anatoliy.flink.service.Serv;
import com.example.sharov.anatoliy.flink.service.ServImpl;
import com.google.protobuf.ProtocolStringList;

public class StoryMessageParser implements MapFunction<Story, StoryPojo> {
	private static final long serialVersionUID = -7308096184214934235L;
	private static final Logger LOG = LoggerFactory.getLogger(StoryMessageParser.class); 
	
	public static final Long UNABLE_ID_DATA_EXISTS = 0L;
	public static final Long UNABLE_ID_ABSENT_DATA = -1L;

	private Serv service;

	public StoryMessageParser() {
		this.service = new ServImpl();
	}

	@Override
	public StoryPojo map(Story message) throws Exception {
		LOG.debug("StoryMessageParser.map have started with message = {}", message);
		return new StoryPojo.Builder()
				.id(message.getId())
				.title(message.getTitle())
				.url(message.getUrl())
				.site(message.getSite())
				.time(new Timestamp(Long.valueOf(message.getTime())))
				.faviconUrl(message.getFaviconUrl())
				.description(message.getDescription())
				.tags(createTagsPojoList(message.getTagsList()))
				.similarStories(createSimilarStoryPojoList(message.getSimilarStoriesList()))
				.build();
	}

	private List<SimilarStoryPojo> createSimilarStoryPojoList(ProtocolStringList similarStoriesList) {
		LOG.debug("StoryMessageParser.createSimilarStoryPojoList have started with similarStoriesList = {}", similarStoriesList);
		
		if(!similarStoriesList.isEmpty()) {
			return similarStoriesList.stream().map(this::makeSimilarStoryPojo).collect(Collectors.toList());
		}
		return Arrays.asList(new SimilarStoryPojo(UNABLE_ID_ABSENT_DATA, ""));
	}

	private List<TagPojo> createTagsPojoList(ProtocolStringList tagsList) {
		LOG.debug("StoryMessageParser.createTagsPojoList have started with tagsList = {}", tagsList);
		
		if(!tagsList.isEmpty()) {
			return tagsList.stream().map(this::makeTagPojo).collect(Collectors.toList());
		}
		return Arrays.asList(new TagPojo(UNABLE_ID_ABSENT_DATA, ""));
	}

	private TagPojo makeTagPojo(String each) {
		
		if (each != null && !each.isEmpty() && each.length() != 0) {
			try {
				return service.fillTagId(each);
			} catch (IllegalStateException | SQLException e) {
				e.printStackTrace();
			}
		}
		return new TagPojo(UNABLE_ID_ABSENT_DATA, "");
	}

	private SimilarStoryPojo makeSimilarStoryPojo(String each) {

		if (each != null && !each.isEmpty()) {
			try {
				return service.fillSimilarStoryId(each);
			} catch (IllegalStateException | SQLException e) {
				e.printStackTrace();
			}
		}
		return new SimilarStoryPojo(UNABLE_ID_ABSENT_DATA, "");
	}

}
