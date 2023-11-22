package com.example.sharov.anatoliy.flink.conf;

import java.sql.Timestamp;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;

public class StoryMessageParser implements MapFunction<Story, StoryPojo>{
	private static final long serialVersionUID = -7308096184214934235L;
	
	public static final Long UNABLE_ID_DATA_EXISTS = 0L; 
	public static final Long UNABLE_ID_ABSENT_DATA = -1L; 

	@Override
	public StoryPojo map(Story message) throws Exception {
		return new StoryPojo.Builder()
		.id(message.getId())
		.title(message.getTitle())
		.url(message.getUrl())
		.site(message.getSite())
		.time(new Timestamp(Long.valueOf(message.getTime())))
		.faviconUrl(message.getFaviconUrl())
		.description(message.getDescription())
        .tags(message.getTagsList().stream().map(this::makeTagPojo).collect(Collectors.toList()))
        .similarStories(message.getSimilarStoriesList().stream().map(this::makeSimpleStoryPojo).collect(Collectors.toList()))
		.build();
	}

	@SuppressWarnings("null")
	private TagPojo makeTagPojo(String each) {
    	
		if(each != null || each.length() != 0) {
			return new TagPojo(UNABLE_ID_DATA_EXISTS, each);
		} 
		return new TagPojo();
		
	}	
	
	@SuppressWarnings("null")
	private SimilarStoryPojo makeSimpleStoryPojo(String each) {
		
		if(each != null || each.length() != 0) {
			return new SimilarStoryPojo(UNABLE_ID_DATA_EXISTS, each);
		} 
		return new SimilarStoryPojo();
	}		

}
