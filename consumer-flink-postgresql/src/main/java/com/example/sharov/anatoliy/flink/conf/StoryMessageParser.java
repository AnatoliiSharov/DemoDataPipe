package com.example.sharov.anatoliy.flink.conf;

import java.sql.Timestamp;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;

public class StoryMessageParser implements MapFunction<Story, StoryFlink>{
	private static final long serialVersionUID = -7308096184214934235L;
	private static final String INCOMING_ONE = "incoming"; 
	private static final Long UNABLE_ID_DATA_EXISTS = 0L; 
	private static final Long UNABLE_ID_MISSING_DATA = -1L;
	//TODO instead of static final we need enum

	@Override
	public StoryFlink map(Story message) throws Exception {
		StoryFlink result = new StoryFlink();

		result.setId(message.getId());
		result.setTitle(message.getTitle());
		result.setUrl(message.getUrl());
		result.setSite(message.getSite());
		result.setTime(new Timestamp(Long.valueOf(message.getTime())));
		result.setFavicon_url(message.getFaviconUrl());
        result.setTags(message.getTagsList().stream().map(this::makeTupleEach).collect(Collectors.toList()));
        result.setSimilar_stories(message.getTagsList().stream().map(this::makeTupleEach).collect(Collectors.toList()));
		result.setDescription(message.getDescription());
		return result;
	}

	@SuppressWarnings("null")
	private Tuple3<Long, String, String> makeTupleEach(String each) {
		Tuple3<Long, String, String> instedOfSimilarStory = new Tuple3<>(UNABLE_ID_MISSING_DATA, INCOMING_ONE,  null);
    	
		if(each != null || each.length() != 0) {
			instedOfSimilarStory = new Tuple3<>(UNABLE_ID_DATA_EXISTS, INCOMING_ONE,each);
		} 
		return instedOfSimilarStory;
	}		

}
