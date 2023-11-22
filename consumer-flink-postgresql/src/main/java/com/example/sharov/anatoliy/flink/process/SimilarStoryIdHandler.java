package com.example.sharov.anatoliy.flink.process;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.service.Serv;
import com.example.sharov.anatoliy.flink.service.ServImpl;

public class SimilarStoryIdHandler implements MapFunction<StoryPojo, StoryPojo> {
	private static final long serialVersionUID = -5557863778756215197L;
	private Serv service;

	public SimilarStoryIdHandler() {
		this.service = new ServImpl();
	}

	@Override
	public StoryPojo map(StoryPojo story) throws Exception {

		if (story.getSimilarStories() != null) {
			List<SimilarStoryPojo> result = new ArrayList<>();

			for (SimilarStoryPojo each : story.getSimilarStories()) {
				result.add(service.fillId(each));
			}
			return new StoryPojo.Builder().id(story.getId()).title(story.getTitle()).url(story.getUrl())
					.site(story.getSite()).time(story.getTime()).faviconUrl(story.getFaviconUrl())
					.description(story.getDescription())
					.tags(story.getTags())
					.similarStories(result).build();
		}
		return story;
	}

}