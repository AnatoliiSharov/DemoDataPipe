package com.example.sharov.anatoliy.flink.process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.conf.ConfParams;
import com.example.sharov.anatoliy.flink.conf.StoryFlink;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.service.Serv;
import com.example.sharov.anatoliy.flink.service.ServImpl;

public class TegIdHandler implements MapFunction<StoryPojo, StoryPojo> {
	private static final long serialVersionUID = -8655749500756635739L;
	private static final Logger LOG = LoggerFactory.getLogger(TegIdHandler.class);

	private Serv service;

	public TegIdHandler() {
		this.service = new ServImpl();
	}

	@Override
	public StoryPojo map(StoryPojo story) throws Exception {

		if (story.getTags() != null) {
			List<TagPojo> result = new ArrayList<>();

			for (TagPojo each : story.getTags()) {
				result.add(service.fillId(each));
			}
			return new StoryPojo.Builder().id(story.getId()).title(story.getTitle()).url(story.getUrl())
					.site(story.getSite()).time(story.getTime()).faviconUrl(story.getFaviconUrl())
					.description(story.getDescription())
					.tags(result)
					.similarStories(story.getSimilarStories()).build();
		}
		return story;
	}

}
