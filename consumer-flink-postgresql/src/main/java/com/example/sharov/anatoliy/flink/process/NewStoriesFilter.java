package com.example.sharov.anatoliy.flink.process;

import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.service.Serv;
import com.example.sharov.anatoliy.flink.service.ServImpl;

public class NewStoriesFilter implements FilterFunction<StoryPojo>{
	private static final long serialVersionUID = 975616519208227157L;
	private static final Logger LOG = LoggerFactory.getLogger(NewStoriesFilter.class);
	
	private Serv service;
	
	public NewStoriesFilter() {
		this.service = new ServImpl();
	}

	@Override
	public boolean filter(StoryPojo value) throws Exception {
		return !service.checkStoryAlreadyExist(value);
	}

}
