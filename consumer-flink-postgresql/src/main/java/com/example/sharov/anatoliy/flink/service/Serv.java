package com.example.sharov.anatoliy.flink.service;

import java.io.Serializable;
import java.sql.SQLException;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;
import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.entity.TagPojo;

public interface Serv extends Serializable{
	
	public boolean checkStoryAlreadyExist(StoryPojo value) throws SQLException;

	public TagPojo fillId(TagPojo value) throws SQLException;
	
	public SimilarStoryPojo fillId(SimilarStoryPojo value) throws SQLException;

	public StoryPojo load(StoryPojo value) throws SQLException;
}

