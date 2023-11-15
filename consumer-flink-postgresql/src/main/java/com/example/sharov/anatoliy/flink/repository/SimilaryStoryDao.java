package com.example.sharov.anatoliy.flink.repository;

import com.example.sharov.anatoliy.flink.entity.SimilarStoryPojo;

public interface SimilaryStoryDao {

	public boolean checkBySimilarStory(String similarStory);
	
	public boolean checkById(Long similarStoryId);
	
	public Long retrieveFutureId(String similarStory);
	
	public SimilarStoryPojo retrieveBySimilarStory(String similarStory);
	
	public SimilarStoryPojo save(SimilarStoryPojo similarStory);
	
}
