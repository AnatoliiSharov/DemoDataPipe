package com.example.sharov.anatoliy.flink.repository;

import com.example.sharov.anatoliy.flink.entity.TagPojo;

public interface TagDao {

	public boolean checkByTag(String tag);
	
	public boolean checkById(Long tagId);
	
	public Long retrieveNewTagFutureId(String tag);
	
	public TagPojo retrieveByTag(String tag);
	
	public TagPojo saveTag(TagPojo tag);
}
