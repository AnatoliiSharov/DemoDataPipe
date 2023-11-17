package com.example.sharov.anatoliy.flink.repository.impl;

import java.sql.Connection;

import com.example.sharov.anatoliy.flink.entity.TagPojo;
import com.example.sharov.anatoliy.flink.repository.TagDao;

public class TagDaoImpl implements TagDao {

	@Override
	public boolean checkByTag(Connection connection, String tag) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean checkById(Connection connection, Long tagId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Long retrieveNewTagFutureId(Connection connection, String tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TagPojo retrieveByTag(Connection connection, String tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TagPojo saveTag(Connection connection, TagPojo tag) {
		// TODO Auto-generated method stub
		return null;
	}

}
