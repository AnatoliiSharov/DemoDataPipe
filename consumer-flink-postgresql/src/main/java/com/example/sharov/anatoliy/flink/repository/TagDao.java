package com.example.sharov.anatoliy.flink.repository;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import com.example.sharov.anatoliy.flink.entity.TagPojo;

public interface TagDao extends Serializable{

	public boolean check(Connection connection, String tag) throws SQLException;
	
	public boolean check(Connection connection, Long tagId) throws SQLException;
	
	public Optional<TagPojo> findWithFutureId(Connection connection, String tag) throws SQLException;
	
	public Optional<TagPojo> find(Connection connection, String tag) throws SQLException;
	
	public void save(Connection connection, TagPojo tag) throws SQLException;
	
}
