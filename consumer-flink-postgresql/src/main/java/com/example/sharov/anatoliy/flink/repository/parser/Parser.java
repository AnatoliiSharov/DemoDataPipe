package com.example.sharov.anatoliy.flink.repository.parser;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Parser <T>{

	T apply(ResultSet rs) throws SQLException;
	
}
