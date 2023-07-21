package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.COLOMN_OF_RESULT;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.SELECT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcOperations {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcOperations.class);

	public int lookForNuberWord(String word) throws SQLException {
		LOG.debug("JdbcOperations start lookForNuberWord with word = {}", word);
		int count = 0;
		
		try (Connection connect = DriverManager.getConnection(URL, USERNAME, PASSWORD);
				PreparedStatement ps = connect.prepareStatement(SELECT_SQL_QUERY);) {
			LOG.debug("JdbcOperations get connection lookForNuberWord with word = {}", word);
			
			ps.setString(1, word);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				count = resultSet.getInt(COLOMN_OF_RESULT);
				LOG.debug("JdbcOperations found result lookForNuberWord with word = {}, count = {}", word, count);
			} else {
				LOG.debug("JdbcOperations not found result lookForNuberWord with word = {}", word);
			}
		} catch (SQLException e) {
			LOG.error("JdbcOperations has errors lookForNuberWord with word = {}", word);
        }
		
		
		return count + 1;
	}

	public void putResult(Tuple2<String, Integer> value, String sqlQuery) {
		LOG.debug("JdbcOperations start putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
		int count = value.f1;
        String word = value.f0;

        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
             PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
        	LOG.debug("JdbcOperations get connection putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
            statement.setString(1, word);
            statement.setInt(2, count);
            statement.executeUpdate();
        } catch (SQLException e) {
        	LOG.error("JdbcOperations has errors putResult with Tuple2 = {}, sqlQuery = {}", value, sqlQuery);
        }
    }

}
