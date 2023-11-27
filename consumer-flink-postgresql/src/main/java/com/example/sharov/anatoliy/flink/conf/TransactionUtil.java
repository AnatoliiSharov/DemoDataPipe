package com.example.sharov.anatoliy.flink.conf;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public class TransactionUtil implements Serializable{
	private static final long serialVersionUID = 4184134524971985300L;

	public <T> Optional<T> goReturningTransaction(DatabaseConnector connector, ConnectionConcumer<T> consumer) throws SQLException {
		Optional<T> result;
		
		try(Connection connection = connector.getConnection()){
			connection.setAutoCommit(false);
			
			try{
				result = consumer.concume(connection);
				connection.commit();
			} catch (Exception e) {
				connection.rollback();
				throw new SQLException ("Exception within transaction", e);
			}
		}
		return result;
	}
	
	public interface ConnectionConcumer<T>{
		Optional<T> concume(Connection connection) throws Exception;
	}
	public interface ConnectionVoidConcumer{
		void concume(Connection connection) throws Exception;
	}

	public void goVoidingTransaction(DatabaseConnector connector, ConnectionVoidConcumer consumer) throws SQLException {
		try(Connection connection = connector.getConnection()){
			connection.setAutoCommit(false);
			
			try{
				consumer.concume(connection);
				connection.commit();
				
			} catch (Exception e) {
				connection.rollback();
				throw new SQLException ("Exception within transaction", e);
			}
		}
	}

}
