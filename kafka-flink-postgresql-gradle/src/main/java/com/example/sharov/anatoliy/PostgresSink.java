package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.SQL_DRIVER;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class PostgresSink implements SinkFunction<Tuple2<String, Integer>>{
    private final String sqlQuery;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcConnectionOptions connectionOptions;
    private JdbcOperations jdbcOperations;

    public PostgresSink(String sqlQuery) {
        this.sqlQuery = sqlQuery;
        this.executionOptions = JdbcExecutionOptions.builder()
        		.withBatchSize(1000)
        		.withBatchIntervalMs(200)
        		.withMaxRetries(5)
        		.build();
        this.connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(URL)
                .withDriverName(SQL_DRIVER)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .build();
        this.jdbcOperations = new JdbcOperations();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        int count = value.f1;
        String word = value.f0;

        jdbcOperations.putResult(value, sqlQuery);
    }
    
}
