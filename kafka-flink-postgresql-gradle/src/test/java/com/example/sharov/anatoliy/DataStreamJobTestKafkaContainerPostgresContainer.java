package com.example.sharov.anatoliy;

import static com.example.sharov.anatoliy.DataStreamJob.UPDATE_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.INSERT_SQL_QUERY;
import static com.example.sharov.anatoliy.DataStreamJob.URL;
import static com.example.sharov.anatoliy.DataStreamJob.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.example.sharov.anatoliy.DataStreamJob.BOOTSTAP_SERVERS;
import static com.example.sharov.anatoliy.DataStreamJob.PASSWORD;
import static com.example.sharov.anatoliy.DataStreamJob.INPUT_TOPIC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

class DataStreamJobTestKafkaContainerPostgresContainer {
	static KafkaContainer kafkaContainer;
    static PostgreSQLContainer postgresContainer;
        
        /*
        Properties props = KafkaSource.getConsumerProperties();
        String bootstrapServers = props.getProperty("bootstrap.servers");
        assertEquals(kafkaContainer.getBootstrapServers(), bootstrapServers);
        */
    
    @BeforeEach
    public void prepareTestContainers() throws SQLException, InterruptedException, ExecutionException {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.2"));
        postgresContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:15.3"));
        Startables.deepStart(Stream.of(kafkaContainer, postgresContainer)).join();

        Properties kafkaProps = new Properties();
        kafkaProps.put(DataStreamJob.BOOTSTAP_SERVERS, kafkaContainer.getBootstrapServers());
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(kafkaProps.getProperty(DataStreamJob.BOOTSTAP_SERVERS))
                .setTopics(DataStreamJob.INPUT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    	
    	Thread.sleep(3000);
    	
    	 try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
    	        List<String> startData = Arrays.asList(new String[]{"word1", "word2", "word3", "word1"});
    	        
    	        for (String value : startData) {
    	            ProducerRecord<String, String> record = new ProducerRecord<>(INPUT_TOPIC, null, value);
    	            producer.send(record).get();
    	        }
    	    }
    	
    	Properties postgresProps = new Properties();
        postgresProps.put(USERNAME, postgresContainer.getUsername());
        postgresProps.put(PASSWORD, postgresContainer.getPassword());
        postgresProps.put("counted_words", postgresContainer.getDatabaseName());
    
        try (Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),postgresContainer.getUsername(),postgresContainer.getPassword());
                Statement statement = connection.createStatement();) {
            statement.execute("CREATE TABLE counted_words(word_id SERIAL PRIMARY KEY  NOT NULL, word CHARACTER VARYING(189819) UNIQUE NOT NULL, number INTEGER NOT NULL);");
            statement.execute("INSERT INTO counted_words (word, number) VALUES ('word2', 2);");
    	}
    }
    
	@Test
	void test() throws Exception {
        new DataStreamJob().main(null);
        		
		Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		PreparedStatement statement = connection.prepareStatement("SELECT word, number FROM counted_words;");
		ResultSet resultSet = statement.executeQuery();
		
		List<CountedWordPojo> actual = new ArrayList<>();
		List<CountedWordPojo> expected = Arrays.asList(new CountedWordPojo("word1", 1));
		
		while(resultSet.next()) {
			actual.add(new CountedWordPojo(resultSet.getString("word"), resultSet.getInt("number")));
		}
		
		assertEquals(expected, actual);
	}
	
}
