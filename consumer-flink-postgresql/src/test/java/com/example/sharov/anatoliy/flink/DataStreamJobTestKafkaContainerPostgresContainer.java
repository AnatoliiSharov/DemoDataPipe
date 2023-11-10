package com.example.sharov.anatoliy.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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

import com.example.sharov.anatoliy.flink.DataStreamJob;

class DataStreamJobTestKafkaContainerPostgresContainer {
	static KafkaContainer kafkaContainer;
    static PostgreSQLContainer postgresContainer;
    Properties postgresProps;
    Properties kafkaProps;
/*        
    @BeforeEach
    public void prepareTestContainers() throws SQLException, InterruptedException, ExecutionException {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.2"));
        postgresContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:15.3"));
        Startables.deepStart(Stream.of(kafkaContainer, postgresContainer)).join();

        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());

        //Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> kafkaContainer.isRunning());
        
    	Thread.sleep(3000);
    	
    	 try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
    	        List<String> startData = Arrays.asList(new String[]{"word1", "word2", "word3", "word1"});
    	        
    	        for (String value : startData) {
    	            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, value);
    	            producer.send(record).get();
    	        }
    	    }
    	
    	postgresProps = new Properties();
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
		DataStreamJob dataStreamJobnew = new DataStreamJob();
		KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                .setTopics(DataStreamJob.TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
		
		dataStreamJobnew.processData(kafkaSource);		
		
		Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		PreparedStatement statement = connection.prepareStatement("SELECT word, number FROM counted_words;");
		ResultSet resultSet = statement.executeQuery();
		
		List<CountedWordPojo> actual = new ArrayList<>();
		CountedWordPojo expectedPojo = new CountedWordPojo();
		expectedPojo.setNumber(1);
		expectedPojo.setWord("word1");
		List<CountedWordPojo> expected = Arrays.asList(expectedPojo);
		CountedWordPojo сountedWordPojo = new CountedWordPojo();
		
		while(resultSet.next()) {
			сountedWordPojo.setNumber(resultSet.getInt("number"));
			сountedWordPojo.setWord(resultSet.getString("word"));
			actual.add(сountedWordPojo);
		}
		
		assertEquals(expected, actual);
	}
	*/
	
}