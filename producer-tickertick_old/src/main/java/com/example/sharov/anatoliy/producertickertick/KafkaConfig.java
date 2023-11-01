package com.example.sharov.anatoliy.producertickertick;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String bootstrapAddress;
	
	@Value(value = "${spring.kafka.topic}")
	private String topic;
	
	@Value(value = "${spring.kafka.producer.value-serializer}")
	private String serializer;
	
	
	@Bean
    public NewTopic topic1() {
         return new NewTopic(topic, 1, (short) 1);
    }
	
	@Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }
	
	@Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          bootstrapAddress);
        configProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
          serializer);
        return new DefaultKafkaProducerFactory(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
	
}
