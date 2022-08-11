package com.kafkademo.kafkaprodcons.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.google.gson.Gson;
import com.kafkademo.kafkaprodcons.models.SimpleModel;

@Configuration
@EnableKafka
public class KafkaConfig {

	
	@Autowired
	private Environment env;
	
	/**
	 * Bean de configuracion Kafka Server
	 * @return
	 */
	
//	Original config with Kafka serializer and deserializer
//	@Bean
//	public ProducerFactory<String, SimpleModel> producerFactory(){
//		Map<String, Object> config = new HashMap<>();
//		
//		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
//		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//
//		return new DefaultKafkaProducerFactory<>(config);
//	}	
//	
//	@Bean
//	public KafkaTemplate<String, SimpleModel> kafkaTemplate(){
//		return new KafkaTemplate<>(producerFactory());
//	}
//	
//	@Bean
//	public ConsumerFactory<String, SimpleModel> consumerFactory(){
//		Map<String, Object> config = new HashMap<>();
//
//		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
//		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//		config.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroupId");
//		
//		return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>(SimpleModel.class));
//	}
//	
//	@Bean
//	public ConcurrentKafkaListenerContainerFactory<String, SimpleModel> kafkaListenerContainerFactory(){
//		
//		ConcurrentKafkaListenerContainerFactory<String, SimpleModel> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
//		
//		concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
//		
//		return concurrentKafkaListenerContainerFactory;
//		
//	}
	

//	Config with GSON serializer and deserializer
	
	@Bean
	public ProducerFactory<String, String> producerFactory(){
		Map<String, Object> config = new HashMap<>();
		
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return new DefaultKafkaProducerFactory<>(config);
	}	
	
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
	
	@Bean
	public ConsumerFactory<String, String> consumerFactory(){
		Map<String, Object> config = new HashMap<>();

		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroupId");
		
		return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer());
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
		
		ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		
		concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
		
		return concurrentKafkaListenerContainerFactory;
		
	}
	
	@Bean
	public Gson jsonConverter() {
		return new Gson();
	}
	
}
