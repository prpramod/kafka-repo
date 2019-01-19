package com.pramod.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pramod.kafka.producer.Producer02WithCallBacks;

public class Consumer05Groups {

	public static void main(String[] args) {
	
		final Logger logger = LoggerFactory.getLogger(Consumer05Groups.class);
		String bootstrapServer =  "127.0.0.1:9092";
		String groupId ="Pramods-Java-Consumer-group"; 
		
		// By Changing the group id we reset the  application or reset the offsets 
	 	// If we run this program 3 times it will create 3 consumers consuming "first_topic 
		// belonging to the consumer group "Pramods-Java-Consumer-group"
		
		String offsetResetPolicy = "earliest"; // latest - shows only unseen messages , earliest - shows All the messages ,none - no messages will throw error 
		String topic="first_topic";
		
		Properties kafkaProperties = new Properties();
	
		//Create Consumer config 
		kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPolicy);
		
		
		// Create consumer 
		
		KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(kafkaProperties);
		
		
		
		// Subscribe to Topic(s) 
		
		// kafkaConsumer.subscribe(Collections.singleton(topic)); //Collections.singleton() means forcing the consumer to  subscribe to only one topic 
		kafkaConsumer.subscribe(Arrays.asList(topic)); // we can also provide a list of topics in the form of array as well 
		
		// Poll for new data 
		
		while(true) {
			
			
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));// added  kafka 2.0 onwards 
		
		  for(ConsumerRecord consumerRecord:consumerRecords)
		  {
			  
		logger.info("key:"+consumerRecord.key()+", value:"+consumerRecord.value());
		logger.info("partition:"+consumerRecord.partition()+", offset"+consumerRecord.offset());
		
		
			  
		  }
		
		
		}
		
		
		
	}

}
