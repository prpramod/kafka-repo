package com.pramod.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pramod.kafka.producer.Producer02WithCallBacks;

public class Consumer07AssignAndSeek {

	public static void main(String[] args) {
	
		final Logger logger = LoggerFactory.getLogger(Consumer07AssignAndSeek.class);
		String bootstrapServer =  "127.0.0.1:9092";
		 
		
			
		String offsetResetPolicy = "earliest"; // latest - shows only unseen messages , earliest - shows All the messages ,none - no messages will throw error 
		String topic="first_topic";
		
		Properties kafkaProperties = new Properties();
	
		//Create Consumer config 
		kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
	
		kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPolicy);
		
		
		// Create consumer 
		
		KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(kafkaProperties);
		
		// Assign and seek are mostly used to replay data and or fetch a specific message 
		
		
		// Assign 
		
		TopicPartition partition = new  TopicPartition(topic,0);
		long offsetToReadFrom =20l;
		kafkaConsumer.assign(Arrays.asList(partition));
		
		//
		
		kafkaConsumer.seek(partition, offsetToReadFrom);
		int NumberOfMessagesToread=5;
		boolean keepReading=true;
		int messagesReadSoFar=0;
		
		
		
		// Poll for new data 
		
		while(keepReading) {
			
			
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));// added  kafka 2.0 onwards 
		
		  for(ConsumerRecord consumerRecord:consumerRecords)
		  { 
			  messagesReadSoFar+=1;
			  
		logger.info("key:"+consumerRecord.key()+", value:"+consumerRecord.value());
		logger.info("partition:"+consumerRecord.partition()+", offset"+consumerRecord.offset());
		
		 if(messagesReadSoFar>=NumberOfMessagesToread) {
			 
			 keepReading=false; // to exit while lookop 
			 break; // To exit the for loop 
		 }
			 
			  
		  }
		
		
		}
		
		logger.info("Exiting the application ....");
		
		
	}

}
