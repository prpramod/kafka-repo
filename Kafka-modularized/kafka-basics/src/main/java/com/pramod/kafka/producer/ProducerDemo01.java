package com.pramod.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo01 {
	
	public static void main(String args[]) {
		
		// Create  Kafka Properties 
		String bootstrapServer =  "127.0.0.1:9092";
		Properties kafkaProperties = new Properties();
		
		// Refer various properties found here 
		// https://kafka.apache.org/documentation/#producerconfigs 
	/*	
	 *  Method-1 
		
		kafkaProperties.setProperty("bootstrap.servers",bootstrapServer);
		*/
		// Method-2 
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create Producer 
		
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(kafkaProperties);
		
		// Producer Record 
		
		
		ProducerRecord<String,String> ProducerRecord = new ProducerRecord<String,String>("first_topic","Generating Random Number"+(int)10*Math.random());
		

		
		// Send data 
		
		producer.send(ProducerRecord);
		
		//Flush 
		producer.flush();
		
		//Flush and close 
		
		producer.close();
		
		
		
	}

}
