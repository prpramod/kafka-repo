package com.pramod.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer02WithCallBacks {
	
	public static void main(String args[]) {
		
		final Logger logger = LoggerFactory.getLogger(Producer02WithCallBacks.class);
		
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
		

		
		// Send data Asynchronously
		
	//	producer.send(ProducerRecord);
		//Callback callback = null;
		producer.send(ProducerRecord,new Callback() { 
			
			
			public void onCompletion(RecordMetadata recordMetadata,Exception e ) {
				
				// Executes every time a record is send or exception is thrown 
				
				if(e==null) 
				{
					logger.info("Received New Metada"+"\n" +
				        
                          "Topic:"+recordMetadata.topic()+"\n"+
                          "Partition:"+recordMetadata.partition()+"\n"+
                          "Offset:"+recordMetadata.offset()+"\n"+
                          "Timestamp:"+recordMetadata.timestamp()+"\n"
							
							);
					
				}
				
				 else
				 {
					 logger.error("Error While Producing"+e);
					
				 }
				
			}
		});
		
		//Flush 
		producer.flush();
		
		//Flush and close 
		
		producer.close();
		
		
		
	}

}
