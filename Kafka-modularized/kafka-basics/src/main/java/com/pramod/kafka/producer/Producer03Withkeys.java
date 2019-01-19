package com.pramod.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer03Withkeys {

	public static void main(String args[]) throws InterruptedException, ExecutionException {

		final Logger logger = LoggerFactory.getLogger(Producer03Withkeys.class);

		// Create Kafka Properties
		String bootstrapServer = "127.0.0.1:9092";
		Properties kafkaProperties = new Properties();

		// Refer various properties found here
		// https://kafka.apache.org/documentation/#producerconfigs
		/*
		 * Method-1
		 * 
		 * kafkaProperties.setProperty("bootstrap.servers",bootstrapServer);
		 */
		// Method-2
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);

		
		

		for (int i = 0; i < 10; i++) {
             
			
			// Producer Record

			String topic ="first_topic";
			String value ="Hello From Java"+Integer.toString(i);
			String key="id_"+Integer.toString(i);
			
			ProducerRecord<String, String> ProducerRecord = new ProducerRecord<String, String>(topic,key,value);
			// By providing the key we are making sure that the same key always goes to the same partition 
            logger.info("key"+key);
			
			
			// Send data 
			producer.send(ProducerRecord, new Callback() {

				public void onCompletion(RecordMetadata recordMetadata, Exception e) {

					// Executes every time a record is send or exception is thrown

					if (e == null) {
						logger.info("Received New Metada" + "\n" +

								"Topic:" + recordMetadata.topic() + "\n" + "Partition:" + recordMetadata.partition()
								+ "\n" + "Offset:" + recordMetadata.offset() + "\n" + "Timestamp:"
								+ recordMetadata.timestamp() + "\n"

						);

					}

					else {
						logger.error("Error While Producing" + e);

					}

				}
			}).get(); // adding .get() makes it synchronous - dont do it in production 

		}

		// Flush
		producer.flush();

		// Flush and close

		producer.close();

	}

}
