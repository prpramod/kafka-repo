package com.pramod.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pramod.kafka.producer.Producer02WithCallBacks;

public class Consumer06WithThread {

	public static void main(String[] args) {

	 new 	Consumer06WithThread().run();
	}

	public Consumer06WithThread() {
	}

	private void run() {

		Logger logger = LoggerFactory.getLogger(Consumer06WithThread.class);
		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "Pramods-MultiThreadedApp-group";
		String offsetResetPolicy = "earliest";
		String topic = "first_topic";
		
		// Latch for dealing with multiple threads 
        CountDownLatch latch = new CountDownLatch(1);
		
        // Create the consumer runnable 
        logger.info("Creating the consumer thread ....");
		Runnable myConsumerRunnable = new ConsumerRunnable(topic,bootstrapServer,groupId,offsetResetPolicy,latch);
		
		// Start the thread 
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// add a shutdown hook to shutdown the application properly 
		
		Runtime.getRuntime().addShutdownHook(new Thread( 
				() -> { 
					
				logger.info("caught shutdown hook ");	
				((ConsumerRunnable) myConsumerRunnable).shutdown();
				try {
					latch.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.info("Application has exited ");	
				}
				
				));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted!!!",e);
			e.printStackTrace();
		} finally 
		{
			
			logger.info("Application is closing.....");
		}
		
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String,String> kafkaConsumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		public ConsumerRunnable(String topic, String bootstrapServer, String groupId, String offsetResetPolicy,
				CountDownLatch latch) {
			this.latch = latch;

			Properties kafkaProperties = new Properties();

			// Create Consumer config
			kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());
			kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());

			kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPolicy);

			// Create consumer

			kafkaConsumer = new KafkaConsumer<String, String>(kafkaProperties);

			// Subscribe to Topic(s)

			// kafkaConsumer.subscribe(Collections.singleton(topic));
			// //Collections.singleton() means forcing the consumer to subscribe to only one
			// topic
			kafkaConsumer.subscribe(Arrays.asList(topic)); // we can also provide a list of topics in the form of array
															// as well

			// Poll for new data

		}

		@Override
		public void run() {

			try {

				while (true) {

					ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));// added
																													// kafka
																													// 2.0
																													// onwards

					for (ConsumerRecord consumerRecord : consumerRecords) {

						logger.info("key:" + consumerRecord.key() + ", value:" + consumerRecord.value());
						logger.info("partition:" + consumerRecord.partition() + ", offset" + consumerRecord.offset());

					}

				}

			} catch (WakeupException we) {

				logger.info("Received Shutdown Signal");

			} finally {
				// To tell the main thread that we are done with the consumer
				kafkaConsumer.close();

			}

		}

		// To shutdown consumer thread
		public void shutdown() {
			// wakeup() is a special method to interrupt kafkaConsumer.poll()
			// It throws WakeupException
			kafkaConsumer.wakeup();
		}

	}

}
