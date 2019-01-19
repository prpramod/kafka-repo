package com.pramod.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	private String consumerKey = "xIXCwAsClVAiVrheSHckNIjth";
	private String consumerSecret = "npwKhwp51hkCGsKZvndPUCiiixDHpgXgSAc1VG3p00Oq8wNzHd";
	private String token = "1075758338426789888-tshlwHzEIlL79wSt7EwLRDpXN2SpMg";
	private String secret = "lNtQV9rd64aDT04VKR0CvcBGia8wTBL6qYN0Xwgwomi1y";
	private List<String> terms = Lists.newArrayList("java","2018","Google","Christmas"); // To follow terms

	public TwitterProducer() {
	}

	public static void main(String[] args) {

		new TwitterProducer().run();
	}

	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// Create a twitter client

		Client client = createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		client.connect();

		
		// Create a kafka producer

		KafkaProducer<String, String> kafkaProducer = createTwitterKafkaProducer();
		
		// Add a shutdown hook 
		
		Runtime.getRuntime().addShutdownHook(new Thread(  () -> {
			
			logger.info("closing the Application....");
			logger.info("Shutting down  the twitter client ....");
			client.stop();
			logger.info("closing the producer");
			kafkaProducer.close();
			logger.info("done");
		}
				
				
				));

		// loop to send tweets to kafka

		// on a different thread, or multiple different threads....
		String msg = null;
		while (!client.isDone()) {

			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);

			} catch (InterruptedException ie) {
				ie.printStackTrace();
				client.stop();
			}

			if (msg != null) {

				logger.info(msg);
				kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback()

				{

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
					
						if(exception!=null)
						{
							
							logger.info("Something bad happened while producing .... ");
						}
						
						
					}

				}

				);

			}

		}
		logger.info("End of Twitter producer Application....");
	}

	private KafkaProducer<String, String> createTwitterKafkaProducer() {

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
		
		// Additional kafka producer  properties to make the producer safer&roboust  ( not to lose messages due to network errors ,maintains order of the message within each partition etc ) 
	  	kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // 5 for kafka 2.1  >= 1.1 or 1 otherwise 
		
		// kafka producer  properties to make it a High throughput  producer (At the expense of a bit of latency & CPU usage ) 
		
		kafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // snappy by Google 
		kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // in Milliseconds
		kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32kb  batch size ,    default is 16*1024 i.e 16kb 
		
		

		// Create Producer

		KafkaProducer<String, String> kafkaTwitterProducer = new KafkaProducer<String, String>(kafkaProperties);

		return kafkaTwitterProducer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L); // To follow
		// people
		
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue); // optional: use this if you want to process
		// client events

		Client hosebirdClient = builder.build();

		return hosebirdClient;

	}

}
