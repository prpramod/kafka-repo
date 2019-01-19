package com.pramod.kafka.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class TwitterFilterUsingKafkaStreams {

	static Logger logger = LoggerFactory.getLogger(TwitterFilterUsingKafkaStreams.class.getName());
	public static void main(String[] args) {
	

		// Create kafkaStreams Properties 
		
	Properties kafkaStreamProperties = new 	Properties();
	
	kafkaStreamProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");	
	kafkaStreamProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-twitter"); // ID of the consumer equivalent of the kafka Streams 
	kafkaStreamProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
	kafkaStreamProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
	
	
	// Create a topology 
	
	StreamsBuilder streamsBuilder = new StreamsBuilder();
	
	// Input topic to the stream 
	
	
	KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
	
	KStream<String, String> filteredTweeterStream = inputTopic.filter( (key,tweetInJsonFormat) -> extractUserFollowerFromTweets(tweetInJsonFormat)>10000);
	
	 
	filteredTweeterStream.to("important_tweets");
	
	// Build the stream  Topology 
	
	KafkaStreams  kafkaStreams  = new KafkaStreams(streamsBuilder.build(), kafkaStreamProperties);
	
	// Start streams application 
	
	kafkaStreams.start();
	
	
	
	}
	
	private static JsonParser jsonParser = new JsonParser();

	private static Integer  extractUserFollowerFromTweets(String tweetInJsonFormat) {
		// Using GSON library to extract fields  from JSON .We can also use Jackson library but GSON is much simpler to use.
		
		try {
	   return 	jsonParser.parse(tweetInJsonFormat).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
		
		}catch (NullPointerException e )
		{
			logger.error("bad tweet"+e);
			return 0;
		}

		

	}

}
