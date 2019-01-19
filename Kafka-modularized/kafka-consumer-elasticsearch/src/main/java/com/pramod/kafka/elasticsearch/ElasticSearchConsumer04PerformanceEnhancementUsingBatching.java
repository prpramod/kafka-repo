package com.pramod.kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer04PerformanceEnhancementUsingBatching {

	public static RestHighLevelClient createClient() {
		// https://at25nml65q:ogdcj8xu83@
		String hostname = "pramod-5965629745.ap-southeast-2.bonsaisearch.net";
		String username = "at25nml65q";
		String password = "ogdcj8xu83";

		// Dont do this if you a local Elastic Search
		// This is for bonzai elastic search which runs on cloud
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();

		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);

		return restHighLevelClient;

	}

	public static KafkaConsumer<String, String> createKafkaTwitterConsumer(String topic) {

		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "Kafka-Twitter-Consumer-group";
		String offsetResetPolicy = "earliest"; // latest - shows only unseen messages , earliest - shows All the
												// messages ,none - no messages will throw error

		Properties kafkaProperties = new Properties();

		// Create Consumer config
		kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPolicy);
		kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// Disables the Autocommit of offsets ,by default that is set to autocommit 
		kafkaProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

		KafkaConsumer<String, String> KafkaTwitterConsumer = new KafkaConsumer<String, String>(kafkaProperties);

		
		KafkaTwitterConsumer.subscribe(Arrays.asList(topic)); // we can also provide a list of topics in the form of
																// array as well

		return KafkaTwitterConsumer;

	}

	public static void main(String[] args) throws IOException, InterruptedException {

		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer04PerformanceEnhancementUsingBatching.class);

		RestHighLevelClient restHighLevelClient = createClient();
		KafkaConsumer<String, String> kafkaTwitterConsumer = createKafkaTwitterConsumer("twitter_tweets");

		// Poll for new data
		while (true) {

			ConsumerRecords<String, String> consumerRecords = kafkaTwitterConsumer.poll(Duration.ofMillis(100));// added
																												// kafka
			   int recordCount = consumerRecords.count();																						// 2.0
																												// onwards
             logger.info("Received "+recordCount+" messages");   
		     BulkRequest bulkRequest = new BulkRequest();
             
             for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

				
            	 try {
				String id = extractIdFromTweet(consumerRecord.value());
//				logger.info("key:" + consumerRecord.key() + ", value:" + consumerRecord.value());
//				logger.info("partition:" + consumerRecord.partition() + ", offset" + consumerRecord.offset());

				//
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id) // To make consumer idempotent
						.source(consumerRecord.value(), XContentType.JSON);
				
				bulkRequest.add(indexRequest);
				
             } catch (NullPointerException e )
            	 {
            	 
            	 logger.warn(" Skip bad data:"+consumerRecord.value());
            //	 e.printStackTrace();
            	 }
				
			}
             
             if(recordCount>0) {
             BulkResponse  indexResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
             
             
			logger.info("Commiting offsets.......");
			kafkaTwitterConsumer.commitSync();
			logger.info(" offsets have been comitted .......");
			
			Thread.sleep(1000);
			
             }
			// This will poll 10 records and then commit offsets 
			// If after 5 records/messages  the system goes down ( Manually stop the application ) the the consumer should start reading from these 5 records/messages 
      // verify this in Kafka console running the command mentioned below 
			
			// kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group Kafka-Twitter-Consumer-group --describe 
		}

	}

	private static JsonParser jsonParser = new JsonParser();

	private static String extractIdFromTweet(String tweetInJsonFormat) {
		// Using GSON library to extract fields  from JSON .We can also use Jackson library but GSON is much simpler to use.

		return jsonParser.parse(tweetInJsonFormat).getAsJsonObject().get("id_str").getAsString();

	}

}
