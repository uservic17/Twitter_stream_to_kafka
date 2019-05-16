package com.stuff.twitter;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {


  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  public static void main(String[] args) {
    TwitterProducer producer = new TwitterProducer();
    producer.run();
  }

  private void run() {
    //Create  a twitter client
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10);

    Client hosebirdClient = createTwitterClient(msgQueue);
    hosebirdClient.connect();
    KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down everything..");
      hosebirdClient.stop();
      kafkaProducer.flush();
      kafkaProducer.close();
    }));
    while (!hosebirdClient.isDone()) {
      String msg = null;
      try {
        msg = (msgQueue.poll(5, TimeUnit.SECONDS));
        //Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("twitter_tweets", msg);
        logger.info(msg);
        //send data - asynchronous
        kafkaProducer.send(producerRecord, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              logger.error("Something has happened.. " + e);
            }
          }
        });
      } catch (InterruptedException e) {
        e.printStackTrace();
        hosebirdClient.stop();
        kafkaProducer.flush();
        kafkaProducer.close();
      }
    }
    logger.info("Done reading tweets...");
    //create kafka producer

    //send data
  }



  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //SAFE PRODUCER
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


    return new KafkaProducer<>(properties);

  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<String> terms = Lists.newArrayList("bitcoin");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Properties properties = PropertiesReader.loadTwitterProperties();
    String consumer_key = properties.getProperty("CONSUMER_KEY");
    System.out.println(consumer_key);
    String consumer_secret = properties.getProperty("CONSUMER_SECRET");
    System.out.println(consumer_key);
    String token = properties.getProperty("TOKEN");
    System.out.println(token);
    String secret = properties.getProperty("SECRET");
    System.out.println(secret);
    Authentication hosebirdAuth = new OAuth1(consumer_key, consumer_secret, token, secret);

    ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }
}
