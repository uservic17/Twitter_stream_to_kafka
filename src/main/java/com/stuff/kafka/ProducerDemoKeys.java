package com.stuff.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {


  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    //Create producer properties
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //Partition 0: 2,4,5
    //Partition 1: 1,3,
    //Partition 2: 0,6,7,8,9

    //Create a producer record
    for (int i = 0; i < 5; i++) {
      String topic = "my-topic1";
      String value = "hello world " + i;
      String key = "key_" + i;
      logger.info("################### Key: " + key);


      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
      producer.send(producerRecord, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          //executes everytime a record is successfully sent or an exception is thrown
          if (e == null) {
            logger.info("\nRecieved new metata "
                    + (("\nTopic: " + recordMetadata.topic())
                    + " \nPartition: " + recordMetadata.partition())
                    + "\nTimestamp: " + recordMetadata.timestamp());
          } else {
            logger.error("Error while producing:  ", e);
          }
        }
      }).get();

    }
    producer.flush();
    producer.close();

    //send data - asynchronous
  }
}
