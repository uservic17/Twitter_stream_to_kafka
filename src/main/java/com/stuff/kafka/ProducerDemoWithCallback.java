package com.stuff.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {


  public static void main(String[] args) {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    //Create producer properties
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


    //Create a producer record
    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("my-topic1", "hello jumanji " + i);
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
      });

    }
      producer.flush();
      producer.close();

    //send data - asynchronous
  }
}
