package com.stuff.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {


  public static void main(String[] args) {
    //Create producer properties
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


    //Create a producer record
    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("my-topic1", "hello world");

    //send data - asynchronous
    producer.send(producerRecord);
    producer.flush();
    producer.close();
  }
}
