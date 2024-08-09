package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        // Define the properties for the Kafka consumer
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // To start reading from the beginning of the topic

        // Create a new Kafka consumer
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList("dom.order.status.0"));

            // Poll for new data
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, byte[]> record : records) {
                    // Print the key and value of each record
                    String key = record.key();
                    byte[] value = record.value();
                    System.out.printf("Key: %s, Value: %s%n", key, new String(value));
                }
            }
        }
    }
}
//
//package org.example;
//
//import io.confluent.kafka.serializers.KafkaAvroDeserializer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//
//public class KafkaConsumerExample {
//
//    public static void main(String[] args) {
//        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
//        properties.put("schema.registry.url", "http://localhost:8081"); // Replace with your Schema Registry URL
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        // Create Kafka consumer
//        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties);
//        consumer.subscribe(Collections.singletonList("dom.order.status.0")); // Replace with your topic name
//
//        try {
//            while (true) {
//                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
//                for (ConsumerRecord<String, Object> record : records) {
//                    System.out.println("Key: " + record.key());
//                    System.out.println("Value: " + record.value()); // The value will be deserialized Avro object
//                    System.out.println("Partition: " + record.partition());
//                    System.out.println("Offset: " + record.offset());
//                }
//            }
//        } finally {
//            consumer.close();
//        }
//    }
//}

