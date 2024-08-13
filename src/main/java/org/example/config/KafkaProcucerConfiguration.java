package org.example.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.order.status.Order;

import java.util.Properties;

import static org.example.config.KafkaConfiguration.*;

public class KafkaProcucerConfiguration {

    private static KafkaConfiguration kafkaConfiguration;

    public static synchronized KafkaProducer<String, Order> getConfiguredKafkaProcucer() throws InstantiationError{
        try {
            kafkaConfiguration = KafkaConfiguration.getINSTANCE();

            Properties properties = new Properties();
            properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
            properties.put("schema.registry.url", "http://localhost:8081");

            return new KafkaProducer<>(properties);

        } catch (Exception e){
            throw new IndexOutOfBoundsException(e.getMessage());
        }
    }


}
