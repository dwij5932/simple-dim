package org.example.transform;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.common.serialization.CustomerSerializer;
import org.example.config.KafkaConfiguration;
import status.customer.email.Customer;
import java.util.Map;
import java.util.Properties;

import static org.example.config.KafkaConfiguration.*;
import static org.example.config.KafkaConfiguration.VALUE_SERIALIZER_CLASS;

public class CustomerKafkaWrite extends PTransform<PCollection<Customer>, PDone> {
//    private static KafkaConfiguration kafkaConfiguration;
//    private static KafkaProducer<Customer> kafkaProducer;
 //   ObjectMapper objectMapper;
//
//    @DoFn.Setup
//    public void steup() {
//        kafkaConfiguration = KafkaConfiguration.getINSTANCE();
//        Properties properties = new Properties();
//        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        properties.put(KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
//        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
//        properties.put("schema.registry.url", "http://localhost:8081");
//        kafkaProducer(properties);
//    }
//    @DoFn.Setup
//    public void steup(){
//        objectMapper = new ObjectMapper();
//    }

    @Override
    public PDone expand(PCollection<Customer> input) {

        return input
//                .apply("Convert to KV",
//                        MapElements
//                                .into(TypeDescriptor.of(KV.class))
//                                .via((Customer customer) -> KV.of(customer.getKey(), customer)) )
                .apply(
                        "writePubsubMessagesToKafka", KafkaIO.<Void, Customer>write()
                                .withBootstrapServers("localhost:9092")
                                .withValueSerializer(CustomerSerializer.class)
                                .withTopic("status.customer.email.0")
                                .withProducerConfigUpdates(
                                        ImmutableMap.of(
                                                "schema.registry.url","http://localhost:8081"
                                        )
                                )
                                .values()
                                );
    }
}