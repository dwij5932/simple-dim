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

    @Override
    public PDone expand(PCollection<Customer> input) {

        return input
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