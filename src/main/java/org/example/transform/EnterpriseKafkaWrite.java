package org.example.transform;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.example.common.serialization.CustomerSerializer;
import org.example.common.serialization.EnterpriseSerializer;
import status.enterprise.email.Enterprise;

public class EnterpriseKafkaWrite extends PTransform<PCollection<Enterprise>, PDone> {
    @Override
    public PDone expand(PCollection<Enterprise> input) {
        return input
//                .apply("Convert to KV",
//                        MapElements
//                                .into(TypeDescriptor.of(KV.class))
//                                .via((Customer customer) -> KV.of(customer.getKey(), customer)) )
                .apply(
                        "writePubsubMessagesToKafka", KafkaIO.<Void, Enterprise>write()
                                .withBootstrapServers("localhost:9092")
                                .withValueSerializer(EnterpriseSerializer.class)
                                .withTopic("status.enterprise.email.0")
                                .withProducerConfigUpdates(
                                        ImmutableMap.of(
                                                "schema.registry.url","http://localhost:8081"
                                        )
                                )
                                .values()
                );
    }
}
