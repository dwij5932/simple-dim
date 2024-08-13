package org.example.transform;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.example.common.serialization.ErrorSerializer;
import status.error.email.Error;

public class ErrorKafkaWrite extends PTransform<PCollection<Error>, PDone> {
    @Override
    public PDone expand(PCollection<Error> input) {
        return input
//                .apply("Convert to KV",
//                        MapElements
//                                .into(TypeDescriptor.of(KV.class))
//                                .via((Customer customer) -> KV.of(customer.getKey(), customer)) )
                .apply(
                        "writePubsubMessagesToKafka", KafkaIO.<Void, Error>write()
                                .withBootstrapServers("localhost:9092")
                                .withValueSerializer(ErrorSerializer.class)
                                .withTopic("status.error.email.0")
                                .withProducerConfigUpdates(
                                        ImmutableMap.of(
                                                "schema.registry.url","http://localhost:8081"
                                        )
                                )
                                .values()
                );
    }
}
