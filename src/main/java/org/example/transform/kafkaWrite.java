//package org.example.transform;
//
//import org.apache.avro.generic.GenericRecord;
//import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
//import org.apache.beam.sdk.io.kafka.KafkaIO;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.PTransform;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.PDone;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.example.dto.CustomerDetailsDTO;
//import org.example.transform.common.CustomDoFn;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//public class kafkaWrite extends PTransform<PCollection<CustomerDetailsDTO>, PDone> {
//
//    @Override
//    public PDone expand(PCollection<CustomerDetailsDTO> input) {
//        return input
//                .apply("PrintMessage", KafkaIO.<String, CustomerDetailsDTO>write()
//                        .withBootstrapServers("localhost:9092")
//                        .withTopic("dom.order.status.0")
//                        .withKeySerializer(StringSerializer.class))
//                        .withValueSerializer(
//                                ConfluentSchemaRegistryDeserializerProvider.of(
//                                        "http://localhost:8081",
//                                        "dom.order.status.0-value",
//                                        3
//                                )
//                        ));;
//    }
//}
