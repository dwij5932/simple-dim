package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.Deserializer;
import org.order.status.Order;
import org.order.status.Order_Status;
import org.order.status.Order_Type;
import org.example.options.RequiredAppOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Objects;

public class PipelineApplication {

    public static void main(String[] args) {

        RequiredAppOptions options = PipelineOptionsFactory.as(RequiredAppOptions.class);
        options.setRunner(FlinkRunner.class);

        setupPipeline(options).run().waitUntilFinish();
    }

    public static Pipeline setupPipeline(RequiredAppOptions options){
        Pipeline pipeline = Pipeline.create(options);
        ObjectMapper objectMapper = new ObjectMapper();

        pipeline
                .apply("ReadFromKafka",
                        KafkaIO.<String, GenericRecord>read()
                                .withBootstrapServers("localhost:9092")
                                .withTopic("dom.order.status.0")
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(
                                        ConfluentSchemaRegistryDeserializerProvider.of(
                                                "http://localhost:8081",
                                                "dom.order.status.0-value",
                                                3
                                        )
                                )
                                .withConsumerConfigUpdates(Map.of(
                                        ConsumerConfig.GROUP_ID_CONFIG, "group1"
                                        //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                                ))
                )
                .apply("ExtractKV", ParDo.of(new DoFn<KafkaRecord<String, GenericRecord>, Order>() {
                    @ProcessElement
                    public void processElement(@Element KafkaRecord<String, GenericRecord> record, OutputReceiver<Order> out) throws JsonProcessingException {
                        GenericRecord element = Objects.requireNonNull(record).getKV().getValue();
                        String content = Objects.requireNonNull(element).toString();
                        Order orderEvent = objectMapper.readValue(content, Order.class);
                        out.output(orderEvent);
                    }
                }))
                .apply("PrintMessage", ParDo.of(new DoFn<Order, Void>() {
                    @ProcessElement
                    public void processElement(@Element Order element, OutputReceiver<Void> receiver) {
                        System.out.println(element);
                    }
                }));

        return pipeline;
    }
}
