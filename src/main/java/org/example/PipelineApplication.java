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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.dto.CustomerDetailsDTO;
import org.example.entity.CustomerResult;
import org.example.transform.*;
import org.joda.time.Duration;
import org.order.status.Order;
import org.order.status.Order_Type;
import status.customer.email.Customer;
import status.enterprise.email.Enterprise;
import status.error.email.Error;
import org.example.options.RequiredAppOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.http.client.config.RequestConfig;
import org.example.util.Order_Status;
import javax.naming.ServiceUnavailableException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.joda.time.format.DateTimeFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.time.LocalDateTime;

public class PipelineApplication {

    public static void main(String[] args) {

        RequiredAppOptions options = PipelineOptionsFactory.as(RequiredAppOptions.class);
        options.setRunner(FlinkRunner.class);

        setupPipeline(options).run().waitUntilFinish();
    }

    public static Pipeline setupPipeline(RequiredAppOptions options){
        Pipeline pipeline = Pipeline.create(options);

        final TupleTag<CustomerDetailsDTO> sendToCustomer = new TupleTag<CustomerDetailsDTO>() {
        };
        final TupleTag<CustomerDetailsDTO> sendToEnterprise = new TupleTag<CustomerDetailsDTO>() {
        };
        final TupleTag<CustomerDetailsDTO> sendToError = new TupleTag<CustomerDetailsDTO>() {
        };

        PCollectionTuple mixedCollection = pipeline
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
                .apply("ExtractKV", ParDo.of(new KafkaGenericRecordConverter()))
                .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply("Grouping", GroupByKey.create())
                .apply("ExtractLatestUpdate", ParDo.of(new ExtractLatestOrder()))
                .apply("Get Details", ParDo.of(new GenerateCustomerDeatils()))
                .apply("Branching Orders", ParDo.of(new BranchingOrders(sendToCustomer,sendToEnterprise,sendToError)
                        ).withOutputTags(sendToCustomer,
                        TupleTagList.of(sendToEnterprise).and(sendToError)));

        mixedCollection.get(sendToCustomer)
                .apply("Convert to Customer", ParDo.of(new ConverttoCustomer()
                ))
                        .apply("Write To Kafka", new CustomerKafkaWrite());

        mixedCollection.get(sendToEnterprise)
                .apply("Convert to Enterprise", ParDo.of(new ConverttoEnterprise()
                ))
                .apply("Write To Kafka", new EnterpriseKafkaWrite());

        mixedCollection.get(sendToError)
                .apply("Convert to Error", ParDo.of(new ConverttoError()
                ))
                .apply("Write To Kafka", new ErrorKafkaWrite());

        return pipeline;
    }
}
