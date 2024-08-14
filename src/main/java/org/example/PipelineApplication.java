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
//import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
//import org.joda.time.Duration;
//import java.time.Instant;
//import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Duration;
import org.order.status.Order;
//import org.order.status.Order_Status;
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
                .apply("Process Customer Details", ParDo.of(new DoFn<CustomerDetailsDTO, CustomerDetailsDTO>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        CustomerDetailsDTO customerResult = c.element();
                        System.out.println(customerResult);
                        if (Objects.equals(customerResult.getCustomerResult().getSourceTable(), "Customer")) {
                            c.output(sendToCustomer, customerResult);
                        } else if (Objects.equals(customerResult.getCustomerResult().getSourceTable(), "Enterprise")) {
                            c.output(sendToEnterprise, customerResult);
                        } else {
//                            System.out.println("Send to Error Branch");
                            c.output(sendToError, customerResult);
                        }
                    }
                }).withOutputTags(sendToCustomer,
                        TupleTagList.of(sendToEnterprise).and(sendToError)));

        mixedCollection.get(sendToCustomer)
                .apply("Convert to Customer", ParDo.of(new DoFn<CustomerDetailsDTO, Customer>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        Order order = c.element().getOrder();
                        CustomerResult customerResult = c.element().getCustomerResult();
                        Customer customer =  Customer.newBuilder()
                            .setMessageId(order.getMessageId())
                            .setCustomerNumber(order.getMessageId())
                            .setCustomerName(customerResult.getCustomerName())
                            .setCustomerEmail(customerResult.getEmail())
                            .setCustomerTelephone(customerResult.getTelephone())
                            .setCustomerAddress(customerResult.getAddress())
                            .setOrderNumber(order.getOrderNumber())
                            .setDeliveryDate(order.getDeliveryDate())
                            .setDeliveryMethod(order.getDeliveryMethod())
                            .setOrderStatus(Order_Status.toCustomerStatus(order.getOrderStatus()))
                            .setTotalPrice(order.getTotalPrice())
                            .setOrderDate(order.getOrderDate())
                            .setCreatedTimestamp(order.getCreatedTimestamp())
                            .setUpdatedTimestamp(new Date().toString())
                            .build();
//                        System.out.println(customer);
                        c.output(customer);
                    }
                }))
                        .apply("Write To Kafka", new CustomerKafkaWrite());

        mixedCollection.get(sendToEnterprise)
                .apply("Convert to Enterprise", ParDo.of(new DoFn<CustomerDetailsDTO, Enterprise>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        Order order = c.element().getOrder();
                        CustomerResult customerResult = c.element().getCustomerResult();
                        Enterprise enterprise =  Enterprise.newBuilder()
                                .setMessageId(order.getMessageId())
                                .setCustomerNumber(order.getMessageId())
                                .setEnterpriseName(customerResult.getCustomerName())
                                .setEnterpriseEmail(customerResult.getEmail())
                                .setEnterpriseTelephone(customerResult.getTelephone())
                                .setEnterpriseAddress(customerResult.getAddress())
                                .setOrderNumber(order.getOrderNumber())
                                .setDeliveryDate(order.getDeliveryDate())
                                .setDeliveryMethod(order.getDeliveryMethod())
                                .setOrderStatus(Order_Status.toEnterpriseStatus(order.getOrderStatus()))
                                .setTotalPrice(order.getTotalPrice())
                                .setOrderDate(order.getOrderDate())
                                .setCreatedTimestamp(order.getCreatedTimestamp())
                                .setUpdatedTimestamp(new Date().toString())
                                .build();
                        c.output(enterprise);
                    }
                }))
                .apply("Write To Kafka", new EnterpriseKafkaWrite());

        mixedCollection.get(sendToError)
                .apply("Convert to Error", ParDo.of(new DoFn<CustomerDetailsDTO, Error>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        Order order = c.element().getOrder();
                        Error error =  Error.newBuilder()
                                .setMessageId(order.getMessageId())
                                .setCustomerNumber(order.getMessageId())
                                .setOrderNumber(order.getOrderNumber())
                                .setDeliveryDate(order.getDeliveryDate())
                                .setDeliveryMethod(order.getDeliveryMethod())
                                .setOrderStatus(Order_Status.toErrorStatus(order.getOrderStatus()))
                                .setTotalPrice(order.getTotalPrice())
                                .setOrderDate(order.getOrderDate())
                                .setCreatedTimestamp(order.getCreatedTimestamp())
                                .setUpdatedTimestamp(new Date().toString())
                                .build();
                        c.output(error);
                    }
                }))
                .apply("Write To Kafka", new ErrorKafkaWrite());

        return pipeline;
    }
}
