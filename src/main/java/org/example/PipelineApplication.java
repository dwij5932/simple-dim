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
//                .apply("ExtractLatestUpdate", ParDo.of(new DoFn<KV<String,Iterable<Order>>,KV<String,Order>>(){
//
//                    DateTimeFormatter formatter;
//
//                    @Setup
//                    public void setup(){
//                        formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy");
//                    }
//                    @ProcessElement
//                    public void processElement(ProcessContext context){
//                        KV<String,Iterable<Order>> element = context.element();
//                        Iterable<Order> updates = element.getValue();
////                        System.out.println("After Group By");
////                        System.out.println(context);
//                        Order lastesrUpdate = null;
//                        long latestTimestamp = context.timestamp().getMillis();
//
//                        for (Order update: updates){
//                            ZonedDateTime dateTime = ZonedDateTime.parse(update.getCreatedTimestamp(),  formatter.withZone(ZoneId.of("Asia/Kolkata")));
//                            long eventTimestamp = dateTime.toInstant().toEpochMilli();
////                            Instant eventTimestamp = dateTime.toInstant();
//                            System.out.println(latestTimestamp+" "+ eventTimestamp+" - "+update);
//                            if (lastesrUpdate == null || eventTimestamp > latestTimestamp){
//                                lastesrUpdate = update;
//                                latestTimestamp = eventTimestamp;
//                            }
//                        }
//                        if (lastesrUpdate != null){
//                            context.output(KV.of("f",lastesrUpdate));
//                        }
//                    }
//                }))
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
//                .apply("PrintMessage", ParDo.of(new DoFn<CustomerDetailsDTO, Void>() {
//                    @ProcessElement
//                    public void processElement(@Element CustomerDetailsDTO customerDetailsDTO, ProcessContext processContext){
//                        System.out.println(customerDetailsDTO);
//                    }
//                }));
//                .apply("PrintMessage", ParDo.of(new DoFn<Order, Void>() {
//                    CloseableHttpClient httpClient;
//                    RequestConfig requestConfig;
//
//                    @Setup
//                    public void setup(){
//                        httpClient = HttpClients.createDefault();
//                        requestConfig = RequestConfig.custom()
//                                .setConnectTimeout(5000)
//                                .setSocketTimeout(5000)
//                                .build();
//                    }
//
//                    @ProcessElement
//                    public void processElement(@Element Order element, ProcessContext receiver) {
//                        System.out.println(element);
//                        String customerNo = (String)element.getCustomerNumber();
//                        System.out.println(customerNo);
//                        HttpGet httpGet = new HttpGet("http://localhost:5435/customer/"+ customerNo);
//                        httpGet.setConfig(requestConfig);
//                        try (CloseableHttpResponse response = httpClient.execute(httpGet)){
//                            String responseBody = EntityUtils.toString(response.getEntity());
//
//                            // Print the response
//                            System.out.println("Response Status: " + response.getStatusLine());
//                            System.out.println("Response Body: " + responseBody);
//                        } catch (IOException e) {
//                            System.out.println(e);
//                        }
//                    }
//                }));
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
//                .apply("PrintMessage", ParDo.of(new DoFn<Customer, Void>() {
//                    @ProcessElement
//                    public void processElement(@Element Customer customer, ProcessContext processContext){
//                        System.out.println("Customer branch");
//                        System.out.println(customer);
//                    }
//                }));
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
//                .apply("PrintMessage", ParDo.of(new DoFn<Enterprise, Void>() {
//                    @ProcessElement
//                    public void processElement(@Element Enterprise enterprise, ProcessContext processContext){
//                        System.out.println("Enterprise branch");
//                        System.out.println(enterprise);
//                    }
//                }));
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
//                .apply("PrintMessage", ParDo.of(new DoFn<Error, Void>() {
//                    @ProcessElement
//                    public void processElement(@Element Error error, ProcessContext processContext){
//                        System.out.println("Error branch");
//                        System.out.println(error);
//                    }
//                }));

        return pipeline;
    }
}
