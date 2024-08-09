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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.dto.CustomerDetailsDTO;
import org.example.transform.GenerateCustomerDeatils;
import org.example.transform.KafkaGenericRecordConverter;
import org.order.status.Order;
import org.order.status.Order_Status;
import org.order.status.Order_Type;
import org.example.options.RequiredAppOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.http.client.config.RequestConfig;

import javax.naming.ServiceUnavailableException;
import java.io.IOException;
import java.net.http.HttpClient;
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
                .apply("ExtractKV", ParDo.of(new KafkaGenericRecordConverter()))
                .apply("GetDetails", ParDo.of(new GenerateCustomerDeatils()))
                .apply("PrintMessage", ParDo.of(new DoFn<CustomerDetailsDTO, Void>() {
                    @ProcessElement
                    public void processElement(@Element CustomerDetailsDTO customerDetailsDTO, ProcessContext processContext){
                        System.out.println(customerDetailsDTO);
                    }
                }));
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

        return pipeline;
    }
}
