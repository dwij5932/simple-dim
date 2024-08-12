package org.example.config;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class KafkaConfiguration {

    private static KafkaConfiguration INSTANCE;

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    public static final String SCHEMA_FILE_PATH = "C:\\Users\\Dasun Theekshana\\Desktop\\KT\\demo-dom\\src\\main\\resources\\dom.order.status.0-value.avsc";

    public static synchronized KafkaConfiguration getINSTANCE() throws ExecutionException, InterruptedException, TimeoutException, IOException{
        if (INSTANCE == null){
            INSTANCE = new KafkaConfiguration();
        }
        return INSTANCE;
    }
}
