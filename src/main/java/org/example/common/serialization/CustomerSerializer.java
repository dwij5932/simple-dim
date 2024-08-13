package org.example.common.serialization;

import io.confluent.kafka.serializers.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.serialization.Serializer;
import status.customer.email.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import java.util.Map;

public class CustomerSerializer extends AbstractKafkaAvroSerializer implements Serializer<Customer> {
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> config, boolean isKey){
        this.isKey = isKey;
        this.configure(new KafkaAvroSerializerConfig(config));
    }
    @Override
    public byte[] serialize(String s, Customer customer) {
        return this.serializeImpl(this.getSubjectName(s,this.isKey, customer, AvroSchemaUtils.getSchema(customer)), customer);
    }

    @Override
    public void close(){
        Serializer.super.close();
    }
}
