package org.example.common.serialization;

import io.confluent.kafka.serializers.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.serialization.Serializer;
import status.enterprise.email.Enterprise;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import java.util.Map;

public class EnterpriseSerializer extends AbstractKafkaAvroSerializer implements Serializer<Enterprise> {
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> config, boolean isKey){
        this.isKey = isKey;
        this.configure(new KafkaAvroSerializerConfig(config));
    }
    @Override
    public byte[] serialize(String s, Enterprise enterprise) {
        return this.serializeImpl(this.getSubjectName(s,this.isKey, enterprise, AvroSchemaUtils.getSchema(enterprise)), enterprise);
    }

    @Override
    public void close(){
        Serializer.super.close();
    }
}
