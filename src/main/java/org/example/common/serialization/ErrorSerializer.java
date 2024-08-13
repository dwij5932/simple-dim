package org.example.common.serialization;

import io.confluent.kafka.serializers.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.serialization.Serializer;
import status.error.email.Error;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import java.util.Map;

public class ErrorSerializer extends AbstractKafkaAvroSerializer implements Serializer<Error> {
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> config, boolean isKey){
        this.isKey = isKey;
        this.configure(new KafkaAvroSerializerConfig(config));
    }
    @Override
    public byte[] serialize(String s, Error error) {
        return this.serializeImpl(this.getSubjectName(s,this.isKey, error, AvroSchemaUtils.getSchema(error)), error);
    }

    @Override
    public void close(){
        Serializer.super.close();
    }
}
