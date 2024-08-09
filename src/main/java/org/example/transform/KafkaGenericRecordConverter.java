package org.example.transform;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.transform.common.CustomDoFn;
import org.order.status.Order;

import java.util.Objects;

public class KafkaGenericRecordConverter extends CustomDoFn<KafkaRecord<String, GenericRecord>, Order>{

    ObjectMapper mapper;

    @Setup
    public void steup(){
        mapper = new ObjectMapper();
    }

    @Override
    protected void process(DoFn<KafkaRecord<String, GenericRecord>, Order>.ProcessContext c) throws Exception {
        GenericRecord element = Objects.requireNonNull(c.element()).getKV().getValue();
        String content = Objects.requireNonNull(element).toString();
        Order order = mapper.readValue(content, Order.class);
        c.output(order);

    }
}
