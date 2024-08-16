package org.example.transform;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.transform.common.CustomDoFn;
import org.joda.time.Instant;
import org.order.status.Order;
import javax.xml.crypto.Data;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Objects;

public class KafkaGenericRecordConverter extends CustomDoFn<KafkaRecord<String, GenericRecord>, KV<String,Order>>{

    ObjectMapper mapper;

    @Setup
    public void steup(){
        mapper = new ObjectMapper();
    }

    @Override
    protected void process(DoFn<KafkaRecord<String, GenericRecord>, KV<String,Order>>.ProcessContext c) throws Exception {
        GenericRecord element = Objects.requireNonNull(c.element()).getKV().getValue();
        String content = Objects.requireNonNull(element).toString();
        Order order = mapper.readValue(content, Order.class);

        String orderNumber = order.getCustomerNumber().toString();
        System.out.println(orderNumber);
        c.output(KV.of(orderNumber,order));

    }
}
