//package org.example.transform;
//
//import org.apache.beam.sdk.io.kafka.KafkaRecord;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.example.entity.Order;
//import org.example.transform.common.CustomDoFn;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import java.util.Objects;
//
//public class OrderDeserializer extends CustomDoFn<KafkaRecord<String, byte[]>, Order> {
//
//    private final ObjectMapper mapper;
//
//    public OrderDeserializer() {
//        this.mapper = new ObjectMapper();
//    }
//
//
//    @Override
//    protected void process(DoFn<KafkaRecord<String, byte[]>, Order>.ProcessContext c) throws Exception {
//        byte[] value = Objects.requireNonNull(c.element()).getKV().getValue();
//
//        try {
//            Order order = mapper.readValue(value, Order.class);
//            System.out.println("Order: " + order);
//            c.output(order);
//        } catch (Exception e) {
//            System.err.println("Failed to deserialize message: " + e.getMessage());
//        }
//    }
//}
