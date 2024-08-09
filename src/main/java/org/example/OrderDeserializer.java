//package org.example.demodip;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.common.serialization.Deserializer;
//import org.example.demodip.entity.Order;
//
//import java.util.Map;
//
//public class OrderDeserializer implements Deserializer<GenericRecord> {
//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//    @Override
//    public void configure(Map<String, ?> configs, boolean isKey) {
//        // Configuration code, if necessary
//    }
//
//    @Override
//    public Order deserialize(String topic, byte[] data) {
//        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            Order order = objectMapper.readValue(data, Order.class);
//            return order;
//            //return objectMapper.readValue(json, Order.class);
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to deserialize Order", e);
//        }
//    }
//
//    @Override
//    public void close() {
//        // Cleanup code, if necessary
//    }
//}
//
