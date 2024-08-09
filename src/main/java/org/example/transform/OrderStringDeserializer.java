//package org.example.demodip.transform;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.common.serialization.Deserializer;
//import org.example.demodip.entity.Order;
//
//import java.util.Map;
//
//public class OrderStringDeserializer implements Deserializer<Order> {
//    private ObjectMapper objectMapper = new ObjectMapper();
//
//    @Override
//    public void configure(Map<String, ?> configs, boolean isKey) {
//    }
//
//    @Override
//    public Order deserialize(String topic, byte[] data) {
//        try {
//            System.out.println("Converted");
//            return objectMapper.readValue(data, Order.class);
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to deserialize Order", e);
//        }
//    }
//
//    @Override
//    public void close() {
//    }
//}
//
//
