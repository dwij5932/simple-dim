//package org.example.transform;
//
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.PDone;
//import org.example.entity.Order;
//import org.apache.beam.sdk.transforms.PTransform;
//
//public class PrintOrder extends PTransform<PCollection<Order>, PDone> {
//    public static class PrintOrderFn extends DoFn<Order, Void> {
//        @ProcessElement
//        public void processElement(ProcessContext c) {
//            Order order = c.element();
//            System.out.println(order);
//        }
//    }
//
//    @Override
//    public PDone expand(PCollection<Order> input) {
//        input.apply(ParDo.of(new PrintOrderFn()));
//        return PDone.in(input.getPipeline());
//    }
//}
