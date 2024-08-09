//package org.example.demodip.transform;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.example.demodip.entity.Order;
//
//static class LatestOrderUpdateFn extends DoFn<Order, Order> {
//    private transient ValueState<Order> latestOrderState;
//
//    @Override
//    public void open(org.apache.beam.sdk.options.PipelineOptions options) {
//        ValueStateDescriptor<Order> descriptor = new ValueStateDescriptor<>(
//                "latestOrderState",
//                Order.class
//        );
//        latestOrderState = getRuntimeContext().getState(descriptor);
//    }
//
//    @DoFn.ProcessElement
//    public void processElement(ProcessContext context) throws Exception {
//        Order order = context.element();
//        Order currentLatestOrder = latestOrderState.value();
//
//        if (currentLatestOrder == null || order.getCreated_timestamp() > currentLatestOrder.getCreated_timestamp()) {
//            latestOrderState.update(order);
//
//            // Register a timer to clear the state after 1 minute
//            context.timerService().registerEventTimeTimer(order.getTimestamp() + 60000);
//        }
//    }
//
//    @OnTimer
//    public void onTimer(OnTimerContext ctx, OutputReceiver<Order> out) throws Exception {
//        Order latestOrder = latestOrderState.value();
//
//        if (latestOrder != null && latestOrder.getTimestamp() + 60000 <= ctx.timestamp()) {
//            // Output the latest order
//            out.output(latestOrder);
//
//            // Clear the state
//            latestOrderState.clear();
//        }
//    }
//}