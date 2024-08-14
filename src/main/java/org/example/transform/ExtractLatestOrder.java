package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.transform.common.CustomDoFn;
import org.order.status.Order;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ExtractLatestOrder extends CustomDoFn<KV<String,Iterable<Order>>,KV<String,Order>> {

    DateTimeFormatter formatter;

    @Setup
    public void setup(){
        formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy");
    }

    @Override
    protected void process(ProcessContext context) throws Exception {
        KV<String,Iterable<Order>> element = context.element();
        Iterable<Order> updates = element.getValue();
//                        System.out.println("After Group By");
//                        System.out.println(context);
        Order lastesrUpdate = null;
        long latestTimestamp = context.timestamp().getMillis();

        for (Order update: updates){
            ZonedDateTime dateTime = ZonedDateTime.parse(update.getCreatedTimestamp(),  formatter.withZone(ZoneId.of("Asia/Kolkata")));
            long eventTimestamp = dateTime.toInstant().toEpochMilli();
//                            Instant eventTimestamp = dateTime.toInstant();
            System.out.println(latestTimestamp+" "+ eventTimestamp+" - "+update);
            if (lastesrUpdate == null || eventTimestamp > latestTimestamp){
                lastesrUpdate = update;
                latestTimestamp = eventTimestamp;
            }
        }
        if (lastesrUpdate != null){
            context.output(KV.of("f",lastesrUpdate));
        }
    }
}
