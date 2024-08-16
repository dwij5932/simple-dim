package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.example.dto.CustomerDetailsDTO;
import org.example.transform.common.CustomDoFn;
import org.example.util.Order_Status;
import org.order.status.Order;
import status.error.email.Error;

import java.util.Date;

public class ConverttoError extends CustomDoFn<CustomerDetailsDTO, Error> {

    @Override
    protected void process(DoFn<CustomerDetailsDTO, Error>.ProcessContext c) throws Exception {

        Order order = c.element().getOrder();
        Error error =  Error.newBuilder()
                .setMessageId(order.getMessageId())
                .setCustomerNumber(order.getMessageId())
                .setOrderNumber(order.getOrderNumber())
                .setDeliveryDate(order.getDeliveryDate())
                .setDeliveryMethod(order.getDeliveryMethod())
                .setOrderStatus(Order_Status.toErrorStatus(order.getOrderStatus()))
                .setTotalPrice(order.getTotalPrice())
                .setOrderDate(order.getOrderDate())
                .setCreatedTimestamp(order.getCreatedTimestamp())
                .setUpdatedTimestamp(new Date().toString())
                .build();
        c.output(error);
    }
}
