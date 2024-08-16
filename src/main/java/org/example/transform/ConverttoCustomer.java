package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.example.dto.CustomerDetailsDTO;
import org.example.entity.CustomerResult;
import org.example.transform.common.CustomDoFn;
import org.example.util.Order_Status;
import org.order.status.Order;
import status.customer.email.Customer;
import java.util.Date;

public class ConverttoCustomer extends CustomDoFn<CustomerDetailsDTO, Customer> {

    @Override
    protected void process(DoFn<CustomerDetailsDTO, Customer>.ProcessContext c) throws Exception {

        Order order = c.element().getOrder();
        CustomerResult customerResult = c.element().getCustomerResult();
        Customer customer =  Customer.newBuilder()
                .setMessageId(order.getMessageId())
                .setCustomerNumber(order.getMessageId())
                .setCustomerName(customerResult.getCustomerName())
                .setCustomerEmail(customerResult.getEmail())
                .setCustomerTelephone(customerResult.getTelephone())
                .setCustomerAddress(customerResult.getAddress())
                .setOrderNumber(order.getOrderNumber())
                .setDeliveryDate(order.getDeliveryDate())
                .setDeliveryMethod(order.getDeliveryMethod())
                .setOrderStatus(Order_Status.toCustomerStatus(order.getOrderStatus()))
                .setTotalPrice(order.getTotalPrice())
                .setOrderDate(order.getOrderDate())
                .setCreatedTimestamp(order.getCreatedTimestamp())
                .setUpdatedTimestamp(new Date().toString())
                .build();
        c.output(customer);
    }
}
