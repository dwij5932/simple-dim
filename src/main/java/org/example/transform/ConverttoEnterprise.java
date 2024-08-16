package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.example.dto.CustomerDetailsDTO;
import org.example.entity.CustomerResult;
import org.example.transform.common.CustomDoFn;
import org.example.util.Order_Status;
import org.order.status.Order;
import status.enterprise.email.Enterprise;

import java.util.Date;

public class ConverttoEnterprise extends CustomDoFn<CustomerDetailsDTO, Enterprise> {

    @Override
    protected void process(DoFn<CustomerDetailsDTO, Enterprise>.ProcessContext c) throws Exception {

        Order order = c.element().getOrder();
        CustomerResult customerResult = c.element().getCustomerResult();
        Enterprise enterprise =  Enterprise.newBuilder()
                .setMessageId(order.getMessageId())
                .setCustomerNumber(order.getMessageId())
                .setEnterpriseName(customerResult.getCustomerName())
                .setEnterpriseEmail(customerResult.getEmail())
                .setEnterpriseTelephone(customerResult.getTelephone())
                .setEnterpriseAddress(customerResult.getAddress())
                .setOrderNumber(order.getOrderNumber())
                .setDeliveryDate(order.getDeliveryDate())
                .setDeliveryMethod(order.getDeliveryMethod())
                .setOrderStatus(Order_Status.toEnterpriseStatus(order.getOrderStatus()))
                .setTotalPrice(order.getTotalPrice())
                .setOrderDate(order.getOrderDate())
                .setCreatedTimestamp(order.getCreatedTimestamp())
                .setUpdatedTimestamp(new Date().toString())
                .build();
        c.output(enterprise);
    }
}
