package org.example.transform;

import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.example.dto.CustomerDetailsDTO;
import org.example.entity.CustomerResult;
import org.example.transform.common.CustomDoFn;
import org.example.util.Order_Status;
import org.order.status.Order;
import status.customer.email.Customer;
import status.enterprise.email.Enterprise;
import status.error.email.Error;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class GenerateCustomerDeatils extends CustomDoFn<KV<String,Order>, CustomerDetailsDTO> {

    CloseableHttpClient httpClient;
    RequestConfig requestConfig;
    ObjectMapper objectMapper;

    @Setup
    public void setup(){
        httpClient = HttpClients.createDefault();
        requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        objectMapper = new ObjectMapper();
    }

    @Override
    protected void process(DoFn<KV<String,Order>, CustomerDetailsDTO>.ProcessContext c) throws Exception {
        SpecificRecord order;
        String customerNo = (String)c.element().getValue().getCustomerNumber();
//        System.out.println(c.element().getKey());
//        System.out.println(customerNo);
        HttpGet httpGet = new HttpGet("http://localhost:5435/customer/"+ customerNo);
        httpGet.setConfig(requestConfig);

        try (CloseableHttpResponse response = httpClient.execute(httpGet)){
            String responseBody = EntityUtils.toString(response.getEntity());
            int status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK){
//                System.out.println("Response Body: " + responseBody);
                CustomerResult customerResult = objectMapper.readValue(responseBody, CustomerResult.class);
//                if (Objects.equals(customerResult.getSourceTable(), "Customer")){
//                    System.out.println("Customer");
//                    order = Customer.newBuilder()
//                            .setMessageId(c.element().getMessageId())
//                            .setCustomerNumber(c.element().getMessageId())
//                            .setCustomerName(customerResult.getCustomerName())
//                            .setCustomerEmail(customerResult.getEmail())
//                            .setCustomerTelephone(customerResult.getTelephone())
//                            .setCustomerAddress(customerResult.getAddress())
//                            .setOrderNumber(c.element().getOrderNumber())
//                            .setDeliveryDate(c.element().getDeliveryDate())
//                            .setDeliveryMethod(c.element().getDeliveryMethod())
//                            .setOrderStatus(Order_Status.toCustomerStatus(c.element().getOrderStatus()))
//                            .setTotalPrice(c.element().getTotalPrice())
//                            .setOrderDate(c.element().getOrderDate())
//                            .setCreatedTimestamp(c.element().getCreatedTimestamp())
//                            .setUpdatedTimestamp(new Date().toString())
//                            .build();
//                }else if (Objects.equals(customerResult.getSourceTable(), "Enterprises")){
//                    System.out.println("Enterprises");
//                    order = Enterprise.newBuilder()
//                            .setMessageId(c.element().getMessageId())
//                            .setCustomerNumber(c.element().getMessageId())
//                            .setEnterpriseName(customerResult.getCustomerName())
//                            .setEnterpriseEmail(customerResult.getEmail())
//                            .setEnterpriseTelephone(customerResult.getTelephone())
//                            .setEnterpriseAddress(customerResult.getAddress())
//                            .setOrderNumber(c.element().getOrderNumber())
//                            .setDeliveryDate(c.element().getDeliveryDate())
//                            .setDeliveryMethod(c.element().getDeliveryMethod())
//                            .setOrderStatus(Order_Status.toEnterpriseStatus(c.element().getOrderStatus()))
//                            .setTotalPrice(c.element().getTotalPrice())
//                            .setOrderDate(c.element().getOrderDate())
//                            .setCreatedTimestamp(c.element().getCreatedTimestamp())
//                            .setUpdatedTimestamp(new Date().toString())
//                            .build();
//                }else {
//                    order = Error.newBuilder()
//                            .setMessageId(c.element().getMessageId())
//                            .setCustomerNumber(c.element().getMessageId())
//                            .setOrderNumber(c.element().getOrderNumber())
//                            .setDeliveryDate(c.element().getDeliveryDate())
//                            .setDeliveryMethod(c.element().getDeliveryMethod())
//                            .setOrderStatus(Order_Status.toErrorStatus(c.element().getOrderStatus()))
//                            .setTotalPrice(c.element().getTotalPrice())
//                            .setOrderDate(c.element().getOrderDate())
//                            .setCreatedTimestamp(c.element().getCreatedTimestamp())
//                            .setUpdatedTimestamp(new Date().toString())
//                            .build();
//                }
//                System.out.println(order);
                CustomerDetailsDTO customerDetailsDTO = CustomerDetailsDTO.builder()
                        .customerResult(customerResult)
                        .order(c.element().getValue())
                        .build();
                c.output(customerDetailsDTO);
            } else if (status == HttpStatus.SC_NOT_FOUND) {
                try {
//                    System.out.println("No user Found");
                    CustomerResult customerResult = CustomerResult.builder()
                            .customerID("")
                            .customerName("")
                            .email("")
                            .telephone("")
                            .address("")
                            .sourceTable("")
                            .build();
                    CustomerDetailsDTO customerDetailsDTO = CustomerDetailsDTO.builder()
                            .customerResult(customerResult)
                            .order(c.element().getValue())
                            .build();
//                    System.out.println("CustomerDetailsDTO created: " + customerDetailsDTO);
                    c.output(customerDetailsDTO);
                } catch (Exception e) {
                    System.err.println("Error processing element: " + e.getMessage());
                    e.printStackTrace();
                }
            }else {
                System.out.println("Error");
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
