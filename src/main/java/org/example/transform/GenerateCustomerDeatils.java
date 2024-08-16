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
        HttpGet httpGet = new HttpGet("http://localhost:5435/customer/"+ customerNo);
        httpGet.setConfig(requestConfig);

        try (CloseableHttpResponse response = httpClient.execute(httpGet)){
            String responseBody = EntityUtils.toString(response.getEntity());
            int status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK){
                CustomerResult customerResult = objectMapper.readValue(responseBody, CustomerResult.class);
                CustomerDetailsDTO customerDetailsDTO = CustomerDetailsDTO.builder()
                        .customerResult(customerResult)
                        .order(c.element().getValue())
                        .build();
                c.output(customerDetailsDTO);
            } else if (status == HttpStatus.SC_NOT_FOUND) {
                try {
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
