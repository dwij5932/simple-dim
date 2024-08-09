package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
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
import org.order.status.Order;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

public class GenerateCustomerDeatils extends CustomDoFn<Order, CustomerDetailsDTO> {

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
    protected void process(DoFn<Order, CustomerDetailsDTO>.ProcessContext c) throws Exception {
        String customerNo = (String)c.element().getCustomerNumber();
        System.out.println(customerNo);
        HttpGet httpGet = new HttpGet("http://localhost:5435/customer/"+ customerNo);
        httpGet.setConfig(requestConfig);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)){
            String responseBody = EntityUtils.toString(response.getEntity());
            int status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK){
                System.out.println("Response Body: " + responseBody);
                CustomerResult customerResult = objectMapper.readValue(responseBody, CustomerResult.class);
                if (Objects.equals(customerResult.getSourceTable(), "Customer")){
                    System.out.println("Customer");
                }else{
                    System.out.println("Enterprises");
                }
                CustomerDetailsDTO customerDetailsDTO = CustomerDetailsDTO.builder()
                        .customerResult(customerResult)
                        .order(c.element())
                        .build();
                c.output(customerDetailsDTO);
            } else if (status == HttpStatus.SC_NOT_FOUND) {
                System.out.println("No user Found");
                c.output(new CustomerDetailsDTO());
            }else {
                System.out.println("Error");
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
