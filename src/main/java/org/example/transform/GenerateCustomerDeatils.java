package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.example.transform.common.CustomDoFn;
import org.order.status.Order;

import java.io.IOException;

public class GenerateCustomerDeatils extends CustomDoFn<Order, Void> {

    CloseableHttpClient httpClient;
    RequestConfig requestConfig;

    @Setup
    public void setup(){
        httpClient = HttpClients.createDefault();
        requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
    }

    @Override
    protected void process(DoFn<Order, Void>.ProcessContext c) throws Exception {
        String customerNo = (String)c.element().getCustomerNumber();
        System.out.println(customerNo);
        HttpGet httpGet = new HttpGet("http://localhost:5435/customer/"+ customerNo);
        httpGet.setConfig(requestConfig);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)){
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println("Response Status: " + response.getStatusLine());
            System.out.println("Response Body: " + responseBody);
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
