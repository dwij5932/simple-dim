package org.example.dto;

import lombok.*;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.example.entity.CustomerResult;
import org.order.status.Order;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@DefaultCoder(AvroCoder.class)
public class CustomerDetailsDTO {

    private Order order;

    private CustomerResult customerResult;
}
