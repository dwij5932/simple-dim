package org.example.entity;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Customer {

    private String customerID;

    private String customerName;

    private String email;

    private String telephone;

    private String address;

}
