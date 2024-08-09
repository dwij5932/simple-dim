package org.example.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CustomerResult {

    @JsonProperty("customerID")
    private String customerID;

    @JsonProperty("customerName")
    private String customerName;

    @JsonProperty("email")
    private String email;

    @JsonProperty("telephone")
    private String telephone;

    @JsonProperty("address")
    private String address;

    @JsonProperty("sourceTable")
    private String sourceTable;
}
