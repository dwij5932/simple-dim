package org.example.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EnterpriseC {

    @JsonProperty("customerID")
    private String customerID;

    @JsonProperty("enterpriseName")
    private String enterpriseName;

    private String email;

    private String telephone;

    private String address;

}
