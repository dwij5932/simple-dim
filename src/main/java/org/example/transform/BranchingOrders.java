package org.example.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.example.dto.CustomerDetailsDTO;
import org.example.transform.common.CustomDoFn;

import java.util.Objects;

public class BranchingOrders extends CustomDoFn<CustomerDetailsDTO, CustomerDetailsDTO> {
    private final TupleTag<CustomerDetailsDTO> sendToCustomer ;
    final TupleTag<CustomerDetailsDTO> sendToEnterprise ;
    final TupleTag<CustomerDetailsDTO> sendToError;

    public BranchingOrders(TupleTag<CustomerDetailsDTO> sendToCustomer, TupleTag<CustomerDetailsDTO> sendToEnterprise, TupleTag<CustomerDetailsDTO> sendToError) {
        this.sendToCustomer = sendToCustomer;
        this.sendToEnterprise = sendToEnterprise;
        this.sendToError = sendToError;
    }

    @Override
    protected void process(DoFn<CustomerDetailsDTO, CustomerDetailsDTO>.ProcessContext c) throws Exception {
        CustomerDetailsDTO customerResult = c.element();
        System.out.println(customerResult);
        if (Objects.equals(customerResult.getCustomerResult().getSourceTable(), "Customer")) {
            c.output(sendToCustomer, customerResult);
        } else if (Objects.equals(customerResult.getCustomerResult().getSourceTable(), "Enterprise")) {
            c.output(sendToEnterprise, customerResult);
        } else {
            c.output(sendToError, customerResult);
        }
    }
}
