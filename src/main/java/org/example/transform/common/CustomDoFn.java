package org.example.transform.common;

import org.apache.beam.sdk.transforms.DoFn;

public abstract class CustomDoFn<I, O> extends DoFn<I, O> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception{
        try {
            process(c);
        } catch (Exception e){
            System.out.println(e);
        }
    }

    protected abstract void process(DoFn<I, O>.ProcessContext c) throws Exception;
}
