package org.example.demodip;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.demodip.options.RequiredAppOptions;

public class PipelineApplication {

    public static void main(String[] args) {
//        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
//        options.setRunner(FlinkRunner.class);
        RequiredAppOptions options = PipelineOptionsFactory.as(RequiredAppOptions.class);
        options.setRunner(FlinkRunner.class);

//        Pipeline p = Pipeline.create(options);
//
//        p.apply("Read Data", Create.of("Hello", "Beam", "Pipeline"))
//                .apply("Transform Data", MapElements.into(TypeDescriptors.strings())
//                        .via((String word) -> "Processed: " + word))
//                .apply("Calculate Length", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void calculateLength(@Element String word, OutputReceiver<String> out){
//                        int len = word.length();
//                        String result = word + "Length: " + len;
//                        out.output(result);
//                    }
//                }))
//                .apply("Print Data", ParDo.of(new DoFn<String, Void>() {
//                    @ProcessElement
//                    public void processElement(@Element String word, OutputReceiver<Void> out) {
//                        System.out.println(word);
//                    }
//                }));

        // Run the pipeline
//        p.run().waitUntilFinish();
        setupPipeline(options).run().waitUntilFinish();
    }

    public static Pipeline setupPipeline(RequiredAppOptions options){
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read Data", Create.of("Hello", "Beam", "Pipeline"))
                .apply("Transform Data", MapElements.into(TypeDescriptors.strings())
                        .via((String word) -> "Processed: " + word))
                .apply("Calculate Length", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void calculateLength(@Element String word, OutputReceiver<String> out){
                        int len = word.length();
                        String result = word + "Length: " + len;
                        out.output(result);
                    }
                }))
                .apply("Print Data", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String word, OutputReceiver<Void> out) {
                        System.out.println(word);
                    }
                }));

        return pipeline;
    }
}
