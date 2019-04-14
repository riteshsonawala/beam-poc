package com.rs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class CSVToParquetPipeline {


    public static void main(String[] args) {

        CSVToParquetPipeline.Options options= PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.run().waitUntilFinish();

    }

    private interface Options extends PipelineOptions{



    }
}
