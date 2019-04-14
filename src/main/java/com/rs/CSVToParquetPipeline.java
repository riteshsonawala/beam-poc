package com.rs;

import com.rs.transforms.ConvertCsvToParquet;
import com.rs.transforms.CsvParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

public class CSVToParquetPipeline {


    public CSVToParquetPipeline() {
    }

    public static void main(String[] args) {

        CSVToParquetPipeline.Options options= PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Schema schema = null;
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(FileIO.match().filepattern(options.getInputFile()))
                .apply(FileIO.readMatches())
                .apply(ParDo.of(new CsvParser()))
                .apply(ParDo.of(new ConvertCsvToParquet(schema.toString())))
                .setCoder(AvroCoder.of(GenericRecord.class, schema)) //PCollection<GenericRecord>
                .apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to(options.getOutputPath()).withSuffix(".parquet"));

        pipeline.run().waitUntilFinish();

    }

    private interface Options extends PipelineOptions{

        @Description("Path and name of the input file")
        @Default.String("/Users/ritesh/temp/test-bulk-upload.txt")
        public String getInputFile();
        public void setInputFile(String value);

        @Description("Path to outputfile")
        @Default.String("/Users/ritesh/temp/test-bulk-upload.txt")
        public String getOutputPath();
        public void setOutputPath(String value);

    }
}
