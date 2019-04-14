package com.rs;

import com.rs.transforms.ConvertCsvToParquet;
import com.rs.transforms.CsvParser;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CSVToParquetPipeline {


    public CSVToParquetPipeline() {
    }

    public static void main(String[] args) throws IOException {

        CSVToParquetPipeline.Options options= PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Path inputFilePath = Paths.get(options.getInputFile());
        List<String> headers = fetchHeaders(inputFilePath);

        Schema schema = getAvroSchemaForHeaders("com.rs", inputFilePath.getFileName().toString(), headers);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(FileIO.match().filepattern(options.getInputFile()))
                .apply(FileIO.readMatches())
                .apply(ParDo.of(new CsvParser()))
                .apply(ParDo.of(new ConvertCsvToParquet(schema.toString())))
                .setCoder(AvroCoder.of(GenericRecord.class, schema)) //PCollection<GenericRecord>
                .apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to(options.getOutputPath()).withPrefix(inputFilePath.getFileName().toString()).
                         withSuffix(".parquet").withNumShards(1)
                );

        pipeline.run().waitUntilFinish();

    }

    private static List<String> fetchHeaders(Path inputFilePath) throws IOException {
        String[] headers = Files.lines(inputFilePath)
                .map(s -> s.split(","))
                .findFirst().get();
        return Arrays.stream(headers).collect(Collectors.toList());
    }

    private interface Options extends PipelineOptions{

        @Description("Path and name of the input file")
        @Default.String("/Users/ritesh/temp/test-bulk-upload.txt")
        public String getInputFile();
        public void setInputFile(String value);

        @Description("Path to outputfile")
        @Default.String("/Users/ritesh/temp/")
        public String getOutputPath();
        public void setOutputPath(String value);

    }

    public static Schema getAvroSchemaForHeaders(String namespace, String entityName,  List<String> headers){

        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(entityName).namespace(namespace);
        SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();
        for (String header : headers) {
            fields.requiredString(header.replaceAll(" ", "_"));
        }
        return fields.endRecord();
    }
}
