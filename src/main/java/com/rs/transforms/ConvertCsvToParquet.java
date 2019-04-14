package com.rs.transforms;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVRecord;

public class ConvertCsvToParquet extends DoFn<CSVRecord, GenericRecord> {
    public ConvertCsvToParquet(Object toString) {
    }
}
