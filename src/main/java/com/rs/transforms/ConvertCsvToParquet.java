package com.rs.transforms;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVRecord;

import java.util.List;

public class ConvertCsvToParquet extends DoFn<CSVRecord, GenericRecord> {
    private String schemaString;

    public ConvertCsvToParquet(String schemaString) {
        this.schemaString = schemaString;
    }


    @DoFn.ProcessElement
    public void processElement(@Element CSVRecord element, DoFn.OutputReceiver<GenericRecord> receiver) {

        Schema schema = new Schema.Parser().parse(this.schemaString);
        GenericRecord genericRecord = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();

        for (Schema.Field field : fields) {
            String fieldType = field.schema().getType().getName().toLowerCase();
            String fieldName=field.name();
            String value=element.get(fieldName.replaceAll("_", " "));
            switch (fieldType) {
                case "string":
                    genericRecord.put(fieldName, value);
                    break;
                case "int":
                    genericRecord.put(fieldName, Integer.valueOf(value));
                    break;
                case "long":
                    genericRecord.put(fieldName, Long.valueOf(value));
                    break;
                default:
                    throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
        receiver.output(genericRecord);
    }

}
