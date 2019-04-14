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
            switch (fieldType) {
                case "string":
                    genericRecord.put(field.name(), element.get(field.name().toUpperCase()));
                    break;
                case "int":
                    genericRecord.put(field.name(), Integer.valueOf(element.get(field.name().toUpperCase())));
                    break;
                case "long":
                    genericRecord.put(field.name(), Long.valueOf(element.get(field.name().toUpperCase())));
                    break;
                default:
                    throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
        receiver.output(genericRecord);
    }

}
