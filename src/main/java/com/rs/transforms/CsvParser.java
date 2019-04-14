package com.rs.transforms;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

public class CsvParser extends DoFn<FileIO.ReadableFile, CSVRecord> {

    @DoFn.ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, DoFn.OutputReceiver<CSVRecord> receiver) throws IOException {
        InputStream is = Channels.newInputStream(element.open());
        Reader reader = new InputStreamReader(is);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter(',').withFirstRecordAsHeader().parse(reader);

        for (CSVRecord record : records) { receiver.output(record); }
    }

}
