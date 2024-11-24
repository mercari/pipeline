package com.mercari.solution.util.schema.converter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class TextToAvroConverter {

    public static final Schema DEFAULT_SCHEMA = SchemaBuilder
            .record("record").fields()
            .optionalString("text")
            .endRecord();

    public static GenericRecord convert(final String text) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(DEFAULT_SCHEMA);
        builder.set("text", text);
        return builder.build();
    }

}
