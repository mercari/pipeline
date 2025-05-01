package com.mercari.solution.util.schema.converter;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

public class TextToRowConverter {

    public static final Schema DEFAULT_SCHEMA = Schema.builder().addStringField("text").build();

    public static Row convert(final String text) {
        final Row.Builder builder = Row.withSchema(DEFAULT_SCHEMA);
        builder.addValue(text);
        return builder.build();
    }

}
