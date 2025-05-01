package com.mercari.solution.util.pipeline.select;

import com.mercari.solution.module.Schema;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CurrentTimestamp implements SelectFunction {

    private final String name;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    CurrentTimestamp(String name, boolean ignore) {
        this.name = name;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.TIMESTAMP.withNullable(true);
        this.ignore = ignore;
    }

    public static CurrentTimestamp of(String name, boolean ignore) {
        return new CurrentTimestamp(name, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return Instant.now().getMillis() * 1000L;
    }

}
