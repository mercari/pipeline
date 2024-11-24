package com.mercari.solution.util.pipeline.select;

import com.mercari.solution.module.Schema;
import org.joda.time.Instant;

import java.util.List;
import java.util.Map;

public class Switch implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Switch(String name, String field, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return null;
    }

    @Override
    public void setup() {

    }

    @Override
    public List<Schema.Field> getInputFields() {
        return List.of();
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return null;
    }

    @Override
    public boolean ignore() {
        return false;
    }
}
