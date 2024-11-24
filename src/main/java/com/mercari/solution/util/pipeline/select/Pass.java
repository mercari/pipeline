package com.mercari.solution.util.pipeline.select;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Pass implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Pass.class);

    private final String name;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Pass(String name, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(Schema.Field.of(name, outputFieldType));
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Pass of(String name, List<Schema.Field> inputFields, boolean ignore) {
        final Schema.FieldType outputFieldType = ElementSchemaUtil.getInputFieldType(name, inputFields);
        return new Pass(name, outputFieldType, ignore);
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
        return input.get(name);
    }

}