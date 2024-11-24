package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Rename implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Rename.class);

    private final String name;
    private final String field;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Rename(String name, String field, Schema.Field inputField, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Rename of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }

        final String inputFieldName;
        final String field = jsonObject.get("field").getAsString();
        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            inputFieldName = fields[0];
        } else {
            inputFieldName = field;
        }
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(inputFieldName, inputFields);
        final Schema.Field inputField = Schema.Field.of(inputFieldName, inputFieldType);

        final Schema.FieldType outputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);

        return new Rename(name, field, inputField, outputFieldType, ignore);
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
        return ElementSchemaUtil.getValue(input, field);
    }

}