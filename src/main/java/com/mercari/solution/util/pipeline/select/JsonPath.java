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

public class JsonPath implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(JsonPath.class);

    private final String name;
    private final String field;
    private final String path;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    JsonPath(String name, String field, String path, Schema.Field inputField, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.path = path;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static JsonPath of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }
        if(!jsonObject.has("path")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires path parameter");
        }

        final String field = jsonObject.get("field").getAsString();
        final String path = jsonObject.get("path").getAsString();
        final Schema.Field inputField = ElementSchemaUtil.getInputField(field, inputFields);

        final Schema.FieldType outputFieldType = Schema.FieldType.STRING.withNullable(true);

        return new JsonPath(name, field, path, inputField, outputFieldType, ignore);
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
        final Object value = ElementSchemaUtil.getValue(input, field);
        final String json = (String)ElementSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, value);
        try {
            final Object str = org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.JsonPath.read(json, path);
            return str;
        } catch (Throwable e) {
            System.out.println("field: " + field + ", path: " + path + ", json: " + json + e);
            return null;
        }
    }

}