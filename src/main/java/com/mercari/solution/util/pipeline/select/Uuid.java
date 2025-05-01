package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Uuid implements SelectFunction {

    private final String name;
    private final Integer size;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    Uuid(String name, Integer size, boolean ignore) {
        this.name = name;
        this.size = size;

        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.STRING.withNullable(true);
        this.ignore = ignore;
    }

    public static Uuid of(String name, JsonObject jsonObject, boolean ignore) {
        final Integer size;
        if(jsonObject.has("size")) {
            size = jsonObject.get("size").getAsInt();
        } else {
            size = null;
        }

        return new Uuid(name, size, ignore);
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
        final String str = UUID.randomUUID().toString();
        if(size == null) {
            return str;
        } else if(size > str.length()) {
            return str;
        } else {
            return str.substring(0, size);
        }
    }

}
