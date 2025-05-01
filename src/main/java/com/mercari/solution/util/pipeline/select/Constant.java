package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Constant implements SelectFunction {

    private final String name;
    private final String type;
    private final String valueString;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient JsonElement value;

    Constant(String name, String type, String valueString, boolean ignore) {
        this.name = name;
        this.type = type;
        this.valueString = valueString;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.type(type);
        this.ignore = ignore;
    }

    public static Constant of(String name, JsonObject jsonObject, boolean ignore) {
        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField constant: " + name + " requires type parameter");
        }
        final String type = jsonObject.get("type").getAsString();

        if(!jsonObject.has("value")) {
            throw new IllegalArgumentException("SelectField constant: " + name + " requires value parameter");
        }
        final String value = jsonObject.get("value").toString();

        return new Constant(name, type, value, ignore);
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
        this.value = new Gson().fromJson(valueString, JsonElement.class);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(value == null || value.isJsonNull()) {
            return null;
        }
        //return Schema.Type.getValue(type, value);
        return JsonToMapConverter.getAsPrimitiveValue(Schema.FieldType.type(type), value);
    }

}
