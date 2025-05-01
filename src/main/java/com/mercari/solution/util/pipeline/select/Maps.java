package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Maps  {

    /*
    private static final Logger LOG = LoggerFactory.getLogger(Maps.class);

    private final String name;
    private final String selectFunctionsJson;

    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    private transient List<SelectFunction> selectFunctions;

    Maps(final String name,
           final String selectFunctionsJson,
           final DataType outputType,
           final Schema.FieldType outputFieldType,
           final List<Schema.Field> inputFields,
           final boolean ignore) {

        this.name = name;
        this.selectFunctionsJson = selectFunctionsJson;
        this.outputType = outputType;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Maps of(String name, JsonObject jsonObject, DataType outputType, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("fields")) {
            throw new IllegalArgumentException("SelectField: " + name + " map func requires fields parameter");
        } else if(!jsonObject.get("fields").isJsonArray()) {
            throw new IllegalArgumentException("SelectField: " + name + " map func fields parameter must be array");
        }
        final JsonElement fieldsElement = jsonObject.get("fields");
        final List<SelectFunction> childFunctions = SelectFunction.of(fieldsElement.getAsJsonArray(), inputFields, outputType);
        final List<Schema.Field> nestedInputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : childFunctions) {
            nestedInputFields.addAll(selectFunction.getInputFields());
        }
        final Schema rowSchema = SelectFunction.createSchema(childFunctions);
        final Schema.FieldType mapValueType = rowSchema.getField(0).getType();
        for(int i = 1; i<rowSchema.getFieldCount(); i++) {
            final Schema.Field field = rowSchema.getField(i);
            if(!mapValueType.getTypeName().equals(field.getType().getTypeName())) {
                throw new IllegalArgumentException("SelectField: " + name + " map func fields parameter type is not same. "
                        + "field: " + rowSchema.getField(0).getName() + ", type: " + mapValueType.getTypeName()
                        + " : field: " + field.getName() + ", type: " + field.getType().getTypeName());
            }
        }

        final Schema.FieldType outputFieldType = Schema.FieldType.map(
                Schema.FieldType.STRING,
                mapValueType);

        return new Maps(name, fieldsElement.toString(), outputType, outputFieldType, nestedInputFields, ignore);
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
        final JsonArray jsonArray = new Gson().fromJson(this.selectFunctionsJson, JsonArray.class);
        this.selectFunctions = SelectFunction.of(jsonArray, inputFields, outputType);
        for(final SelectFunction selectFunction : this.selectFunctions) {
            selectFunction.setup();
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> newInput = new HashMap<>(input);
        final Map<String, Object> processed = SelectFunction.apply(selectFunctions, newInput, outputType, timestamp);
        final Map<String, Object> output = new HashMap<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            output.put(selectFunction.getName(), processed.get(selectFunction.getName()));
        }
        return output;
    }

     */

}

