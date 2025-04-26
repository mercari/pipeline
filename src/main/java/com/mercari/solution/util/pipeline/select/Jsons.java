package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Jsons implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Jsons.class);

    private final String name;
    private final String selectFunctionsJson;

    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final String eachField;

    private final Schema outputSchema;
    private final boolean isArray;
    private final boolean ignore;


    private transient List<SelectFunction> selectFunctions;

    Jsons(final String name,
          final String selectFunctionsJson,
          final Schema.FieldType outputFieldType,
          final List<Schema.Field> inputFields,
          final String eachField,
          final Schema outputSchema,
          final boolean isArray,
          final boolean ignore) {

        this.name = name;
        this.selectFunctionsJson = selectFunctionsJson;
        this.outputType = DataType.ROW;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.eachField = eachField;

        this.outputSchema = outputSchema;
        this.isArray = isArray;
        this.ignore = ignore;
    }

    public static Jsons of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("fields")) {
            throw new IllegalArgumentException("SelectField: " + name + " json func requires fields parameter");
        } else if(!jsonObject.get("fields").isJsonArray()) {
            throw new IllegalArgumentException("SelectField: " + name + " json func fields parameter must be array");
        }
        final JsonElement fieldsElement = jsonObject.get("fields");
        final List<SelectFunction> childFunctions = SelectFunction.of(fieldsElement.getAsJsonArray(), inputFields);
        final Schema outputSchema = SelectFunction.createSchema(childFunctions);
        final List<Schema.Field> nestedInputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : childFunctions) {
            nestedInputFields.addAll(selectFunction.getInputFields());
        }

        final String eachField;
        if(jsonObject.has("each") && jsonObject.get("each").isJsonPrimitive()) {
            eachField = jsonObject.get("each").getAsString();
        } else {
            eachField = null;
        }

        final String mode;
        if(jsonObject.has("mode") && jsonObject.get("mode").isJsonPrimitive()) {
            mode = jsonObject.get("mode").getAsString();
        } else if(eachField != null) {
            mode = "repeated";
        } else {
            mode = "nullable";
        }

        final Schema.FieldType fieldType = Schema.FieldType.STRING;
        final Schema.FieldType outputFieldType = switch (mode) {
            case "required" -> fieldType;
            case "nullable" -> fieldType.withNullable(true);
            case "repeated" -> Schema.FieldType.array(fieldType).withNullable(true);
            default -> throw new IllegalArgumentException("illegal json mode: " + mode);
        };

        final boolean isArray = Schema.Type.array.equals(outputFieldType.getType());

        final List<Schema.Field> deduplicatedNestedInputFields = new ArrayList<>();
        final Set<String> nestedInputFieldNames = new HashSet<>();
        for(final Schema.Field nestedInputField : nestedInputFields) {
            if(!nestedInputFieldNames.contains(nestedInputField.getName())) {
                deduplicatedNestedInputFields.add(nestedInputField);
                nestedInputFieldNames.add(nestedInputField.getName());
            }
        }

        return new Jsons(name, fieldsElement.toString(), outputFieldType,
                deduplicatedNestedInputFields, eachField,
                outputSchema, isArray, ignore);
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
        this.selectFunctions = SelectFunction.of(jsonArray, inputFields);
        for(final SelectFunction selectFunction : this.selectFunctions) {
            selectFunction.setup();
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(isArray) {
            if(eachField == null || !input.containsKey(eachField)) {
                final Map<String, Object> newInput = new HashMap<>(input);
                final Map<String, Object> output = SelectFunction.apply(selectFunctions, newInput, timestamp);
                final JsonObject jsonObject = toJsonObject(output);
                return List.of(jsonObject.toString());
            }
            final List<Object> eachValues = (List) input.get(eachField);
            if(eachValues == null) {
                return new ArrayList<>();
            }
            final List<Map<String, Object>> outputs = new ArrayList<>();
            for(final Object value :eachValues) {
                final Map<String, Object> newInput = new HashMap<>(input);
                newInput.put("_", value);
                final Map<String, Object> output =  SelectFunction.apply(selectFunctions, newInput, timestamp);
                outputs.add(output);
            }

            final JsonArray jsonArray = new JsonArray();
            for(final Map<String, Object> value : outputs) {
                jsonArray.add(toJsonObject(value));
            }
            return jsonArray.toString();
        } else {
            final Map<String, Object> newInput = new HashMap<>(input);
            final Map<String, Object> output = SelectFunction.apply(selectFunctions, newInput, timestamp);
            final JsonObject jsonObject = toJsonObject(output);
            return jsonObject.toString();
        }
    }

    private JsonObject toJsonObject(final Map<String, Object> values) {
        if(values == null) {
            return null;
        }
        return ElementToJsonConverter.convert(outputSchema, values, null);
    }

}
