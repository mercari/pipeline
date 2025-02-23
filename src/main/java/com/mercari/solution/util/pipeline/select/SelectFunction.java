package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface SelectFunction extends Serializable {

    String getName();
    Object apply(Map<String, Object> input, Instant timestamp);
    void setup();
    List<Schema.Field> getInputFields();
    Schema.FieldType getOutputFieldType();
    boolean ignore();

    enum Func implements Serializable {
        pass,
        constant,
        rename,
        cast,
        expression,
        text,
        concat,
        nullif,
        uuid,
        hash,
        event_timestamp,
        current_timestamp,
        struct,
        map,
        json,
        jsonpath,
        http,
        scrape,
        generate,
        base64encode,
        base64decode
    }

    static List<SelectFunction> of(final JsonArray selects, final List<Schema.Field> inputFields) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final Schema.Field field : inputFields) {
            fields.add(field.copy());
        }
        final List<SelectFunction> selectFunctions = new ArrayList<>();
        if(selects == null || !selects.isJsonArray()) {
            return selectFunctions;
        }

        for(final JsonElement select : selects) {
            if(!select.isJsonObject()) {
                continue;
            }
            final SelectFunction selectFunction = SelectFunction.of(select.getAsJsonObject(), fields);
            if(selectFunction.ignore()) {
                continue;
            }
            selectFunctions.add(selectFunction);
            fields.add(Schema.Field.of(selectFunction.getName(), selectFunction.getOutputFieldType()));
        }
        return selectFunctions;
    }

    static SelectFunction of(JsonObject jsonObject, List<Schema.Field> inputFields) {

        if(!jsonObject.has("name")) {
            throw new IllegalArgumentException("selectField requires name parameter");
        }
        final String name = jsonObject.get("name").getAsString();

        final Func func;
        if(jsonObject.has("func")) {
            func = Func.valueOf(jsonObject.get("func").getAsString());
        } else {
            if(jsonObject.size() == 1) {
                func = Func.pass;
            } else if(jsonObject.has("field")) {
                if(jsonObject.has("type")) {
                    func = Func.cast;
                } else {
                    func = Func.rename;
                }
            } else if(jsonObject.has("value")) {
                if(jsonObject.has("type")) {
                    func = Func.constant;
                } else  {
                    throw new IllegalArgumentException("selectField value requires type parameter");
                }
            } else if(jsonObject.has("type") && jsonObject.size() == 2) {
                func = Func.cast;
            } else if(jsonObject.has("expression")) {
                func = Func.expression;
            } else if(jsonObject.has("text")) {
                func = Func.text;
            } else {
                throw new IllegalArgumentException("selectField requires func parameter");
            }
        }

        final boolean ignore;
        if(jsonObject.has("ignore")) {
            ignore = jsonObject.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        return switch (func) {
            case pass -> Pass.of(name, inputFields, ignore);
            case rename -> Rename.of(name, jsonObject, inputFields, ignore);
            case cast -> Cast.of(name, jsonObject, inputFields, ignore);
            case constant -> Constant.of(name, jsonObject, ignore);
            case expression -> Expression.of(name, jsonObject, ignore);
            case text -> Text.of(name, jsonObject, inputFields, ignore);
            case concat -> Concat.of(name, jsonObject, inputFields, ignore);
            case nullif -> Nullif.of(name, jsonObject, inputFields, ignore);
            case uuid -> Uuid.of(name, jsonObject, ignore);
            case hash -> Hash.of(name, jsonObject, inputFields, ignore);
            case event_timestamp -> EventTimestamp.of(name, ignore);
            case current_timestamp -> CurrentTimestamp.of(name, ignore);
            case struct -> Struct.of(name, jsonObject, inputFields, ignore);
            case json -> Jsons.of(name, jsonObject, inputFields, ignore);
            case jsonpath ->  JsonPath.of(name, jsonObject, inputFields, ignore);
            case http -> Http.of(name, jsonObject, inputFields, ignore);
            case scrape -> Scrape.of(name, jsonObject, inputFields, ignore);
            case generate -> Generate.of(name, jsonObject, inputFields, ignore);
            case base64encode -> Base64Coder.of(name, jsonObject, inputFields, true, ignore);
            case base64decode -> Base64Coder.of(name, jsonObject, inputFields, false, ignore);
            default -> throw new IllegalArgumentException();
        };
    }

    static Schema createSchema(final JsonArray select, List<Schema.Field> outputFields) {
        final List<SelectFunction> selectFunctions = SelectFunction.of(select, outputFields);
        return SelectFunction.createSchema(selectFunctions, null);
    }

    static Schema createSchema(List<SelectFunction> selectFunctions) {
        return createSchema(selectFunctions, null);
    }

    static Schema createSchema(List<SelectFunction> selectFunctions, String flattenField) {
        final List<Schema.Field> selectOutputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Schema.FieldType selectOutputFieldType = selectFunction.getOutputFieldType();
            Schema.Field field = Schema.Field.of(selectFunction.getName(), selectOutputFieldType);
            selectOutputFields.add(field);
        }
        final Schema schema = Schema.builder().withFields(selectOutputFields).build();
        if(flattenField != null) {
            if(!schema.hasField(flattenField)) {
                throw new IllegalArgumentException("flatten field: " + flattenField + " not found in schema; " + schema);
            }
            return createFlattenSchema(schema, flattenField);
        } else {
            return schema;
        }
    }

    static Map<String, Object> apply(
            final List<SelectFunction> selectFunctions,
            final MElement element,
            final Instant timestamp) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            for(final Schema.Field inputField : selectFunction.getInputFields()) {
                final Object primitiveValue = element.getPrimitiveValue(inputField.getName());
                primitiveValues.put(inputField.getName(), primitiveValue);
            }
        }
        return apply(selectFunctions, primitiveValues, timestamp);
    }

    static Map<String, Object> apply(
            final List<SelectFunction> selectFunctions,
            final Map<String, Object> primitiveValues,
            final Instant timestamp) {

        final Map<String, Object> primitiveValues_ = new HashMap<>(primitiveValues);
        final Map<String, Object> output = new HashMap<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Object outputPrimitiveValue = selectFunction.apply(primitiveValues_, timestamp);
            primitiveValues_.put(selectFunction.getName(), outputPrimitiveValue);
            output.put(selectFunction.getName(), outputPrimitiveValue);
        }
        return output;
    }

    static String getStringParameter(final String name, final JsonObject jsonObject, final String field, final String defaultValue) {
        final String value;
        if(jsonObject.has(field)) {
            if(!jsonObject.get(field).isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField: " + name + "." + field + " parameter must be string");
            }
            value = jsonObject.get(field).getAsString();
        } else {
            value = defaultValue;
        }
        return value;
    }

    static Schema createFlattenSchema(final Schema inputSchema, final String flattenField) {
        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : inputSchema.getFields()) {
            if(field.getName().equals(flattenField)) {
                if(!Schema.Type.array.equals(field.getFieldType().getType())) {
                    throw new IllegalArgumentException("flattenField: " + flattenField + " type: " + field.getFieldType() + " is not array type");
                }
                builder.withField(field.getName(), field.getFieldType().getArrayValueType().withNullable(true));
            } else {
                builder.withField(field);
            }
        }
        return builder.build();
    }

}
