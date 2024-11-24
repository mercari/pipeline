package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;

import java.util.HashMap;
import java.util.Map;

public class MutationToJsonConverter {


    public static String convertJsonString(final Mutation mutation) {
        final JsonObject jsonObject = convert(mutation);

        if(jsonObject == null) {
            return null;
        }
        return jsonObject.toString();
    }

    public static String convertJsonString(final MutationGroup mutationGroup) {
        final JsonArray jsonArray = convert(mutationGroup);

        if(jsonArray == null) {
            return null;
        }
        return jsonArray.toString();
    }

    public static JsonObject convert(final Mutation mutation) {
        if(mutation == null) {
            return null;
        }
        if(Mutation.Op.DELETE.equals(mutation.getOperation())) {
            final JsonArray keys = new JsonArray();
            for(final Key key : mutation.getKeySet().getKeys()) {
                keys.add(key.toString());
            }
            final JsonObject jsonObject = new JsonObject();
            jsonObject.add("keys", keys);
            return jsonObject;
        } else {
            return convertJsonObject(mutation.asMap());
        }
    }

    public static JsonArray convert(final MutationGroup mutationGroup) {
        if(mutationGroup == null) {
            return null;
        }

        final JsonArray jsonArray = new JsonArray();
        final JsonObject primary = convert(mutationGroup.primary());
        jsonArray.add(primary);
        for(final Mutation mutation : mutationGroup.attached()) {
            final JsonObject attached = convert(mutation);
            jsonArray.add(attached);
        }
        return jsonArray;
    }

    private static JsonObject convertJsonObject(final Map<String, Value> values) {
        final JsonObject jsonObject = new JsonObject();
        for(Map.Entry<String, Value> entry : values.entrySet()) {
            final Value value = entry.getValue();
            if(value.isNull()) {
                jsonObject.add(entry.getKey(), null);
                continue;
            }
            switch (entry.getValue().getType().getCode()) {
                case BOOL -> jsonObject.addProperty(entry.getKey(), value.getBool());
                case STRING -> jsonObject.addProperty(entry.getKey(), value.getString());
                case BYTES -> jsonObject.addProperty(entry.getKey(), value.getBytes().toBase64());
                case JSON -> jsonObject.addProperty(entry.getKey(), value.getJson());
                case INT64 -> jsonObject.addProperty(entry.getKey(), value.getInt64());
                case FLOAT64 -> jsonObject.addProperty(entry.getKey(), value.getFloat64());
                case NUMERIC, PG_NUMERIC -> jsonObject.addProperty(entry.getKey(), value.getNumeric());
                case PG_JSONB -> jsonObject.addProperty(entry.getKey(), value.getPgJsonb());
                case DATE -> jsonObject.addProperty(entry.getKey(), value.getDate().toString());
                case TIMESTAMP -> {
                    if (value.isCommitTimestamp()) {
                        jsonObject.addProperty(entry.getKey(), "COMMIT_TIMESTAMP");
                    } else {
                        jsonObject.addProperty(entry.getKey(), value.getTimestamp().toString());
                    }
                }
                case STRUCT -> {
                    final Struct childStruct = value.getStruct();
                    final Map<String, Value> childValues = toMap(childStruct);
                    final JsonObject child = convertJsonObject(childValues);
                    jsonObject.add(entry.getKey(), child);
                }
                case ARRAY -> {
                    final JsonArray jsonArray = new JsonArray();
                    switch (value.getType().getArrayElementType().getCode()) {
                        case BOOL -> value.getBoolArray().forEach(jsonArray::add);
                        case STRING -> value.getStringArray().forEach(jsonArray::add);
                        case BYTES -> value.getBytesArray().stream().map(ByteArray::toBase64).forEach(jsonArray::add);
                        case JSON -> value.getJsonArray().forEach(jsonArray::add);
                        case INT64 -> value.getInt64Array().forEach(jsonArray::add);
                        case FLOAT64 -> value.getFloat64Array().forEach(jsonArray::add);
                        case NUMERIC, PG_NUMERIC -> value.getNumericArray().forEach(jsonArray::add);
                        case PG_JSONB -> value.getPgJsonbArray().forEach(jsonArray::add);
                        case DATE -> value.getDateArray().stream().map(Date::toString).forEach(jsonArray::add);
                        case TIMESTAMP ->
                                value.getTimestampArray().stream().map(Timestamp::toString).forEach(jsonArray::add);
                        case STRUCT -> value.getStructArray().stream()
                                .map(MutationToJsonConverter::toMap)
                                .map(MutationToJsonConverter::convertJsonObject)
                                .forEach(jsonArray::add);
                        case ARRAY -> {
                            throw new IllegalStateException();
                        }
                        default -> throw new IllegalStateException();
                    }
                    jsonObject.add(entry.getKey(), jsonArray);
                }
                case UNRECOGNIZED -> throw new IllegalStateException();
            }
        }
        return jsonObject;
    }

    private static Map<String, Value> toMap(final Struct struct) {
        final Map<String, Value> map = new HashMap<>();
        if(struct == null) {
            return map;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final Value value = struct.getValue(field.getName());
            map.put(field.getName(), value);
        }
        return map;
    }

}
