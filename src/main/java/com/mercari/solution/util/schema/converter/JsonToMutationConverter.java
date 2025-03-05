package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.gson.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class JsonToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToMutationConverter.class);

    public static Map<String, Value> convertValues(final Schema schema, final JsonObject jsonObject) {
        final Map<String, Value> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final JsonElement jsonElement = jsonObject.has(field.getName()) ? jsonObject.get(field.getName()) : null;
            final Value value = convertValue(field, jsonElement);
            values.put(field.getName(), value);
        }
        return values;
    }

    private static Value convertValue(final Schema.Field field, final JsonElement jsonElement) {
        final Schema.Options options = field.getOptions();
        final boolean isNull = jsonElement == null || jsonElement.isJsonNull();
        return switch (field.getType().getTypeName()) {
            case BOOLEAN -> Value.bool(jsonElement.getAsBoolean());
            case STRING -> {
                final String stringValue = isNull ? null : jsonElement.getAsString();
                final String sqlType = options.hasOption("sqlType") ? options.getValue("sqlType") : null;
                if("DATETIME".equals(sqlType)) {
                    yield Value.timestamp(isNull ? null : Timestamp.parseTimestamp(stringValue));
                } else if("JSON".equals(sqlType)) {
                    yield Value.json(stringValue);
                } else if("GEOGRAPHY".equals(sqlType)) {
                    yield Value.string(stringValue);
                } else {
                    yield Value.string(stringValue);
                }
            }
            case BYTES -> Value.bytes(isNull ? null : ByteArray.copyFrom(Base64.getDecoder().decode(jsonElement.getAsString())));
            case FLOAT -> Value.float64(isNull ? null : jsonElement.getAsFloat());
            case DOUBLE -> Value.float64(isNull ? null : jsonElement.getAsDouble());
            case DECIMAL -> Value.numeric(isNull ? null : jsonElement.getAsBigDecimal());
            case BYTE -> Value.int64(isNull ? null : jsonElement.getAsByte());
            case INT16 -> Value.int64(isNull ? null : jsonElement.getAsShort());
            case INT32 -> Value.int64(isNull ? null : jsonElement.getAsInt());
            case INT64 -> Value.int64(isNull ? null : jsonElement.getAsLong());
            case DATETIME -> Value.timestamp(isNull ? null : Timestamp.parseTimestamp(jsonElement.getAsString()));
            case LOGICAL_TYPE -> {
                if(!jsonElement.isJsonPrimitive()) {
                    final String message = "json fieldType: " + field.getType().getTypeName() + ", value: " + jsonElement + " could not be convert to logicalType";
                    LOG.warn(message);
                    throw new IllegalStateException(message);
                }
                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    yield Value.date(isNull ? null : Date.parseDate(primitive.getAsString()));
                } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                    yield Value.string(isNull ? null : primitive.getAsString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    yield Value.timestamp(isNull ? null : Timestamp.parseTimestamp(primitive.getAsString()));
                } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                    yield Value.string(isNull ? null : primitive.getAsString());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                }
            }
            case ROW, MAP -> throw new IllegalArgumentException("Unsupported row type: " + field.getType());
            case ITERABLE, ARRAY -> {
                final Schema.FieldType elementType = field.getType().getCollectionElementType();
                final JsonArray jsonArray = isNull ? null : jsonElement.getAsJsonArray();
                switch (elementType.getTypeName()) {
                    case BOOLEAN -> {
                        if(isNull) {
                            yield Value.boolArray(new ArrayList<>());
                        }
                        final List<Boolean> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsBoolean());
                        }
                        yield Value.boolArray(values);
                    }
                    case STRING -> {
                        final String sqlType = options.hasOption("sqlType") ? options.getValue("sqlType") : null;
                        if("DATETIME".equals(sqlType)) {
                            if(isNull) {
                                yield Value.timestampArray(new ArrayList<>());
                            }
                            final List<Timestamp> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Timestamp.parseTimestamp(element.getAsString()));
                            }
                            yield Value.timestampArray(values);
                        } else if("JSON".equals(sqlType)) {
                            if(isNull) {
                                yield Value.jsonArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            yield Value.jsonArray(values);
                        } else if("GEOGRAPHY".equals(sqlType)) {
                            if(isNull) {
                                yield Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            yield Value.stringArray(values);
                        } else {
                            if(isNull) {
                                yield Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            yield Value.stringArray(values);
                        }
                    }
                    case BYTES -> {
                        if(isNull) {
                            yield Value.bytesArray(new ArrayList<>());
                        }
                        final List<ByteArray> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            final byte[] bytes = Base64.getDecoder().decode(element.getAsString());
                            values.add(ByteArray.copyFrom(bytes));
                        }
                        yield Value.bytesArray(values);
                    }
                    case FLOAT -> {
                        if (isNull) {
                            yield Value.float32Array(new ArrayList<>());
                        }
                        final List<Float> values = new ArrayList<>();
                        for (final JsonElement element : jsonArray) {
                            if (element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsFloat());
                        }
                        yield Value.float32Array(values);
                    }
                    case DOUBLE -> {
                        if (isNull) {
                            yield Value.float64Array(new ArrayList<>());
                        }
                        final List<Double> values = new ArrayList<>();
                        for (final JsonElement element : jsonArray) {
                            if (element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsDouble());
                        }
                        yield Value.float64Array(values);
                    }
                    case BYTE, INT16, INT32, INT64 -> {
                        if(isNull) {
                            yield Value.int64Array(new ArrayList<>());
                        }
                        final List<Long> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsLong());
                        }
                        yield Value.int64Array(values);
                    }
                    case DATETIME -> {
                        if(isNull) {
                            yield Value.timestampArray(new ArrayList<>());
                        }
                        final List<Timestamp> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            if(element.isJsonPrimitive()) {
                                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                                if(primitive.isString()) {
                                    values.add(Timestamp.parseTimestamp(element.getAsString()));
                                } else if(primitive.isNumber()) {
                                    values.add(Timestamp.ofTimeMicroseconds(primitive.getAsLong()));
                                }
                            } else if(element.isJsonObject()) {
                                values.add(Timestamp.parseTimestamp(element.getAsString()));
                            }
                        }
                        yield Value.timestampArray(values);
                    }
                    case LOGICAL_TYPE -> {
                        if(!jsonElement.isJsonPrimitive()) {
                            final String message = "json element fieldType: " + field.getType().getTypeName() + ", value: " + jsonElement + " could not be convert to logicalType";
                            LOG.warn(message);
                            throw new IllegalStateException(message);
                        }
                        final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                        if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                            if(isNull) {
                                yield Value.dateArray(new ArrayList<>());
                            }
                            final List<Date> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Date.parseDate(element.getAsString()));
                            }
                            yield Value.dateArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                            if(isNull) {
                                yield Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            yield Value.stringArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                            if(isNull) {
                                yield Value.timestampArray(new ArrayList<>());
                            }
                            final List<Timestamp> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Timestamp.parseTimestamp(element.getAsString()));
                            }
                            yield Value.timestampArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                            if(isNull) {
                                yield Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            yield Value.stringArray(values);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                        }
                    }
                    default -> throw new IllegalStateException("Not supported array field schema: " + elementType);
                }
            }
            default -> throw new IllegalArgumentException("Not supported field schema: " + field.getType());
        };
    }

}
