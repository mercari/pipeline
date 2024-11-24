package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.gson.*;
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
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                return Value.bool(jsonElement.getAsBoolean());
            case STRING: {
                final String stringValue = isNull ? null : jsonElement.getAsString();
                final String sqlType = options.hasOption("sqlType") ? options.getValue("sqlType") : null;
                if("DATETIME".equals(sqlType)) {
                    return Value.timestamp(isNull ? null : Timestamp.parseTimestamp(stringValue));
                } else if("JSON".equals(sqlType)) {
                    return Value.json(stringValue);
                } else if("GEOGRAPHY".equals(sqlType)) {
                    return Value.string(stringValue);
                } else {
                    return Value.string(stringValue);
                }
            }
            case BYTES:
                return Value.bytes(isNull ? null : ByteArray.copyFrom(Base64.getDecoder().decode(jsonElement.getAsString())));
            case FLOAT:
                return Value.float64(isNull ? null : jsonElement.getAsFloat());
            case DOUBLE:
                return Value.float64(isNull ? null : jsonElement.getAsDouble());
            case DECIMAL:
                return Value.numeric(isNull ? null : jsonElement.getAsBigDecimal());
            case BYTE:
                return Value.int64(isNull ? null : jsonElement.getAsByte());
            case INT16:
                return Value.int64(isNull ? null : jsonElement.getAsShort());
            case INT32: {
                return Value.int64(isNull ? null : jsonElement.getAsInt());
            }
            case INT64: {
                return Value.int64(isNull ? null : jsonElement.getAsLong());
            }
            case DATETIME:
                return Value.timestamp(isNull ? null : Timestamp.parseTimestamp(jsonElement.getAsString()));
            case LOGICAL_TYPE: {
                if(!jsonElement.isJsonPrimitive()) {
                    final String message = "json fieldType: " + field.getType().getTypeName() + ", value: " + jsonElement + " could not be convert to logicalType";
                    LOG.warn(message);
                    throw new IllegalStateException(message);
                }
                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    return Value.date(isNull ? null : Date.parseDate(primitive.getAsString()));
                } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                    return Value.string(isNull ? null : primitive.getAsString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    return Value.timestamp(isNull ? null : Timestamp.parseTimestamp(primitive.getAsString()));
                } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                    return Value.string(isNull ? null : primitive.getAsString());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                }
            }
            case ROW:
            case MAP:
                throw new IllegalArgumentException("Unsupported row type: " + field.getType());
            case ITERABLE:
            case ARRAY: {
                final Schema.FieldType elementType = field.getType().getCollectionElementType();
                final JsonArray jsonArray = isNull ? null : jsonElement.getAsJsonArray();
                switch (elementType.getTypeName()) {
                    case BOOLEAN: {
                        if(isNull) {
                            return Value.boolArray(new ArrayList<>());
                        }
                        final List<Boolean> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsBoolean());
                        }
                        return Value.boolArray(values);
                    }
                    case STRING: {
                        final String sqlType = options.hasOption("sqlType") ? options.getValue("sqlType") : null;
                        if("DATETIME".equals(sqlType)) {
                            if(isNull) {
                                return Value.timestampArray(new ArrayList<>());
                            }
                            final List<Timestamp> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Timestamp.parseTimestamp(element.getAsString()));
                            }
                            return Value.timestampArray(values);
                        } else if("JSON".equals(sqlType)) {
                            if(isNull) {
                                return Value.jsonArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            return Value.jsonArray(values);
                        } else if("GEOGRAPHY".equals(sqlType)) {
                            if(isNull) {
                                return Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            return Value.stringArray(values);
                        } else {
                            if(isNull) {
                                return Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            return Value.stringArray(values);
                        }
                    }
                    case BYTES: {
                        if(isNull) {
                            return Value.bytesArray(new ArrayList<>());
                        }
                        final List<ByteArray> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            final byte[] bytes = Base64.getDecoder().decode(element.getAsString());
                            values.add(ByteArray.copyFrom(bytes));
                        }
                        return Value.bytesArray(values);
                    }
                    case FLOAT:
                    case DOUBLE: {
                        if (isNull) {
                            return Value.float64Array(new ArrayList<>());
                        }
                        final List<Double> values = new ArrayList<>();
                        for (final JsonElement element : jsonArray) {
                            if (element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsDouble());
                        }
                        return Value.float64Array(values);
                    }
                    case BYTE:
                    case INT16:
                    case INT32:
                    case INT64: {
                        if(isNull) {
                            return Value.int64Array(new ArrayList<>());
                        }
                        final List<Long> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(element.getAsLong());
                        }
                        return Value.int64Array(values);
                    }
                    case DATETIME: {
                        if(isNull) {
                            return Value.timestampArray(new ArrayList<>());
                        }
                        final List<Timestamp> values = new ArrayList<>();
                        for(final JsonElement element : jsonArray) {
                            if(element == null || element.isJsonNull()) {
                                continue;
                            }
                            values.add(Timestamp.parseTimestamp(element.getAsString()));
                        }
                        return Value.timestampArray(values);
                    }
                    case LOGICAL_TYPE: {
                        if(!jsonElement.isJsonPrimitive()) {
                            final String message = "json element fieldType: " + field.getType().getTypeName() + ", value: " + jsonElement + " could not be convert to logicalType";
                            LOG.warn(message);
                            throw new IllegalStateException(message);
                        }
                        final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                        if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                            if(isNull) {
                                return Value.dateArray(new ArrayList<>());
                            }
                            final List<Date> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Date.parseDate(element.getAsString()));
                            }
                            return Value.dateArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                            if(isNull) {
                                return Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            return Value.stringArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                            if(isNull) {
                                return Value.timestampArray(new ArrayList<>());
                            }
                            final List<Timestamp> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(Timestamp.parseTimestamp(element.getAsString()));
                            }
                            return Value.timestampArray(values);
                        } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                            if(isNull) {
                                return Value.stringArray(new ArrayList<>());
                            }
                            final List<String> values = new ArrayList<>();
                            for(final JsonElement element : jsonArray) {
                                if(element == null || element.isJsonNull()) {
                                    continue;
                                }
                                values.add(element.getAsString());
                            }
                            return Value.stringArray(values);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                        }
                    }
                    default: {
                        throw new IllegalStateException("Not supported array field schema: " + elementType);
                    }
                }
            }
            default: {
                throw new IllegalArgumentException("Not supported field schema: " + field.getType());
            }
        }
    }

}
