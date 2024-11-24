package com.mercari.solution.util.schema.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;


public class AvroToJsonConverter {

    public static String convert(final GenericRecord record, final List<String> fields) {
        return convertObject(record, fields).toString();
    }

    public static String convert(final GenericRecord record) {
        return convert(record, null);
    }

    public static JsonObject convertObject(final GenericRecord record) {
        return convertObject(record, null);
    }

    public static JsonObject convertObject(final GenericRecord record, final List<String> fields) {
        final JsonObject obj = new JsonObject();
        if(record == null) {
            return obj;
        }
        if(fields != null && !fields.isEmpty()) {
            record.getSchema().getFields().stream()
                    .filter(f -> fields.contains(f.name()))
                    .forEach(f -> setValue(obj, f, record));
        } else {
            record.getSchema().getFields()
                    .forEach(f -> setValue(obj, f, record));
        }
        return obj;
    }

    private static void setValue(final JsonObject obj, final Schema.Field field, final GenericRecord record) {
        final String fieldName = field.name();
        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
        final Object value = record.hasField(fieldName) ? record.get(fieldName) : null;
        final boolean isNullField = value == null;
        switch (fieldSchema.getType()) {
            case BOOLEAN -> obj.addProperty(fieldName, (Boolean) value);
            case ENUM, STRING -> obj.addProperty(fieldName, isNullField ? null : value.toString());
            case FIXED, BYTES -> {
                if(isNullField) {
                    obj.addProperty(fieldName, (String)null);
                } else {
                    final byte[] bytes = ((ByteBuffer)value).array();
                    if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                        final int scale = AvroSchemaUtil.getLogicalTypeDecimal(fieldSchema).getScale();
                        var decimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                        obj.addProperty(fieldName, decimal.toString());
                    } else {
                        obj.addProperty(fieldName, Base64.getEncoder().encodeToString(bytes));
                    }
                }
            }
            case INT -> {
                if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalDate.ofEpochDay((int) value).format(DateTimeFormatter.ISO_LOCAL_DATE));
                } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                } else {
                    obj.addProperty(fieldName, (Integer) value);
                }
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue)));
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue / 1000)));
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalTime.ofNanoOfDay(longValue * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                } else {
                    obj.addProperty(fieldName, longValue);
                }
            }
            case FLOAT -> {
                final Float floatValue = (Float) value;
                if (isNullField || Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                    obj.addProperty(fieldName, (Float) null);
                } else {
                    obj.addProperty(fieldName, floatValue);
                }
            }
            case DOUBLE -> {
                final Double doubleValue = (Double) value;
                if (isNullField || Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    obj.addProperty(fieldName, (Double) null);
                } else {
                    obj.addProperty(fieldName, doubleValue);
                }
            }
            case RECORD -> obj.add(fieldName, isNullField ? null : convertObject((GenericRecord) value));
            case ARRAY -> obj.add(fieldName, isNullField ? null : convertArray(fieldSchema.getElementType(), (List<?>) value));
            case NULL -> obj.add(fieldName, null);
        }
    }

    private static JsonArray convertArray(final Schema schema, final List<?> arrayValue) {
        final JsonArray array = new JsonArray();
        if(arrayValue == null || arrayValue.isEmpty()) {
            return array;
        }
        final Schema elementSchema = AvroSchemaUtil.unnestUnion(schema);
        switch (elementSchema.getType()) {
            case BOOLEAN ->
                arrayValue.stream()
                        .map(v -> (Boolean) v)
                        .forEach(array::add);
            case FIXED, BYTES ->
                arrayValue.stream()
                        .map(v -> (ByteBuffer)v)
                        .map(v -> {
                            if(v == null) {
                                return null;
                            }
                            final byte[] bytes = v.array();
                            if(AvroSchemaUtil.isLogicalTypeDecimal(elementSchema)) {
                                final int scale = AvroSchemaUtil.getLogicalTypeDecimal(elementSchema).getScale();
                                var decimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                                return decimal.toPlainString();
                            } else {
                                return Base64.getEncoder().encodeToString(bytes);
                            }
                        })
                        .forEach(array::add);
            case ENUM, STRING ->
                arrayValue.stream()
                        .map(v -> v == null ? null : v.toString())
                        .forEach(array::add);
            case INT -> {
                if (LogicalTypes.date().equals(elementSchema.getLogicalType())) {
                    arrayValue.stream()
                            .map(v -> {
                                if (v == null) {
                                    return null;
                                }
                                return LocalDate.ofEpochDay((Integer) v)
                                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
                            })
                            .forEach(array::add);
                } else if (LogicalTypes.timeMillis().equals(elementSchema.getLogicalType())) {
                    arrayValue.stream()
                            .map(v -> {
                                if (v == null) {
                                    return null;
                                }
                                return LocalTime.ofNanoOfDay((1000L * 1000L * (Integer) v))
                                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
                            })
                            .forEach(array::add);
                } else {
                    arrayValue.stream()
                            .map(v -> (Integer) v)
                            .forEach(array::add);
                }
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(elementSchema.getLogicalType())) {
                    arrayValue.stream()
                            .map(v -> (Long) v)
                            .map(v -> {
                                if (v == null) {
                                    return null;
                                }
                                var instant = java.time.Instant.ofEpochMilli(v);
                                return DateTimeFormatter.ISO_INSTANT.format(instant);
                            })
                            .forEach(array::add);
                } else if (LogicalTypes.timestampMicros().equals(elementSchema.getLogicalType())) {
                    arrayValue.stream()
                            .map(v -> (Long) v)
                            .map(v -> {
                                if (v == null) {
                                    return null;
                                }
                                var instant = java.time.Instant.ofEpochMilli(v / 1000);
                                return DateTimeFormatter.ISO_INSTANT.format(instant);
                            })
                            .forEach(array::add);
                } else if (LogicalTypes.timeMicros().equals(elementSchema.getLogicalType())) {
                    arrayValue.stream()
                            .map(v -> (Long) v)
                            .map(v -> {
                                if (v == null) {
                                    return null;
                                }
                                return LocalTime.ofNanoOfDay(v * 1000)
                                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
                            })
                            .forEach(array::add);
                } else {
                    arrayValue.stream()
                            .map(v -> (Long) v)
                            .forEach(array::add);
                }
            }
            case FLOAT ->
                arrayValue.stream()
                        .map(v -> {
                            final Float floatValue = (Float) v;
                            if (v == null || Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                                return null;
                            } else {
                                return floatValue;
                            }
                        })
                        .forEach(array::add);
            case DOUBLE ->
                arrayValue.stream()
                        .map(v -> {
                            final Double doubleValue = (Double) v;
                            if (v == null || Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                                return null;
                            } else {
                                return doubleValue;
                            }
                        })
                        .forEach(array::add);
            case RECORD ->
                arrayValue.stream()
                        .map(o -> (GenericRecord)o)
                        .map(AvroToJsonConverter::convertObject)
                        .forEach(array::add);
        }
        return array;
    }
}
