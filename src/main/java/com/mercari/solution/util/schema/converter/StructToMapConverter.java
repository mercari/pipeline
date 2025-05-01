package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StructToMapConverter {

    public static Map<String, Object> convert(final Struct struct) {
        return convertWithFields(struct, null);
    }

    public static Map<String, Object> convertWithFields(final Struct struct, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(struct == null) {
            return map;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(fields == null || fields.contains(field.getName())) {
                map.put(field.getName(), getValue(field, struct));
            }
        }
        return map;
    }

    private static Object getValue(final Type.StructField field, final Struct struct) {
        if(struct.isNull(field.getName())) {
            return null;
        }
        return switch (field.getType().getCode()) {
            case BOOL -> struct.getBoolean(field.getName());
            case JSON -> struct.getJson(field.getName());
            case PG_JSONB -> struct.getPgJsonb(field.getName());
            case BYTES -> struct.getBytes(field.getName()).toBase64();
            case STRING, PG_NUMERIC -> struct.getString(field.getName());
            case INT64 -> struct.getLong(field.getName());
            case FLOAT32 -> struct.getFloat(field.getName());
            case FLOAT64 -> struct.getDouble(field.getName());
            case NUMERIC -> struct.getBigDecimal(field.getName()).toPlainString();
            case DATE -> {
                final Date date = struct.getDate(field.getName());
                yield LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
            }
            case TIMESTAMP -> {
                final Timestamp timestamp = struct.getTimestamp(field.getName());
                yield Instant.ofEpochMilli(timestamp.toSqlTimestamp().toInstant().toEpochMilli());
            }
            case STRUCT -> convert(struct);
            case ARRAY -> switch (field.getType().getArrayElementType().getCode()) {
                case BOOL -> struct.getBooleanList(field.getName());
                case STRING -> struct.getStringList(field.getName());
                case JSON -> struct.getJsonList(field.getName());
                case PG_JSONB -> struct.getPgJsonbList(field.getName());
                case BYTES -> struct.getBytesList(field.getName())
                        .stream()
                        .map(ByteArray::toBase64)
                        .collect(Collectors.toList());
                case INT64 -> struct.getLongList(field.getName());
                case FLOAT32 -> struct.getFloatList(field.getName());
                case FLOAT64 -> struct.getDoubleList(field.getName());
                case DATE -> struct.getDateList(field.getName())
                        .stream()
                        .map(date -> LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                        .collect(Collectors.toList());
                case TIMESTAMP -> struct.getTimestampList(field.getName())
                        .stream()
                        .map(timestamp -> Instant.ofEpochMilli(timestamp.toSqlTimestamp().toInstant().toEpochMilli()))
                        .collect(Collectors.toList());
                case NUMERIC -> struct.getBigDecimalList(field.getName())
                        .stream()
                        .map(BigDecimal::toPlainString)
                        .collect(Collectors.toList());
                case STRUCT -> struct.getStructList(field.getName())
                        .stream()
                        .map(StructToMapConverter::convert)
                        .collect(Collectors.toList());
                case ARRAY -> throw new IllegalStateException("Array in Array is not supported!");
                default -> null;
            };
            default -> null;
        };
    }
}
