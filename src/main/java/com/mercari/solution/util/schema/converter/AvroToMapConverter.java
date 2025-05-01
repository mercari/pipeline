package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class AvroToMapConverter {

    public static Map<String, Object> convert(final GenericRecord record) {
        return convertWithFields(record, null);
    }

    public static Map<String, Object> convertWithFields(final GenericRecord record, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(record == null) {
            return map;
        }
        for(final Schema.Field field : record.getSchema().getFields()) {
            if(fields == null || fields.contains(field.name())) {
                if(!record.hasField(field.name())) {
                    continue;
                }
                map.put(field.name(), getValue(field.schema(), record.get(field.name())));
            }
        }
        return map;
    }

    private static Object getValue(final Schema schema, final Object value) {
        if(value == null) {
            return null;
        }
        final Schema unnestedSchema = AvroSchemaUtil.unnestUnion(schema);
        return switch (unnestedSchema.getType()) {
            case INT -> {
                final int intValue = (Integer) value;
                if (LogicalTypes.date().equals(unnestedSchema.getLogicalType())) {
                    yield LocalDate.ofEpochDay(intValue);
                } else if (LogicalTypes.timeMillis().equals(unnestedSchema.getLogicalType())) {
                    yield LocalTime.ofNanoOfDay(intValue * 1000 * 1000);
                } else {
                    yield intValue;
                }
            }
            case LONG -> {
                final long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(unnestedSchema.getLogicalType())) {
                    yield java.time.Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(unnestedSchema.getLogicalType())) {
                    yield java.time.Instant.ofEpochMilli(longValue / 1000);
                } else if (LogicalTypes.timeMicros().equals(unnestedSchema.getLogicalType())) {
                    yield LocalTime.ofNanoOfDay(longValue * 1000);
                } else {
                    yield longValue;
                }
            }
            case BOOLEAN, FLOAT, DOUBLE -> value;
            case ENUM, STRING -> value.toString();
            case FIXED, BYTES -> Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
            case RECORD -> convert((GenericRecord) value);
            case ARRAY -> ((List<Object>) value).stream()
                        .map(o -> getValue(unnestedSchema.getElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            default -> null;
        };
    }

}
