package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;
import java.util.stream.Collectors;

public class RowToMapConverter {

    private static final DateTimeFormatter FORMATTER_HH_MM_SS   = DateTimeFormat.forPattern("HH:mm:ss");

    public static Map<String, Object> convert(final Row row) {
        return convertWithFields(row, null);
    }

    public static Map<String, Object> convertWithFields(final Row row, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(row == null) {
            return map;
        }
        for(final Schema.Field field : row.getSchema().getFields()) {
            if(fields == null || fields.contains(field.getName())) {
                map.put(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
            }
        }
        return map;
    }

    private static Object getValue(final Schema.FieldType type, final Object value) {
        if(value == null) {
            return null;
        }
        return switch (type.getTypeName()) {
            case BYTE, INT16, INT32, INT64, BOOLEAN, FLOAT, DOUBLE, STRING, DECIMAL -> value;
            case BYTES -> Base64.getEncoder().encodeToString(((byte[]) value));
            case DATETIME -> java.time.Instant.ofEpochMilli(((ReadableInstant) value).toInstant().getMillis());
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(type)) {
                    yield value;
                } else if (RowSchemaUtil.isLogicalTypeTime(type)) {
                    yield ((Instant) value).toString(FORMATTER_HH_MM_SS);
                } else if (RowSchemaUtil.isLogicalTypeTimestamp(type)) {
                    yield java.time.Instant.ofEpochMilli(((ReadableInstant) value).toInstant().getMillis());
                } else if (RowSchemaUtil.isLogicalTypeEnum(type)) {
                    final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                    yield RowSchemaUtil.toString(type, enumValue);
                } else {
                    throw new IllegalArgumentException("Unsupported Beam logical type: " + type.getLogicalType().getIdentifier());
                }
            }
            case ROW -> convert((Row) value);
            case ITERABLE, ARRAY -> ((List<Object>) value).stream()
                        .map(o -> getValue(type.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            default -> null;
        };
    }

}
