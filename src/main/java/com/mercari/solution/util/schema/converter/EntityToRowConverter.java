package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class EntityToRowConverter {

    private static final String KEY_FIELD_NAME = "__key__";

    private static final Schema KEY_SCHEMA = Schema.builder()
            .addField("namespace", Schema.FieldType.STRING.withNullable(false))
            .addField("app", Schema.FieldType.STRING.withNullable(false))
            .addField("path", Schema.FieldType.STRING.withNullable(false))
            .addField("kind", Schema.FieldType.STRING.withNullable(false))
            .addField("name", Schema.FieldType.STRING.withNullable(true))
            .addField("id", Schema.FieldType.INT64.withNullable(true))
            .build();

    public static Schema addKeyToSchema(final Schema schema) {
        return RowSchemaUtil.toBuilder(schema)
                .addField(KEY_FIELD_NAME, Schema.FieldType.row(KEY_SCHEMA).withNullable(true))
                .build();
    }

    public static Row convert(final Schema schema, final Entity entity) {
        return convert(schema, entity, 0);
    }

    private static Row convert(final Schema schema, final Entity entity, final int depth) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null! " + entity.getKey());
        }
        final Map<String, Object> values = new HashMap<>();
        if(depth == 0 ) {
            try {
                if (schema.hasField(KEY_FIELD_NAME)) {
                    final Row key = convertKeyRecord(entity.getKey());
                    values.put(KEY_FIELD_NAME, key);
                }
            } catch (NullPointerException e) {
                throw new RuntimeException(convertKeyRecord(entity.getKey()).toString(), e);
            }
        }

        for(final Schema.Field field : schema.getFields()) {
            if(field.getName().equals(KEY_FIELD_NAME)) {
                continue;
            }
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
            values.put(field.getName(), convertEntityValue(value, field.getType(), field.getOptions(), depth + 1));
        }
        return Row.withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Object convertEntityValue(final Value value, final Schema.FieldType fieldType, final Schema.Options fieldOptions, final int depth) {
        if (value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {

            if(fieldType.getTypeName().isCollectionType()) {
                return new ArrayList<>();
            }
            return RowSchemaUtil.getDefaultValue(fieldType, fieldOptions);
        }

        switch (fieldType.getTypeName()) {
            case STRING:
                return value.getStringValue();
            case BYTES:
                return value.getBlobValue().toByteArray();
            case BOOLEAN:
                return value.getBooleanValue();
            case BYTE: {
                final Long longValue = value.getIntegerValue();
                return longValue.byteValue();
            }
            case INT16: {
                final Long longValue = value.getIntegerValue();
                return longValue.shortValue();
            }
            case INT32: {
                final Long longValue = value.getIntegerValue();
                return longValue.intValue();
            }
            case INT64:
                return value.getIntegerValue();
            case FLOAT: {
                final Double doubleValue = value.getDoubleValue();
                return doubleValue.floatValue();
            }
            case DOUBLE:
                return value.getDoubleValue();
            case DECIMAL: {
                switch (value.getValueTypeCase()) {
                    case STRING_VALUE:
                        return new BigDecimal(value.getStringValue());
                    case INTEGER_VALUE:
                        return BigDecimal.valueOf(value.getIntegerValue());
                    case DOUBLE_VALUE:
                        return BigDecimal.valueOf(value.getDoubleValue());
                    default:
                        throw new IllegalArgumentException("Not supported decimal type for value: " + value);
                }
            }
            case DATETIME:
                return DateTimeUtil.toJodaInstant(value.getTimestampValue());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            return DateTimeUtil.toLocalDate(value.getStringValue());
                        case INTEGER_VALUE:
                            return LocalDate.ofEpochDay(value.getIntegerValue());
                        case DOUBLE_VALUE:
                            return LocalDate.ofEpochDay(Double.valueOf(value.getDoubleValue()).longValue());
                        default:
                            throw new IllegalStateException("Not supported date value: " + value);
                    }
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            return DateTimeUtil.toLocalTime(value.getStringValue());
                        case INTEGER_VALUE:
                            return LocalTime.ofNanoOfDay(value.getIntegerValue());
                        case DOUBLE_VALUE:
                            return LocalTime.ofNanoOfDay(Double.valueOf(value.getDoubleValue()).longValue());
                        default:
                            throw new IllegalStateException("Not supported time value: " + value);
                    }
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            return RowSchemaUtil.toEnumerationTypeValue(fieldType, value.getStringValue());
                        case INTEGER_VALUE: {
                            final Long longValue = value.getIntegerValue();
                            return new EnumerationType.Value(longValue.intValue());
                        }
                        default:
                            throw new IllegalArgumentException("Not supported enum value: " + value);
                    }
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return DateTimeUtil.toJodaInstant(value.getTimestampValue());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case ROW:
                return convert(fieldType.getRowSchema(), value.getEntityValue(), depth + 1);
            case ITERABLE:
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> convertEntityValue(v, fieldType.getCollectionElementType(), fieldOptions, depth))
                        .collect(Collectors.toList());
            case MAP:
            default:
                return null;
        }
    }

    private static Row convertKeyRecord(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        final String path = key.getPathList().stream()
                .map(e -> String.format("\"%s\", %s", e.getKind(), (e.getName() == null || e.getName().length() == 0) ?
                        Long.toString(e.getId()) : String.format("\"%s\"", e.getName())))
                .collect(Collectors.joining(", "));
        return Row.withSchema(KEY_SCHEMA)
                .withFieldValue("namespace", key.getPartitionId().getNamespaceId())
                .withFieldValue("app", key.getPartitionId().getProjectId())
                .withFieldValue("path", path)
                .withFieldValue("kind", lastPath.getKind())
                .withFieldValue("name", lastPath.getName() == null || lastPath.getName().length() == 0 ? null : lastPath.getName())
                .withFieldValue("id", lastPath.getId() == 0 ? null : lastPath.getId())
                .build();
    }

}
