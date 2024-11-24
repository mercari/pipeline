package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToRowConverter {

    public static Row convert(final Schema schema, final Document document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final Map<String,Object> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            if("__name__".equals(field.getName()) && !document.containsFields(field.getName())) {
                values.put(field.getName(), document.getName());
            } else if("__createtime__".equals(field.getName()) && !document.containsFields(field.getName())) {
                values.put(field.getName(), DateTimeUtil.toJodaInstant(document.getCreateTime()));
            } else if("__updatetime__".equals(field.getName()) && !document.containsFields(field.getName())) {
                values.put(field.getName(), DateTimeUtil.toJodaInstant(document.getUpdateTime()));
            } else {
                final Value value = document.getFieldsMap().get(field.getName());
                values.put(field.getName(), getValue(field.getType(), field.getOptions(), value));
            }
        }
        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Row convert(final Schema schema, final MapValue document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final Map<String,Object> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final Value value = document.getFieldsMap().get(field.getName());
            values.put(field.getName(), getValue(field.getType(), field.getOptions(), value));
        }
        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Object getValue(final Schema.FieldType fieldType, final Schema.Options fieldOptions, final Value value) {
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
                return value.getBytesValue().toByteArray();
            case BOOLEAN:
                return value.getBooleanValue();
            case BYTE:
                return Long.valueOf(value.getIntegerValue()).byteValue();
            case INT16:
                return Long.valueOf(value.getIntegerValue()).shortValue();
            case INT32:
                return Long.valueOf(value.getIntegerValue()).intValue();
            case INT64:
                return value.getIntegerValue();
            case FLOAT:
                return Double.valueOf(value.getDoubleValue()).floatValue();
            case DOUBLE:
                return value.getDoubleValue();
            case DECIMAL:
                return new BigDecimal(value.getStringValue());
            case DATETIME:
                return DateTimeUtil.toJodaInstant(value.getTimestampValue());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toLocalDate(value.getStringValue());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime(value.getStringValue());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return DateTimeUtil.toJodaInstant(value.getTimestampValue());
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    final Long epochMicroSecond = DateTimeUtil.toEpochMicroSecond(value.getTimestampValue());
                    return DateTimeUtil.toLocalDateTime(epochMicroSecond);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return value.getStringValue();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case MAP: {
                final Map<String,Object> values = new HashMap<>();
                for(final Map.Entry<String, Value> entry : value.getMapValue().getFieldsMap().entrySet()) {
                    final Object object = getValue(fieldType.getMapValueType(), fieldOptions, entry.getValue());
                    values.put(entry.getKey(), object);
                }
                return values;
            }
            case ROW:
                return convert(fieldType.getRowSchema(), value.getMapValue());
            case ITERABLE:
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> getValue(fieldType.getCollectionElementType(), fieldOptions, v))
                        .collect(Collectors.toList());
            default:
                return null;
        }
    }

}
