package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class RowToDocumentConverter {

    public static Document.Builder convertBuilder(final Schema schema, final Row row) {
        final Document.Builder builder = Document.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            if(FirestoreUtil.NAME_FIELD.equals(field.getName())) {
                continue;
            }
            builder.putFields(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
        }
        return builder;
    }

    public static MapValue convertMapValue(final Schema schema, final Row row) {
        final MapValue.Builder builder = MapValue.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
        }
        return builder.build();
    }

    public static Value getValueFromString(final Schema.FieldType fieldType, final String strValue) {
        if(strValue == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return switch (fieldType.getTypeName()) {
            case BOOLEAN -> getValue(fieldType, Boolean.valueOf(strValue));
            case STRING -> getValue(fieldType, strValue);
            case BYTES -> getValue(fieldType, Base64.getDecoder().decode(strValue));
            case INT16 -> getValue(fieldType, Short.valueOf(strValue));
            case INT32 -> getValue(fieldType, Integer.valueOf(strValue));
            case INT64 -> getValue(fieldType, Long.valueOf(strValue));
            case FLOAT -> getValue(fieldType, Float.valueOf(strValue));
            case DOUBLE -> getValue(fieldType, Double.valueOf(strValue));
            case DECIMAL -> getValue(fieldType, BigDecimal.valueOf(Double.parseDouble(strValue)));
            case DATETIME -> getValue(fieldType, Instant.parse(strValue));
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield getValue(fieldType, strValue);
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield getValue(fieldType, strValue);
                } else if (RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    yield getValue(fieldType, Instant.parse(strValue));
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    yield getValue(fieldType, strValue);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static Value getValue(final Schema.FieldType fieldType, final Object v) {
        if(v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return switch (fieldType.getTypeName()) {
            case BOOLEAN -> Value.newBuilder().setBooleanValue(((Boolean) v)).build();
            case STRING -> Value.newBuilder().setStringValue(((String) v)).build();
            case BYTES -> Value.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) v)).build();
            case BYTE -> Value.newBuilder().setIntegerValue(((Byte) v).longValue()).build();
            case INT16 -> Value.newBuilder().setIntegerValue(((Short) v).longValue()).build();
            case INT32 -> Value.newBuilder().setIntegerValue(((Integer) v).longValue()).build();
            case INT64 -> Value.newBuilder().setIntegerValue(((Long) v)).build();
            case FLOAT -> Value.newBuilder().setDoubleValue(((Float) v)).build();
            case DOUBLE -> Value.newBuilder().setDoubleValue(((Double) v)).build();
            case DECIMAL -> Value.newBuilder().setStringValue(((BigDecimal) v).toString()).build();
            case DATETIME -> Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant) v)).build();
            case LOGICAL_TYPE -> {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield Value.newBuilder().setStringValue((String)v).build();
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield Value.newBuilder().setStringValue((String)v).build();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    yield Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant)v)).build();
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    yield Value.newBuilder().setStringValue((String)v).build();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW -> Value.newBuilder().setMapValue(convertMapValue(fieldType.getRowSchema(), (Row) v)).build();
            case ARRAY, ITERABLE -> Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder()
                                .addAllValues(((List<Object>)v).stream()
                                        .map(c -> getValue(fieldType.getCollectionElementType(), c))
                                        .collect(Collectors.toList()))
                                .build())
                        .build();
            default -> {
                throw new RuntimeException("Not supported fieldType: " + fieldType);
            }
        };
    }

}
