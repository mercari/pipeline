package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;

import java.util.stream.Collectors;

public class EntityToDocumentConverter {

    public static Document.Builder convert(final Schema schema, final Entity entity) {

        final Document.Builder builder = Document.newBuilder();
        for(Schema.Field field : schema.getFields()) {
            final com.google.datastore.v1.Value value = entity.getPropertiesOrDefault(field.getName(), null);
            builder.putFields(field.getName(), convertValue(field.getType(), value));
        }
        return builder;
    }

    private static MapValue convertMapValue(final Schema schema, final Entity entity) {
        final MapValue.Builder builder = MapValue.newBuilder();
        if(entity == null) {
            return builder.build();
        }
        for(final Schema.Field field : schema.getFields()) {
            final com.google.datastore.v1.Value value = entity.getPropertiesOrDefault(field.getName(), null);
            builder.putFields(field.getName(), convertValue(field.getType(), value));
        }
        return builder.build();
    }

    private static Value convertValue(final Schema.FieldType fieldType, final com.google.datastore.v1.Value value) {
        if(value == null || NullValue.NULL_VALUE.equals(value.getNullValue())) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue(value.getBooleanValue()).build();
            case BYTES:
                return Value.newBuilder().setBytesValue(value.getBlobValue()).build();
            case STRING:
                return Value.newBuilder().setStringValue(value.getStringValue()).build();
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
                return Value.newBuilder().setIntegerValue(value.getIntegerValue()).build();
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return Value.newBuilder().setDoubleValue(value.getDoubleValue()).build();
            case DATETIME:
                return Value.newBuilder().setTimestampValue(value.getTimestampValue()).build();
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return Value.newBuilder().setStringValue(value.getStringValue()).build();
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return Value.newBuilder().setStringValue(value.getStringValue()).build();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Value.newBuilder().setTimestampValue(value.getTimestampValue()).build();
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return Value.newBuilder().setStringValue(value.getStringValue()).build();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case MAP:
            case ROW: {
                final Entity childEntity = value.getEntityValue();
                return Value.newBuilder()
                        .setMapValue(convertMapValue(fieldType.getRowSchema(), childEntity))
                        .build();
            }
            case ITERABLE:
            case ARRAY:
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case BOOLEAN:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setBooleanValue(o.getBooleanValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case BYTES:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setBytesValue(o.getBlobValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case STRING:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setStringValue(o.getStringValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case BYTE:
                    case INT16:
                    case INT32:
                    case INT64:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setIntegerValue(o.getIntegerValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setDoubleValue(o.getDoubleValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case DATETIME:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> Value.newBuilder().setTimestampValue(o.getTimestampValue()).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case LOGICAL_TYPE:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(o -> {
                                                    if(RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                                                        return Value.newBuilder().setStringValue(o.getStringValue()).build();
                                                    } else if(RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                                                        return Value.newBuilder().setStringValue(o.getStringValue()).build();
                                                    } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType.getCollectionElementType())) {
                                                        return Value.newBuilder().setTimestampValue(o.getTimestampValue()).build();
                                                    } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                                                        return Value.newBuilder().setStringValue(o.getStringValue()).build();
                                                    } else {
                                                        throw new IllegalArgumentException(
                                                                "Unsupported Beam logical type: " + fieldType.getCollectionElementType().getLogicalType().getIdentifier());
                                                    }
                                                })
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case MAP:
                    case ROW:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                        .addAllValues((value.getArrayValue().getValuesList().stream()
                                                .map(child -> Value.newBuilder().setMapValue(convertMapValue(fieldType.getCollectionElementType().getRowSchema(), child.getEntityValue())).build())
                                                .collect(Collectors.toList())))
                                        .build())
                                .build();
                    case ITERABLE:
                    case ARRAY:
                    default:
                        throw new IllegalArgumentException("Array in Array is not supported!");
                }
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

}
