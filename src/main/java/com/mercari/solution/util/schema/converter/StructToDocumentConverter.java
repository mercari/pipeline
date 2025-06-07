package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Document;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.gcp.FirestoreUtil;

import java.util.stream.Collectors;

public class StructToDocumentConverter {

    public static Document.Builder convert(final Struct struct) {
        return convert(struct.getType(), struct);
    }

    public static Document.Builder convert(final Type type, final Struct struct) {
        final Document.Builder builder = Document.newBuilder();
        for(Type.StructField field : type.getStructFields()) {
            if(FirestoreUtil.NAME_FIELD.equals(field.getName())) {
                continue;
            }
            builder.putFields(field.getName(), convertValue(field, struct));
        }
        return builder;
    }

    private static MapValue convertMapValue(final Type type, final Struct struct) {
        final MapValue.Builder builder = MapValue.newBuilder();
        if(struct == null) {
            return builder.build();
        }
        for(final Type.StructField field : type.getStructFields()) {
            builder.putFields(field.getName(), convertValue(field, struct));
        }
        return builder.build();
    }

    private static Value convertValue(final Type.StructField field, final Struct struct) {
        if(struct.isNull(field.getName())) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        final Value.Builder builder;
        switch (field.getType().getCode()) {
            case BOOL -> builder = Value.newBuilder().setBooleanValue(struct.getBoolean(field.getName()));
            case BYTES -> {
                final byte[] bytes = struct.getBytes(field.getName()).toByteArray();
                builder = Value.newBuilder().setBytesValue(ByteString.copyFrom(bytes));
            }
            case PG_NUMERIC, STRING -> {
                final String stringValue = struct.getString(field.getName());
                builder = Value.newBuilder().setStringValue(stringValue);
            }
            case JSON -> {
                final String stringValue = struct.getJson(field.getName());
                builder = Value.newBuilder().setStringValue(stringValue);
            }
            case PG_JSONB -> {
                final String stringValue = struct.getPgJsonb(field.getName());
                builder = Value.newBuilder().setStringValue(stringValue);
            }
            case INT64 -> builder = Value.newBuilder().setIntegerValue(struct.getLong(field.getName()));
            case FLOAT32 -> builder = Value.newBuilder().setDoubleValue(struct.getFloat(field.getName()));
            case FLOAT64 -> builder = Value.newBuilder().setDoubleValue(struct.getDouble(field.getName()));
            case NUMERIC -> builder = Value.newBuilder().setDoubleValue(struct.getBigDecimal(field.getName()).doubleValue());
            case DATE -> builder = Value.newBuilder().setStringValue(struct.getDate(field.getName()).toString());
            case TIMESTAMP -> builder = Value.newBuilder().setTimestampValue(struct.getTimestamp(field.getName()).toProto());
            case STRUCT -> {
                final Struct childStruct = struct.getStruct(field.getName());
                return Value.newBuilder()
                        .setMapValue(convertMapValue(field.getType(), childStruct))
                        .build();
            }
            case ARRAY -> {
                return switch (field.getType().getArrayElementType().getCode()) {
                    case BOOL -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getBooleanList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setBooleanValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case BYTES -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getBytesList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setBytesValue(ByteString.copyFrom(o.toByteArray())).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case PG_NUMERIC, STRING -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getStringList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setStringValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case JSON -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getJsonList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setStringValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case PG_JSONB -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getPgJsonbList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setStringValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case INT64 -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getLongList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setIntegerValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case FLOAT32 -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getFloatList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setDoubleValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case FLOAT64 -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getDoubleList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setDoubleValue(o).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case NUMERIC -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getBigDecimalList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setDoubleValue(o.doubleValue()).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case DATE -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getDateList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setStringValue(o.toString()).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case TIMESTAMP -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getTimestampList(field.getName()).stream()
                                            .map(o -> Value.newBuilder().setTimestampValue(o.toProto()).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    case STRUCT -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                    .addAllValues((struct.getStructList(field.getName()).stream()
                                            .map(child -> Value.newBuilder().setMapValue(convertMapValue(field.getType().getArrayElementType(), child)).build())
                                            .collect(Collectors.toList())))
                                    .build())
                            .build();
                    default -> throw new IllegalArgumentException("Array in Array is not supported!");
                };
            }
            default -> {
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            }
        }

        return builder.build();
    }
}
