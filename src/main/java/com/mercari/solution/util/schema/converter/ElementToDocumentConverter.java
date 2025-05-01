package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.gcp.FirestoreUtil;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElementToDocumentConverter {

    public static Document.Builder convertBuilder(Schema schema, final MElement element) {
        return switch (element.getType()) {
            case ELEMENT -> convertBuilder(schema, (Map<String, Object>) element.getValue());
            case AVRO -> AvroToDocumentConverter.convertBuilder(schema.getAvroSchema(), (GenericRecord) element.getValue());
            case ROW -> RowToDocumentConverter.convertBuilder(schema.getRowSchema(), (org.apache.beam.sdk.values.Row) element.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static Document.Builder convertBuilder(final Schema schema, final Map<String, Object> values) {
        final Document.Builder builder = Document.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            if(FirestoreUtil.NAME_FIELD.equals(field.getName())) {
                continue;
            }
            builder.putFields(field.getName(), getValue(field.getFieldType(), values.get(field.getName())));
        }
        return builder;
    }

    public static MapValue convertMapValue(final Schema schema, final Map<String, Object> values) {
        final MapValue.Builder builder = MapValue.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.getName(), getValue(field.getFieldType(), values.get(field.getName())));
        }
        return builder.build();
    }

    public static Value getValueFromString(final Schema.FieldType fieldType, final String strValue) {
        if(strValue == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return switch (fieldType.getType()) {
            case bool -> getValue(fieldType, Boolean.valueOf(strValue));
            case string -> getValue(fieldType, strValue);
            case bytes -> getValue(fieldType, Base64.getDecoder().decode(strValue));
            case int16 -> getValue(fieldType, Short.valueOf(strValue));
            case int32 -> getValue(fieldType, Integer.valueOf(strValue));
            case int64 -> getValue(fieldType, Long.valueOf(strValue));
            case float32 -> getValue(fieldType, Float.valueOf(strValue));
            case float64 -> getValue(fieldType, Double.valueOf(strValue));
            case decimal -> getValue(fieldType, BigDecimal.valueOf(Double.parseDouble(strValue)));
            case timestamp -> getValue(fieldType, Instant.parse(strValue));
            case date -> getValue(fieldType, strValue);
            case time -> getValue(fieldType, strValue);
            case enumeration -> getValue(fieldType, strValue);
            default -> throw new IllegalArgumentException();
        };
    }

    private static Value getValue(final Schema.FieldType fieldType, final Object v) {
        if(v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return switch (fieldType.getType()) {
            case bool -> Value.newBuilder().setBooleanValue(((Boolean) v)).build();
            case string -> Value.newBuilder().setStringValue(((String) v)).build();
            case bytes -> Value.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) v)).build();
            case int8 -> Value.newBuilder().setIntegerValue(((Byte) v).longValue()).build();
            case int16 -> Value.newBuilder().setIntegerValue(((Short) v).longValue()).build();
            case int32 -> Value.newBuilder().setIntegerValue(((Integer) v).longValue()).build();
            case int64 -> Value.newBuilder().setIntegerValue(((Long) v)).build();
            case float32 -> Value.newBuilder().setDoubleValue(((Float) v)).build();
            case float64 -> Value.newBuilder().setDoubleValue(((Double) v)).build();
            case decimal -> Value.newBuilder().setStringValue(((BigDecimal) v).toString()).build();
            case timestamp -> Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant) v)).build();
            case date -> Value.newBuilder().setStringValue((String)v).build();
            case time -> Value.newBuilder().setStringValue((String)v).build();
            case enumeration -> Value.newBuilder().setStringValue((String)v).build();
            case element -> Value.newBuilder().setMapValue(convertMapValue(fieldType.getElementSchema(), (Map) v)).build();
            case array -> Value.newBuilder()
                    .setArrayValue(ArrayValue.newBuilder()
                            .addAllValues(((List<Object>)v).stream()
                                    .map(c -> getValue(fieldType.getArrayValueType(), c))
                                    .collect(Collectors.toList()))
                            .build())
                    .build();
            default -> {
                throw new RuntimeException("Not supported fieldType: " + fieldType);
            }
        };
    }

}
