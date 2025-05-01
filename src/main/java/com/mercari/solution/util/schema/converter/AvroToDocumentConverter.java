package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;


public class AvroToDocumentConverter {

    public static Document.Builder convertBuilder(final Schema schema, final GenericRecord record) {
        final Document.Builder builder = Document.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            if(FirestoreUtil.NAME_FIELD.equals(field.name())) {
                continue;
            }
            final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
            builder.putFields(field.name(), getValue(field.schema(), value));
        }
        return builder;
    }

    public static MapValue convertMapValue(final Schema schema, final GenericRecord record) {
        final MapValue.Builder builder = MapValue.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
            builder.putFields(field.name(), getValue(field.schema(), value));
        }
        return builder.build();
    }

    private static Value getValue(final Schema schema, final Object v) {
        if(v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return switch (schema.getType()) {
            case BOOLEAN -> Value.newBuilder().setBooleanValue(((Boolean) v)).build();
            case ENUM, STRING -> Value.newBuilder().setStringValue(v.toString()).build();
            case FIXED, BYTES -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(schema, (ByteBuffer) v);
                    yield Value.newBuilder().setStringValue(decimal.toString()).build();
                }
                yield Value.newBuilder().setBytesValue(ByteString.copyFrom((ByteBuffer) v)).build();
            }
            case INT -> {
                final Integer i = (Integer) v;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    yield Value.newBuilder()
                            .setStringValue(LocalDate.ofEpochDay(i.longValue()).format(DateTimeFormatter.ISO_LOCAL_DATE))
                            .build();
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    yield Value.newBuilder()
                            .setStringValue(LocalTime.ofNanoOfDay(i.longValue() * 1000_000L).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                } else {
                    yield Value.newBuilder().setIntegerValue(i.longValue()).build();
                }
            }
            case LONG -> {
                final Long l = (Long) v;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    yield Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(l/1000L)).build();
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    yield Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(l)).build();
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    yield Value.newBuilder()
                            .setStringValue(LocalTime.ofNanoOfDay(l * 1000L).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                } else {
                    yield Value.newBuilder().setIntegerValue(l).build();
                }
            }
            case FLOAT -> Value.newBuilder().setDoubleValue(((Float) v)).build();
            case DOUBLE -> Value.newBuilder().setDoubleValue(((Double) v)).build();
            case RECORD -> Value.newBuilder().setMapValue(convertMapValue(schema, (GenericRecord) v)).build();
            case ARRAY -> Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder()
                                .addAllValues(((List<Object>)v).stream()
                                        .map(c -> getValue(schema.getElementType(), c))
                                        .collect(Collectors.toList()))
                                .build())
                        .build();
            case UNION -> getValue(AvroSchemaUtil.unnestUnion(schema), v);
            default -> throw new RuntimeException("Not supported fieldType: " + schema);
        };
    }

}
