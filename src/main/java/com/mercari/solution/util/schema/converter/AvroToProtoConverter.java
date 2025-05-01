package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AvroToProtoConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToProtoConverter.class);

    public static DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final GenericRecord record) {
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
        if(record == null) {
            return builder.build();
        }
        final Schema schema = record.getSchema();
        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            final Schema fieldSchema;
            try {
                fieldSchema = AvroSchemaUtil.unnestUnion(schema.getField(field.getName()).schema());
            } catch (NullPointerException e) { // just log and re-throw.
                LOG.error("Field of message descriptor: {} not found in schema: {}", field.getName(), schema);
                throw e;
            }
            if(field.isRepeated()) {
                if(field.isMapField()) {
                    // NOT support map
                    continue;
                } else {
                    final List<Object> objects = Optional.ofNullable((List)record.get(field.getName())).orElse(new ArrayList());
                    for(final Object value : objects) {
                        builder.addRepeatedField(field, convertValue(field, AvroSchemaUtil.unnestUnion(fieldSchema.getElementType()), value));
                    }
                }
            } else {
                final Object value = convertValue(field, fieldSchema, record.get(field.getName()));
                builder.setField(field, value);
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Descriptors.FieldDescriptor field, final Schema schema, final Object value) {

        if(value == null) {
            return null;
        }

        switch (field.getJavaType()) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return value;
            case STRING:
                return value.toString();
            case ENUM: {
                return field.getEnumType().findValueByName(value.toString());
            }
            case BYTE_STRING:
                return ByteString.copyFrom(((ByteBuffer) value));
            case MESSAGE: {
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value)
                                .build();
                    case BYTES_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom((ByteBuffer) value))
                                .build();
                    case STRING_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.toString())
                                .build();
                    case INT32_VALUE:
                    case UINT32_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Integer)value)
                                .build();
                    case INT64_VALUE:
                    case UINT64_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Long)value)
                                .build();
                    case FLOAT_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Float)value)
                                .build();
                    case DOUBLE_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Double)value)
                                .build();
                    case DATE: {
                        if(!schema.getType().equals(Schema.Type.INT)) {
                            throw new IllegalArgumentException("Date field: " + field.getName() + " type is not INT but " + schema.getType().getName() + " with LogicalType: " + schema.getLogicalType());
                        }
                        final int epochDay = (Integer) value;
                        final LocalDate date = LocalDate.ofEpochDay(epochDay);
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), date.getMonthValue())
                                .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                .build();
                    }
                    case TIME: {
                        if(!schema.getType().equals(Schema.Type.INT) && !schema.getType().equals(Schema.Type.LONG)) {
                            throw new IllegalArgumentException("Time field: " + field.getName() + " type is not INT,LONG but " + schema.getType().getName() + " with LogicalType: " + schema.getLogicalType());
                        }

                        final LocalTime time;
                        if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                            time = LocalTime.ofNanoOfDay(1000L * 1000L * (Integer) value);
                        } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                            time = LocalTime.ofNanoOfDay(1000L * (Long) value);
                        } else {
                            throw new IllegalArgumentException("Time field: " + field.getName() + " logicalType is illegal: " + schema.getLogicalType());
                        }
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                .build();
                    }
                    case DATETIME: {
                        final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                        if(!schema.getType().equals(Schema.Type.LONG)) {
                            throw new IllegalArgumentException("DateTime field: " + field.getName() + " type is not LONG but " + schema.getType().getName() + " with LogicalType: " + schema.getLogicalType());
                        }

                        final Long epoch = (Long) value;
                        final LocalDateTime dt;
                        if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                            dt = LocalDateTime.ofEpochSecond(epoch/1000, (int)(1000 * 1000 * (epoch%1000)), ZoneOffset.UTC);
                        } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                            dt = LocalDateTime.ofEpochSecond(epoch/1000_000, (int)(1000 * (epoch%1000_000)), ZoneOffset.UTC);
                        } else {
                            throw new IllegalArgumentException("DateTime field: " + field.getName() + " logicalType is illegal: " + schema.getLogicalType());
                        }

                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(timezone,
                                        DynamicMessage.newBuilder(timezone.getMessageType())
                                                .setField(timezone.getMessageType().findFieldByName("id"), ZoneOffset.UTC.getId())
                                                .build())
                                .setField(field.getMessageType().findFieldByName("year"), dt.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), dt.getMonthValue())
                                .setField(field.getMessageType().findFieldByName("day"), dt.getDayOfMonth())
                                .setField(field.getMessageType().findFieldByName("hours"), dt.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), dt.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), dt.getSecond())
                                .build();
                    }
                    case TIMESTAMP:
                        if(!schema.getType().equals(Schema.Type.LONG)) {
                            throw new IllegalArgumentException("Timestamp field: " + field.getName() + " type is not LONG but " + schema.getType().getName() + " with LogicalType: " + schema.getLogicalType());
                        }

                        final Long epoch = (Long) value;
                        final java.time.Instant instant;
                        if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                            instant = java.time.Instant.ofEpochSecond(epoch/1000, (int)(1000 * 1000 * (epoch%1000)));
                        } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                            instant = java.time.Instant.ofEpochSecond(epoch/1000_000, (int)(1000 * (epoch%1000_000)));
                        } else {
                            throw new IllegalArgumentException("DateTime field: " + field.getName() + " logicalType is illegal: " + schema.getLogicalType());
                        }

                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("seconds"), instant.getEpochSecond())
                                .setField(field.getMessageType().findFieldByName("nanos"), instant.getNano())
                                .build();
                    case ANY: {
                        return Any.newBuilder().setValue(ByteString.copyFromUtf8(value.toString())).build();
                    }
                    case EMPTY: {
                        return Empty.newBuilder().build();
                    }
                    case NULL_VALUE:
                        return NullValue.NULL_VALUE;
                    case CUSTOM:
                    default: {
                        return convert(field.getMessageType(), (GenericRecord) value);
                    }
                }

            }
            default: {
                throw new IllegalArgumentException("");
            }
        }
    }

}
