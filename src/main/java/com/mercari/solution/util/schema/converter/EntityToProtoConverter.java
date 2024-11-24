package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.*;
import com.mercari.solution.util.schema.ProtoSchemaUtil;

import java.time.*;
import java.util.List;
import java.util.stream.Collectors;

public class EntityToProtoConverter {

    public static DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final Entity entity) {
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
            if(field.isRepeated()) {
                if(field.isMapField()) {
                    // NOT support map
                    continue;
                } else {
                    if(value == null
                            || !value.getValueTypeCase().equals(Value.ValueTypeCase.ARRAY_VALUE)) {
                        continue;
                    }
                    final List<Object> objects = value.getArrayValue().getValuesList().stream()
                            .map(v -> convertValue(field, v))
                            .collect(Collectors.toList());
                    for(final Object fieldValue : objects) {
                        builder.addRepeatedField(field, fieldValue);
                    }
                }
            } else {
                final Object fieldValue = convertValue(field, value);
                builder.setField(field, fieldValue);
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Descriptors.FieldDescriptor field, final Value value) {

        if(value == null) {
            return null;
        }

        switch (value.getValueTypeCase()) {
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
                return null;
        }

        switch (field.getJavaType()) {
            case BOOLEAN:
                return value.getBooleanValue();
            case INT:
                return Long.valueOf(value.getIntegerValue()).intValue();
            case LONG:
                return value.getIntegerValue();
            case FLOAT:
                return Double.valueOf(value.getDoubleValue()).floatValue();
            case DOUBLE:
                return value.getDoubleValue();
            case STRING:
                return value.getStringValue();
            case ENUM: {
                return field.getEnumType().findValueByName(value.getStringValue());
            }
            case BYTE_STRING:
                return value.getBlobValue();
            case MESSAGE: {
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.getBooleanValue())
                                .build();
                    case BYTES_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.getBlobValue())
                                .build();
                    case STRING_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.getStringValue())
                                .build();
                    case INT32_VALUE:
                    case UINT32_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), Long.valueOf(value.getIntegerValue()).intValue())
                                .build();
                    case INT64_VALUE:
                    case UINT64_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.getIntegerValue())
                                .build();
                    case FLOAT_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), Double.valueOf(value.getDoubleValue()).floatValue())
                                .build();
                    case DOUBLE_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.getDoubleValue())
                                .build();
                    case DATE: {
                        final LocalDate date = LocalDate.parse(value.getStringValue());
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), date.getMonthValue())
                                .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                .build();
                    }
                    case TIME: {
                        final LocalTime time = LocalTime.parse(value.getStringValue());
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                .build();
                    }
                    case DATETIME: {
                        final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                        Timestamp timestamp = value.getTimestampValue();
                        final ZonedDateTime dt = java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).atZone(ZoneId.of(ZoneOffset.UTC.getId()));
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
                        final Timestamp instant = value.getTimestampValue();
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("seconds"), instant.getSeconds())
                                .setField(field.getMessageType().findFieldByName("nanos"), instant.getNanos())
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
                        switch (value.getValueTypeCase()) {
                            case ENTITY_VALUE:
                                return convert(field.getMessageType(), value.getEntityValue());
                            case KEY_VALUE:
                                // TODO
                            default:
                                return null;
                        }
                    }
                }

            }
            default: {
                throw new IllegalArgumentException("");
            }
        }
    }

}
