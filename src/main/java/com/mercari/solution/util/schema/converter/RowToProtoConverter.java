package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.*;


public class RowToProtoConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RowToProtoConverter.class);

    public static DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final Row row) {
        if(row == null) {
            return null;
        }
        final Schema schema = row.getSchema();
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if(!row.getSchema().hasField(field.getName())) {
                LOG.warn("proto field: {} does not exist in row: {}", field.getName(), row);
                continue;
            }
            final Schema.FieldType fieldType = schema.getField(field.getName()).getType();
            if(field.isMapField()) {
                final List<Object> list = (List<Object>) convertValue(field, fieldType, row.getValue(field.getName()));
                for(final Object object : list) {
                    builder.addRepeatedField(field, object);
                }
            } else if(field.isRepeated()) {
                final Collection<?> array;
                if(Schema.TypeName.ARRAY.equals(fieldType.getTypeName())) {
                    array = row.getArray(field.getName());
                } else {
                    array = new ArrayList<>();
                    array.add(row.getValue(field.getName()));
                }

                final List<Object> list = (List<Object>) convertValue(field, fieldType, array);
                for(final Object object : list) {
                    builder.addRepeatedField(field, object);
                }
            } else {
                final Object value = convertValue(field, fieldType, row.getValue(field.getName()));
                builder.setField(field, value);
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Descriptors.FieldDescriptor field, final Schema.FieldType fieldType, final Object value) {

        if(value == null) {
            return null;
        }

        if(field.isMapField()) {
            final List<DynamicMessage> mapMessages = new ArrayList<>();
            if(value instanceof Row row) {
                for(final Schema.Field rowField : row.getSchema().getFields()) {
                    final DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(field.getMessageType());
                    for(final Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                        if(mapField.getName().equals("key")) {
                            mapBuilder.setField(mapField, rowField.getName());
                        } else if(mapField.getName().equals("value")) {
                            final Object mapValue = convertValue(mapField, rowField.getType(), row.getValue(rowField.getName()));
                            mapBuilder.setField(mapField, mapValue);
                        } else {
                            throw new IllegalArgumentException("illegal map field: " + mapField.getName());
                        }
                    }
                    final DynamicMessage mapMessage = mapBuilder.build();
                    mapMessages.add(mapMessage);
                }
            } else if(value instanceof Map<?,?>) {
                final Map<?,?> map = (Map) value;
                for(final Map.Entry<?,?> entry : map.entrySet()) {
                    final DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(field.getMessageType());
                    for(final Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                        if(mapField.getName().equals("key")) {
                            final Object mapKey = convertValue(mapField, fieldType.getMapKeyType(), entry.getKey());
                            mapBuilder.setField(mapField, mapKey);
                        } else if(mapField.getName().equals("value")) {
                            final Object mapValue = convertValue(mapField, fieldType.getMapValueType(), entry.getValue());
                            mapBuilder.setField(mapField, mapValue);
                        } else {
                            throw new IllegalArgumentException();
                        }
                    }
                    final DynamicMessage mapMessage = mapBuilder.build();
                    mapMessages.add(mapMessage);
                }
            } else {
                throw new IllegalArgumentException("Illegal map field type value: " + value);
            }
            return mapMessages;
        } else if(field.isRepeated()) {
            if(value instanceof Collection<?>) {
                return Optional
                        .ofNullable((Collection<?>)value)
                        .orElseGet(ArrayList::new)
                        .stream()
                        .map(o -> {
                            if(o instanceof Row) {
                                return convert(field.getMessageType(), (Row) o);
                            } else {
                                return convertSingleValue(field, fieldType.getCollectionElementType(), o);
                            }
                        })
                        .toList();
            } else if(value instanceof Row) {
                final Object object = convert(field.getMessageType(), (Row) value);
                return List.of(object);
            } else {
                throw new IllegalArgumentException("illegal repeated value: " + value);
            }
        }

        return convertSingleValue(field, fieldType, value);
    }

    private static Object convertSingleValue(Descriptors.FieldDescriptor field, Schema.FieldType fieldType, Object value) {
        return switch (field.getJavaType()) {
            case BOOLEAN -> switch (fieldType.getTypeName()) {
                case BOOLEAN -> value;
                case STRING -> Boolean.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (fieldType.getTypeName()) {
                case STRING -> value;
                case INT32, INT64, FLOAT, DOUBLE -> value.toString();
                case LOGICAL_TYPE -> {
                    if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                        yield value.toString();
                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                        yield RowSchemaUtil.toString(fieldType, (EnumerationType.Value) value);
                    } else {
                        throw new IllegalArgumentException();
                    }
                }
                default -> throw new IllegalArgumentException();
            };
            case INT -> switch (fieldType.getTypeName()) {
                case INT32 -> value;
                case INT64 -> ((Long) value).intValue();
                case FLOAT -> ((Float) value).intValue();
                case DOUBLE -> ((Double) value).intValue();
                case STRING -> Integer.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case LONG -> switch (fieldType.getTypeName()) {
                case INT32 -> ((Integer)value).longValue();
                case INT64 -> value;
                case FLOAT -> ((Float) value).intValue();
                case DOUBLE -> ((Double) value).intValue();
                case STRING -> Long.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case FLOAT -> switch (fieldType.getTypeName()) {
                case INT32 -> ((Integer)value).floatValue();
                case INT64 -> ((Long) value).floatValue();
                case FLOAT -> value;
                case DOUBLE -> ((Double) value).floatValue();
                case STRING -> Float.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (fieldType.getTypeName()) {
                case INT32 -> ((Integer)value).doubleValue();
                case INT64 -> ((Long) value).doubleValue();
                case DOUBLE -> value;
                case FLOAT -> ((Float) value).doubleValue();
                case STRING -> Double.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case ENUM -> {
                final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                yield field.getEnumType().findValueByNumber(enumValue.getValue());
            }
            case BYTE_STRING -> ByteString.copyFrom(((byte[]) value));
            case MESSAGE ->
                    switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                        case BOOL_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value)
                                .build();
                        case BYTES_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom((byte[]) value))
                                .build();
                        case STRING_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.toString())
                                .build();
                        case INT32_VALUE, UINT32_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Integer) value)
                                .build();
                        case INT64_VALUE, UINT64_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Long) value)
                                .build();
                        case FLOAT_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Float) value)
                                .build();
                        case DOUBLE_VALUE -> DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Double) value)
                                .build();
                        case DATE -> {
                            final LocalDate date = (LocalDate) value;
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                    .setField(field.getMessageType().findFieldByName("month"), date.getMonthValue())
                                    .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                    .build();
                        }
                        case TIME -> {
                            final LocalTime time = (LocalTime) value;
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                    .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                    .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                    .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                    .build();
                        }
                        case DATETIME -> {
                            final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                            final org.joda.time.DateTime dt = ((Instant) value).toDateTime(DateTimeZone.UTC);
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(timezone,
                                            DynamicMessage.newBuilder(timezone.getMessageType())
                                                    .setField(timezone.getMessageType().findFieldByName("id"), ZoneOffset.UTC.getId())
                                                    .build())
                                    .setField(field.getMessageType().findFieldByName("year"), dt.getYear())
                                    .setField(field.getMessageType().findFieldByName("month"), dt.getMonthOfYear())
                                    .setField(field.getMessageType().findFieldByName("day"), dt.getDayOfMonth())
                                    .setField(field.getMessageType().findFieldByName("hours"), dt.getHourOfDay())
                                    .setField(field.getMessageType().findFieldByName("minutes"), dt.getMinuteOfHour())
                                    .setField(field.getMessageType().findFieldByName("seconds"), dt.getSecondOfMinute())
                                    .build();
                        }
                        case TIMESTAMP -> {
                            final Timestamp instant = Timestamps.fromMillis(((Instant) value).getMillis());
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(field.getMessageType().findFieldByName("seconds"), instant.getSeconds())
                                    .setField(field.getMessageType().findFieldByName("nanos"), instant.getNanos())
                                    .build();
                        }
                        case ANY -> Any.newBuilder().setValue(ByteString.copyFromUtf8(value.toString())).build();
                        case EMPTY -> Empty.newBuilder().build();
                        case NULL_VALUE -> NullValue.NULL_VALUE;
                        case CUSTOM -> convert(field.getMessageType(), (Row) value);
                        default -> throw new IllegalArgumentException("Not supported type: " + field.getJavaType());
                    };
        };
    }
}

