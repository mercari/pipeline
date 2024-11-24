package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class ElementToProtoConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ElementToProtoConverter.class);

    public static DynamicMessage convert(
            final Schema schema,
            final MElement element) {

        final Descriptors.Descriptor messageDescriptor = schema.getProtobuf().getDescriptor();
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, messageDescriptor, element.asPrimitiveMap());
            case ROW -> RowToProtoConverter.convert(messageDescriptor, (Row) element.getValue());
            default -> throw new IllegalArgumentException("Not supported type");
        };
    }

    public static DynamicMessage convert(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final Map<?, ?> primitives) {

        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);

        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if(!schema.hasField(field.getName())) {
                continue;
            }
            final Schema.FieldType fieldType = schema.getField(field.getName()).getFieldType();
            if(field.isMapField()) {
                final List<Object> list = (List<Object>) convertValue(field, fieldType, primitives.get(field.getName()));
                if(list == null) {
                    LOG.debug("array field: {} value is null for values: {}", field.getName(), primitives);
                    continue;
                }
                for(final Object object : list) {
                    builder.addRepeatedField(field, object);
                }
            } else if(field.isRepeated()) {
                final List<Object> array;
                if(Schema.Type.array.equals(fieldType.getType())) {
                    array = (List) primitives.get(field.getName());
                } else {
                    array = new ArrayList<>();
                    array.add(primitives.get(field.getName()));
                }

                final List<Object> list = (List<Object>) convertValue(field, fieldType, array);
                if(list == null) {
                    LOG.debug("field: {} value of array is null", field.getName());
                    continue;
                }
                for(final Object object : list) {
                    builder.addRepeatedField(field, object);
                }
            } else {
                final Object value = convertValue(field, fieldType, primitives.get(field.getName()));
                if(value == null) {
                    LOG.debug("field: {} value is null for values: {}", field.getName(), primitives);
                    continue;
                }
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
            if(value instanceof Map<?,?> map) {
                return switch (fieldType.getType()) {
                    case element -> createElementMessage(fieldType.getElementSchema(), field, map);
                    case map -> createMapMessage(fieldType.getMapValueType(), field, map);
                    default -> throw new IllegalArgumentException("Not supported field: " + field.getName() + ", value of map: " + fieldType + " for value: " + value);
                };
            } else {
                throw new IllegalArgumentException("Illegal map field type value: " + value);
            }
        } else if(field.isRepeated()) {
            if(Schema.Type.array.equals(fieldType.getType())) {
                final Schema.FieldType arrayFieldType = fieldType.getArrayValueType();
                return switch (value) {
                    case Collection<?> collection -> collection
                            .stream()
                            .map(o -> switch (o) {
                                case Map<?, ?> map -> switch (arrayFieldType.getType()) {
                                    case element -> convert(arrayFieldType.getElementSchema(), field.getMessageType(), map);
                                    case map -> createMapMessage(arrayFieldType.getMapValueType(), field, map);
                                    default -> throw new IllegalArgumentException("Not supported value of repeated map arrayFieldType: " + arrayFieldType + ", for value: " + value);
                                };
                                default -> convertSingleValue(arrayFieldType, field, o);
                            })
                            .collect(Collectors.toList());
                    case Map<?,?> map -> List.of(switch (arrayFieldType.getType()) {
                        case element -> convert(arrayFieldType.getElementSchema(), field.getMessageType(), map);
                        case map -> createMapMessage(arrayFieldType.getMapValueType(), field, map);
                        default -> throw new IllegalArgumentException("Illegal map value of array. arrayFieldType: " + arrayFieldType + ", for value: " + value);
                    });
                    default -> List.of(convertSingleValue(arrayFieldType, field, value));
                };
            } else {
                return switch (value) {
                    case Map<?,?> map -> List.of(switch (fieldType.getType()) {
                        case element -> convert(fieldType.getElementSchema(), field.getMessageType(), map);
                        case map -> createMapMessage(fieldType.getMapValueType(), field, map);
                        default -> throw new IllegalArgumentException("Illegal map value. fieldType: " + fieldType + ", for value: " + value);
                    });
                    default -> List.of(convertSingleValue(fieldType, field, value));
                };
            }
        } else {
            return convertSingleValue(fieldType, field, value);
        }
    }

    private static Object convertSingleValue(Schema.FieldType fieldType, Descriptors.FieldDescriptor field, Object value) {
        return switch (field.getJavaType()) {
            case BOOLEAN -> switch (fieldType.getType()) {
                case bool -> value;
                case string -> Boolean.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (fieldType.getType()) {
                case string -> value;
                case int32, int64, float32, float64, date, time -> value.toString();
                case enumeration -> fieldType.getSymbols().get((Integer) value);
                default -> throw new IllegalArgumentException();
            };
            case INT -> switch (fieldType.getType()) {
                case int32 -> value;
                case int64 -> ((Long) value).intValue();
                case float32 -> ((Float) value).intValue();
                case float64 -> ((Double) value).intValue();
                case string -> Integer.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case LONG -> switch (fieldType.getType()) {
                case int32 -> ((Integer)value).longValue();
                case int64 -> value;
                case float32 -> ((Float) value).longValue();
                case float64 -> ((Double) value).longValue();
                case string -> Long.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case FLOAT -> switch (fieldType.getType()) {
                case int32 -> ((Integer)value).floatValue();
                case int64 -> ((Long) value).floatValue();
                case float32 -> value;
                case float64 -> ((Double) value).floatValue();
                case string -> Float.valueOf((String) value);
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (fieldType.getType()) {
                case int32 -> ((Integer)value).doubleValue();
                case int64 -> ((Long) value).doubleValue();
                case float64 -> value;
                case float32 -> ((Float) value).doubleValue();
                case string -> Double.valueOf((String) value);
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
                                .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom((ByteBuffer) value))
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
                            final LocalDate date = LocalDate.ofEpochDay(((Integer) value));
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                    .setField(field.getMessageType().findFieldByName("month"), date.getMonthValue())
                                    .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                    .build();
                        }
                        case TIME -> {
                            final LocalTime time = LocalTime.ofNanoOfDay((Long)value * 1000L);
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
                            final Timestamp instant = Timestamps.fromMicros((Long) value);
                            yield DynamicMessage.newBuilder(field.getMessageType())
                                    .setField(field.getMessageType().findFieldByName("seconds"), instant.getSeconds())
                                    .setField(field.getMessageType().findFieldByName("nanos"), instant.getNanos())
                                    .build();
                        }
                        case ANY -> Any.newBuilder().setValue(ByteString.copyFromUtf8(value.toString())).build();
                        case EMPTY -> Empty.newBuilder().build();
                        case NULL_VALUE -> NullValue.NULL_VALUE;
                        case CUSTOM -> convert(fieldType.getElementSchema(), field.getMessageType(), (Map<String,Object>) value);
                        default -> throw new IllegalArgumentException("Not supported type: " + field.getJavaType());
                    };
        };
    }

    private static List<DynamicMessage> createElementMessage(
            final Schema schema,
            final Descriptors.FieldDescriptor field,
            final Map<?, ?> map) {

        final List<DynamicMessage> mapMessages = new ArrayList<>();
        for(final Schema.Field rowField : schema.getFields()) {
            final DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(field.getMessageType());
            for(final Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                if(mapField.getName().equals("key")) {
                    mapBuilder.setField(mapField, rowField.getName());
                } else if(mapField.getName().equals("value")) {
                    final Object mapValue = convertValue(mapField, rowField.getFieldType(), map.get(rowField.getName()));
                    mapBuilder.setField(mapField, mapValue);
                } else {
                    throw new IllegalArgumentException("illegal map field: " + mapField.getName());
                }
            }
            final DynamicMessage mapMessage = mapBuilder.build();
            mapMessages.add(mapMessage);
        }
        return mapMessages;
    }

    private static List<DynamicMessage> createMapMessage(
            final Schema.FieldType fieldType,
            final Descriptors.FieldDescriptor field,
            final Map<?, ?> map) {

        final List<DynamicMessage> mapMessages = new ArrayList<>();
        for(final Map.Entry<?,?> entry : map.entrySet()) {
            final DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(field.getMessageType());
            for(final Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                if(mapField.getName().equals("key")) {
                    final Object mapKey = convertValue(mapField, Schema.FieldType.STRING, entry.getKey());
                    mapBuilder.setField(mapField, mapKey);
                } else if(mapField.getName().equals("value")) {
                    final Object mapValue = convertValue(mapField, fieldType, entry.getValue());
                    mapBuilder.setField(mapField, mapValue);
                } else {
                    throw new IllegalArgumentException("illegal mapField: " + mapField);
                }
            }
            final DynamicMessage mapMessage = mapBuilder.build();
            mapMessages.add(mapMessage);
        }
        return mapMessages;
    }

}
