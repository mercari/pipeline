package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToRowConverter {

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        final Schema.Builder builder = Schema.builder();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            builder.addField(field.getName(), convertFieldType(field));
        }
        return builder.build();
    }

    public static Schema.Builder convertSchemaBuilder(final Descriptors.Descriptor messageType) {
        final Schema.Builder builder = Schema.builder();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            builder.addField(field.getName(), convertFieldType(field));
        }
        return builder;
    }

    public static Row convert(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final byte[] bytes,
            final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(schema, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Row convert(
            final Schema schema,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        final Row.FieldValueBuilder builder = convertBuilder(schema, message.getDescriptorForType(), message, printer);
        return builder.build();
    }

    public static Row convert(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        final Row.FieldValueBuilder builder = convertBuilder(schema, messageDescriptor, message, printer);
        return builder.build();
    }

    public static Row.FieldValueBuilder convertBuilder(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final byte[] bytes,
            final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convertBuilder(schema, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Row.FieldValueBuilder convertBuilder(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        final Map<String, Object> values = new HashMap<>();
        for (final Schema.Field field : schema.getFields()) {
            final Descriptors.FieldDescriptor fieldDescriptor = ProtoSchemaUtil.getField(messageDescriptor, field.getName());
            if(fieldDescriptor == null) {
                continue;
            }
            final Object value = convertValue(fieldDescriptor, message.getField(fieldDescriptor), printer);
            values.put(field.getName(), value);
        }
        return Row.withSchema(schema).withFieldValues(values);
    }

    private static Schema.FieldType convertFieldType(Descriptors.FieldDescriptor field) {
        //final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema.FieldType elementFieldType = switch (field.getJavaType()) {
            case BOOLEAN -> Schema.FieldType.BOOLEAN.withNullable(false);
            case ENUM -> {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                yield Schema.FieldType.logicalType(EnumerationType.create(enumNames)).withNullable(false);
            }
            case STRING -> Schema.FieldType.STRING.withNullable(!field.isRepeated());
            case BYTE_STRING -> Schema.FieldType.BYTES.withNullable(false);
            case INT -> Schema.FieldType.INT32.withNullable(false);
            case LONG -> Schema.FieldType.INT64.withNullable(false);
            case FLOAT -> Schema.FieldType.FLOAT.withNullable(false);
            case DOUBLE -> Schema.FieldType.DOUBLE.withNullable(false);
            case MESSAGE -> {
                if(field.isMapField()) {
                    Descriptors.FieldDescriptor keyField = null;
                    Descriptors.FieldDescriptor valueField = null;
                    for(Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                        if("key".equals(mapField.getName())) {
                            keyField = mapField;
                        } else if("value".equals(mapField.getName())) {
                            valueField = mapField;
                        }
                    }
                    yield Schema.FieldType.map(
                            convertFieldType(keyField),
                            convertFieldType(valueField))
                            .withNullable(false);
                }
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> Schema.FieldType.BOOLEAN.withNullable(false);
                    case STRING_VALUE -> Schema.FieldType.STRING.withNullable(false);
                    case BYTES_VALUE -> Schema.FieldType.BYTES.withNullable(false);
                    case INT32_VALUE -> Schema.FieldType.INT32.withNullable(false);
                    case INT64_VALUE -> Schema.FieldType.INT64.withNullable(false);
                    case FLOAT_VALUE -> Schema.FieldType.FLOAT.withNullable(false);
                    case DOUBLE_VALUE -> Schema.FieldType.DOUBLE.withNullable(false);
                    case UINT32_VALUE -> Schema.FieldType.INT32.withNullable(false);
                    case UINT64_VALUE -> Schema.FieldType.INT64.withNullable(false);
                    case DATE -> CalciteUtils.DATE;
                    case TIME -> CalciteUtils.TIME;
                    case DATETIME, TIMESTAMP -> Schema.FieldType.DATETIME.withNullable(false);
                    case ANY -> Schema.FieldType.STRING.withNullable(false);
                    case NULL_VALUE, EMPTY -> Schema.FieldType.STRING.withNullable(true);
                    default -> Schema.FieldType.row(convertSchema(field.getMessageType())).withNullable(true);
                };
            }
            default -> throw new IllegalArgumentException(field.getName() + " is not supported for beam row type.");
        };

        if(field.isRepeated() && !field.isMapField()) {
            return Schema.FieldType.array(elementFieldType).withNullable(false);
        } else {
            return elementFieldType;
        }

    }

    private static Object convertValue(final Descriptors.FieldDescriptor field,
                                       final Object value,
                                       final JsonFormat.Printer printer) {

        if(field.isRepeated()) {
            if(field.isMapField()) {
                if(value == null) {
                    return new HashMap<>();
                }
                final Descriptors.FieldDescriptor keyFieldDescriptor = field.getMessageType().findFieldByName("key");
                final Descriptors.FieldDescriptor valueFieldDescriptor = field.getMessageType().getFields().stream()
                        .filter(f -> f.getName().equals("value"))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Map value not found for field: " + field));
                return ((List<DynamicMessage>) value).stream()
                        .collect(Collectors.toMap(
                                e -> e.getField(keyFieldDescriptor),
                                e -> convertValue(valueFieldDescriptor, e.getField(field.getMessageType().findFieldByName("value")), printer)));
            }
            if(value == null) {
                return new ArrayList<>();
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(field, v, printer))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return getValue(field, value, printer);
    }

    public static Object getValue(final Descriptors.FieldDescriptor field,
                                  final Object value,
                                  final JsonFormat.Printer printer) {

        boolean isNull = value == null;

        return switch (field.getJavaType()) {
            case BOOLEAN -> !isNull && (Boolean)value;
            case INT -> isNull ? 0 : value;
            case LONG -> isNull ? 0l : value;
            case FLOAT -> isNull ? 0f : value;
            case DOUBLE -> isNull ? 0d : value;
            case STRING -> isNull ? "" : value;
            case ENUM -> {
                if(isNull) {
                    yield new EnumerationType.Value(0);
                }
                yield new EnumerationType.Value(((Descriptors.EnumValueDescriptor)value).getIndex());
            }
            case BYTE_STRING -> isNull ? ByteArray.copyFrom("").toByteArray() : ((ByteString) value).toByteArray();
            case MESSAGE -> {
                final Object object = ProtoSchemaUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                isNull = object == null;
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> !isNull && ((BoolValue) object).getValue();
                    case BYTES_VALUE -> isNull ? ByteArray.copyFrom("").toByteArray() : ByteBuffer.wrap(((BytesValue) object).getValue().toByteArray());
                    case STRING_VALUE -> isNull ? "" : ((StringValue) object).getValue();
                    case INT32_VALUE -> isNull ? 0 : ((Int32Value) object).getValue();
                    case INT64_VALUE -> isNull ? 0L :((Int64Value) object).getValue();
                    case UINT32_VALUE -> isNull ? 0 :((UInt32Value) object).getValue();
                    case UINT64_VALUE -> isNull ? 0L :((UInt64Value) object).getValue();
                    case FLOAT_VALUE -> isNull ? 0f :((FloatValue) object).getValue();
                    case DOUBLE_VALUE -> isNull ? 0d :((DoubleValue) object).getValue();
                    case DATE -> {
                        if(isNull) {
                            yield LocalDate.of(1, 1, 1);
                        }
                        final Date date = (Date) object;
                        yield LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
                    }
                    case TIME -> {
                        if(isNull) {
                            yield LocalTime.of(0, 0, 0, 0);
                        }
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        yield LocalTime.of(timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
                    }
                    case DATETIME -> {
                        if(isNull) {
                            long epochMilli = LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli();
                            yield org.joda.time.Instant.ofEpochMilli(epochMilli);
                        }
                        final DateTime dt = (DateTime) object;
                        long epochMilli = LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli();
                        yield org.joda.time.Instant.ofEpochMilli(epochMilli);
                    }
                    case TIMESTAMP -> {
                        if (isNull) {
                            long epochMilli = LocalDateTime.of(
                                            1, 1, 1,
                                            0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli();
                            yield org.joda.time.Instant.ofEpochMilli(epochMilli);
                        }
                        yield org.joda.time.Instant.ofEpochMilli(Timestamps.toMillis((Timestamp) object));
                    }
                    case ANY -> {
                        if(isNull) {
                            yield "";
                        }
                        final Any any = (Any) object;
                        try {
                            yield printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            yield any.getValue().toStringUtf8();
                        }
                    }
                    case EMPTY, NULL_VALUE -> null;
                    default -> {
                        final Schema schema = convertSchema(field.getMessageType());
                        yield convert(schema, field.getMessageType(), (DynamicMessage) value, printer);
                    }
                };
            }
            default -> throw new IllegalStateException("Not support data type: " + field);
        };
    }

}
