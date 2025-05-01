package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.*;
import com.mercari.solution.util.schema.ProtoSchemaUtil;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProtoToStructConverter {

    public static Type convertSchema(final Descriptors.Descriptor messageType) {
        final List<Type.StructField> fields = new ArrayList<>();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            fields.add(Type.StructField.of(field.getName(), convertSchema(field)));
        }
        return Type.struct(fields);
    }

    public static Struct convert(final Type type,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final byte[] bytes,
                                 final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(type, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Struct convert(final Type type,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final DynamicMessage message,
                                 final JsonFormat.Printer printer) {

        final Struct.Builder builder = Struct.newBuilder();
        for (final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            final Object value = message.getField(field);
            final boolean isNull = !field.isRepeated() && !message.hasField(field);
            setValue(builder, field, value, printer, isNull);
        }
        return builder.build();
    }

    private static Type convertSchema(Descriptors.FieldDescriptor field) {
        final Type elementType = switch (field.getJavaType()) {
            case BOOLEAN -> Type.bool();
            case ENUM, STRING -> Type.string();
            case BYTE_STRING -> Type.bytes();
            case INT, LONG -> Type.int64();
            case FLOAT -> Type.float32();
            case DOUBLE -> Type.float64();
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
                    yield Type.struct(
                            Type.StructField.of("key", convertSchema(keyField)),
                            Type.StructField.of("value", convertSchema(valueField)));
                }
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> Type.bool();
                    case STRING_VALUE -> Type.string();
                    case BYTES_VALUE -> Type.bytes();
                    case INT32_VALUE, INT64_VALUE, UINT32_VALUE, UINT64_VALUE -> Type.int64();
                    case FLOAT_VALUE -> Type.float32();
                    case DOUBLE_VALUE -> Type.float64();
                    case DATE -> Type.date();
                    case TIME -> Type.string();
                    case DATETIME, TIMESTAMP -> Type.timestamp();
                    case ANY -> Type.string();
                    case NULL_VALUE, EMPTY -> Type.string();
                    case CUSTOM -> convertSchema(field.getMessageType());
                    default -> convertSchema(field.getMessageType());
                };
            }
            default -> throw new IllegalArgumentException(field.getName() + " is not supported for spanner.");
        };

        if(field.isRepeated()) {
            return Type.array(elementType);
        } else {
            return elementType;
        }

    }

    private static void setValue(final Struct.Builder builder,
                                 final Descriptors.FieldDescriptor field,
                                 final Object value,
                                 final JsonFormat.Printer printer,
                                 boolean isNull) {

        //boolean isNull = value == null;

        if(field.isRepeated()) {
            final List<Object> array = isNull ? new ArrayList<>() : ((List<Object>) value);
            switch (field.getJavaType()) {
                case BOOLEAN -> builder.set(field.getName()).toBoolArray(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o != null && (Boolean)o)
                            .collect(Collectors.toList()));
                case LONG -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? 0L : (Long)o)
                            .collect(Collectors.toList()));
                case INT -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? 0 : (Integer)o)
                            .map(Integer::longValue)
                            .collect(Collectors.toList()));
                case FLOAT -> builder.set(field.getName()).toFloat32Array(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? 0f : (Float)o)
                            .collect(Collectors.toList()));
                case DOUBLE -> builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? 0d : (Double)o)
                            .collect(Collectors.toList()));
                case STRING -> builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? "" : (String)o)
                            .collect(Collectors.toList()));
                case ENUM -> builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? field.getEnumType().getValues().get(0) : (Descriptors.EnumValueDescriptor)o)
                            .map(Descriptors.EnumValueDescriptor::getName)
                            .collect(Collectors.toList()));
                case BYTE_STRING -> builder.set(field.getName()).toBytesArray(isNull ? new ArrayList<>() : array.stream()
                            .map(o -> o == null ? ByteString.copyFromUtf8("") : (ByteString)o)
                            .map(ByteString::toByteArray)
                            .map(ByteArray::copyFrom)
                            .collect(Collectors.toList()));
                case MESSAGE -> {
                    if(field.isMapField()) {
                        final Type type = convertSchema(field.getMessageType());
                        builder.set(field.getName()).toStructArray(type, isNull ? new ArrayList<>() : array.stream()
                                .filter(Objects::nonNull)
                                .map(o -> (DynamicMessage)o)
                                .map(m -> convert(type, field.getMessageType(), m, printer))
                                .collect(Collectors.toList()));
                        return;
                    }
                    List<Object> messageArray = array.stream()
                            .map(v -> (DynamicMessage) v)
                            .map(v -> ProtoSchemaUtil.convertBuildInValue(field.getMessageType().getFullName(), v))
                            .toList();
                    switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                        case BOOL_VALUE -> builder.set(field.getName()).toBoolArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o != null && ((BoolValue)o).getValue())
                                    .collect(Collectors.toList()));
                        case BYTES_VALUE -> builder.set(field.getName()).toBytesArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? ByteString.copyFromUtf8("") : ((BytesValue)o).getValue())
                                    .map(ByteString::toByteArray)
                                    .map(ByteArray::copyFrom)
                                    .collect(Collectors.toList()));
                        case STRING_VALUE -> builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? "" : ((StringValue)o).getValue())
                                    .collect(Collectors.toList()));
                        case INT32_VALUE -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0 : ((Int32Value) o).getValue())
                                    .map(i -> (long) i)
                                    .collect(Collectors.toList()));
                        case INT64_VALUE -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0L : ((Int64Value)o).getValue())
                                    .collect(Collectors.toList()));
                        case UINT32_VALUE -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0 : ((UInt32Value) o).getValue())
                                    .map(i -> (long) i)
                                    .collect(Collectors.toList()));
                        case UINT64_VALUE -> builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0L : ((UInt64Value)o).getValue())
                                    .collect(Collectors.toList()));
                        case FLOAT_VALUE -> builder.set(field.getName()).toFloat32Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0f : ((FloatValue) o).getValue())
                                    .collect(Collectors.toList()));
                        case DOUBLE_VALUE -> builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? 0d : ((DoubleValue) o).getValue())
                                    .collect(Collectors.toList()));
                        case DATE -> builder.set(field.getName()).toDateArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? Date.newBuilder().setYear(1).setMonth(1).setDay(1).build() : (Date)o)
                                    .map(d -> com.google.cloud.Date.fromYearMonthDay(d.getYear(), d.getMonth(), d.getDay()))
                                    .collect(Collectors.toList()));
                        case TIME -> builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ?
                                            TimeOfDay.newBuilder().setHours(0).setMinutes(0).setSeconds(0).setNanos(0).build() : (TimeOfDay)o)
                                    .map(t -> String.format("%02d:%02d:%02d", t.getHours(), t.getMinutes(), t.getSeconds()))
                                    .collect(Collectors.toList()));
                        case DATETIME -> builder.set(field.getName()).toTimestampArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> o == null ? DateTime.newBuilder()
                                            .setYear(1).setMonth(1).setDay(1)
                                            .setHours(0).setMinutes(0).setSeconds(0).setNanos(0)
                                            .setTimeZone(TimeZone.newBuilder().setId("UTC")).build() : (DateTime)o)
                                    .map(dt -> LocalDateTime
                                            .of(dt.getYear(), dt.getMonth(), dt.getDay(), dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                            .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                            .toInstant())
                                    .map(i -> com.google.cloud.Timestamp
                                            .ofTimeSecondsAndNanos(i.getEpochSecond(), i.getNano()))
                                    .collect(Collectors.toList()));
                        case TIMESTAMP -> builder.set(field.getName()).toTimestampArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> {
                                        if(o == null) {
                                            final Instant ldt = LocalDateTime.of(
                                                    1, 1, 1,
                                                    0, 0, 0, 0)
                                                    .atOffset(ZoneOffset.UTC)
                                                    .toInstant();
                                            return Timestamp.newBuilder().setSeconds(ldt.getEpochSecond()).setNanos(ldt.getNano()).build();
                                        } else {
                                            return (Timestamp)o;
                                        }
                                    })
                                    .map(t -> com.google.cloud.Timestamp
                                            .ofTimeSecondsAndNanos(t.getSeconds(), t.getNanos()))
                                    .collect(Collectors.toList()));
                        case ANY -> builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : messageArray.stream()
                                    .map(o -> {
                                        if(o == null){
                                            return "";
                                        } else {
                                            Any a = (Any)o;
                                            try {
                                                return printer.print(a);
                                            } catch (InvalidProtocolBufferException e) {
                                                return a.getValue().toStringUtf8();
                                            }
                                        }
                                    })
                                    .collect(Collectors.toList()));
                        case EMPTY, NULL_VALUE -> builder.set(field.getName()).toStringArray(new ArrayList<>());
                        default -> {
                            final Type type = convertSchema(field.getMessageType());
                            builder.set(field.getName()).toStructArray(type, isNull ? new ArrayList<>() : messageArray.stream()
                                    .filter(Objects::nonNull)
                                    .map(m -> convert(type, field.getMessageType(), (DynamicMessage) m, printer))
                                    .collect(Collectors.toList()));
                        }
                    }
                }
                default -> builder.set(field.getName()).to((String) null);
            }
        } else {
            switch (field.getJavaType()) {
                case BOOLEAN -> builder.set(field.getName()).to(isNull ? false : (Boolean)value);
                case LONG -> builder.set(field.getName()).to(isNull ? 0L : (Long)value);
                case INT -> builder.set(field.getName()).to(isNull ? 0 : ((Integer)value).longValue());
                case FLOAT -> builder.set(field.getName()).to(isNull ? 0f : ((Float)value));
                case DOUBLE -> builder.set(field.getName()).to(isNull ? 0d : (Double)value);
                case STRING -> builder.set(field.getName()).to(isNull ? "" : value.toString());
                case ENUM -> builder.set(field.getName()).to(isNull ?
                            field.getEnumType().getValues().get(0).getName() : ((Descriptors.EnumValueDescriptor)value).getName());
                case BYTE_STRING -> builder.set(field.getName()).to(isNull ? ByteArray.copyFrom("") : ByteArray.copyFrom(((ByteString) value).toByteArray()));
                case MESSAGE -> {
                    final Object object = ProtoSchemaUtil
                            .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                    isNull = (object == null);
                    switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                        case BOOL_VALUE -> builder.set(field.getName()).to(!isNull && ((BoolValue) object).getValue());
                        case BYTES_VALUE -> builder.set(field.getName()).to(isNull ? ByteArray.copyFrom("") : ByteArray.copyFrom(((BytesValue) object).getValue().toByteArray()));
                        case STRING_VALUE -> builder.set(field.getName()).to(isNull ? "" : ((StringValue) object).getValue());
                        case INT32_VALUE -> builder.set(field.getName()).to(isNull ? 0l : (long)((Int32Value) object).getValue());
                        case INT64_VALUE -> builder.set(field.getName()).to(isNull ? 0l : ((Int64Value) object).getValue());
                        case UINT32_VALUE -> builder.set(field.getName()).to(isNull ? 0l : (long)((UInt32Value) object).getValue());
                        case UINT64_VALUE -> builder.set(field.getName()).to(isNull ? 0l : ((UInt64Value) object).getValue());
                        case FLOAT_VALUE -> builder.set(field.getName()).to(isNull ? 0f : ((FloatValue) object).getValue());
                        case DOUBLE_VALUE -> builder.set(field.getName()).to(isNull ? 0d : ((DoubleValue) object).getValue());
                        case DATE -> {
                            final Date date = (Date) object;
                            builder.set(field.getName()).to(isNull ?
                                    com.google.cloud.Date.fromYearMonthDay(1, 1, 1) :
                                    com.google.cloud.Date.fromYearMonthDay(date.getYear(), date.getMonth(), date.getDay()));
                        }
                        case TIME -> {
                            final TimeOfDay timeOfDay = (TimeOfDay) object;
                            builder.set(field.getName()).to(isNull ? "00:00:00" : String.format("%02d:%02d:%02d", timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds()));
                        }
                        case DATETIME -> {
                            final DateTime dt = (DateTime) object;
                            final Instant ldt;
                            if(isNull) {
                                ldt = LocalDateTime.of(
                                        1, 1, 1,
                                        0, 0, 0, 0)
                                        .atOffset(ZoneOffset.UTC)
                                        .toInstant();
                            } else {
                                ldt = LocalDateTime.of(
                                        dt.getYear(), dt.getMonth(), dt.getDay(),
                                        dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                        .atOffset(ZoneOffset.ofTotalSeconds((int) dt.getUtcOffset().getSeconds()))
                                        .toInstant();
                            }
                            builder.set(field.getName()).to(com.google.cloud.Timestamp
                                    .ofTimeSecondsAndNanos(ldt.getEpochSecond(), ldt.getNano()));
                        }
                        case TIMESTAMP -> {
                            if(isNull) {
                                final Instant ldt = LocalDateTime.of(
                                        1, 1, 1,
                                        0, 0, 0, 0)
                                        .atOffset(ZoneOffset.UTC)
                                        .toInstant();
                                builder.set(field.getName()).to(com.google.cloud.Timestamp
                                        .ofTimeSecondsAndNanos(ldt.getEpochSecond(), ldt.getNano()));
                            } else {
                                final Timestamp timestamp = (Timestamp) object;
                                builder.set(field.getName()).to(com.google.cloud.Timestamp
                                        .ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos()));
                            }
                        }
                        case ANY -> {
                            final Any any = (Any) object;
                            try {
                                builder.set(field.getName()).to(isNull ? "" : printer.print(any));
                            } catch (InvalidProtocolBufferException e) {
                                builder.set(field.getName()).to(isNull ? "" : any.getValue().toStringUtf8());
                            }
                        }
                        case EMPTY, NULL_VALUE -> builder.set(field.getName()).to("");
                        default -> {
                            final Type type = convertSchema(field.getMessageType());
                            final Struct struct = convert(type, field.getMessageType(), (DynamicMessage) value, printer);
                            builder.set(field.getName()).to(type, struct);
                        }
                    }
                }
                default -> throw new IllegalStateException("Not support data type: " + field);
            }
        }

    }

}
