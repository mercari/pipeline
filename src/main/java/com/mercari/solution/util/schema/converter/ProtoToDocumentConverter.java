package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProtoToDocumentConverter {

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        return ProtoToRowConverter.convertSchema(messageType);
    }

    public static Document convert(final Schema schema,
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

    public static Document convert(final Schema schema,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final DynamicMessage message,
                                 final JsonFormat.Printer printer) {

        return convert(messageDescriptor, message, printer);
    }

    public static Document convert(final Descriptors.Descriptor messageDescriptor,
                                 final DynamicMessage message,
                                 final JsonFormat.Printer printer) {

        final Document.Builder builder = Document.newBuilder();
        for (final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            builder.putFields(field.getName(), convertValue(field, message.getField(field), printer));
        }
        return builder.build();
    }

    private static Value convertValue(final Descriptors.FieldDescriptor field,
                                      final Object value,
                                      final JsonFormat.Printer printer) {

        if(field.isRepeated()) {
            if(value == null) {
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().build()).build();
            }
            if(field.isMapField()) {
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(((List<DynamicMessage>) value).stream()
                                .filter(Objects::nonNull)
                                .map(m -> Map.of(
                                                "key", convertValue(field.getMessageType().findFieldByName("key"), m.getField(field.getMessageType().findFieldByName("key")), printer),
                                                "value", convertValue(field.getMessageType().findFieldByName("value"), m.getField(field.getMessageType().findFieldByName("value")), printer)
                                        ))
                                .map(e -> Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(e).build()).build())
                                .collect(Collectors.toList())).build()).build();
            }
            return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                            .addAllValues(((List<Object>) value).stream()
                                    .map(v -> getValue(field, v, printer))
                                    .collect(Collectors.toList()))
                            .build())
                    .build();
        }
        return getValue(field, value, printer);
    }

    private static Value getValue(final Descriptors.FieldDescriptor field,
                                  final Object value,
                                  final JsonFormat.Printer printer) {

        boolean isNull = value == null;

        switch (field.getJavaType()) {
            case BOOLEAN -> {
                return Value.newBuilder().setBooleanValue(!isNull && (Boolean) value).build();
            }
            case LONG -> {
                return Value.newBuilder().setIntegerValue(isNull ? 0 : (Long) value).build();
            }
            case INT -> {
                return Value.newBuilder().setIntegerValue(isNull ? 0 : (Integer) value).build();
            }
            case FLOAT -> {
                return Value.newBuilder().setDoubleValue(isNull ? 0f : (Float) value).build();
            }
            case DOUBLE -> {
                return Value.newBuilder().setDoubleValue(isNull ? 0d : (Double) value).build();
            }
            case STRING -> {
                return Value.newBuilder().setStringValue(isNull ? "" : (String) value).build();
            }
            case ENUM -> {
                if (isNull) {
                    return Value.newBuilder()
                            .setStringValue(field.getEnumType().getValues().get(0).getName())
                            .build();
                } else {
                    return Value.newBuilder()
                            .setStringValue(((Descriptors.EnumValueDescriptor) value).getName())
                            .build();
                }
            }
            case BYTE_STRING -> {
                return Value.newBuilder().setBytesValue(isNull ? ByteString.copyFromUtf8("") : (ByteString) value).build();
            }
            case MESSAGE -> {
                final Object object = ProtoSchemaUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                isNull = object == null;
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> {
                        return Value.newBuilder().setBooleanValue(!isNull && ((BoolValue) object).getValue()).build();
                    }
                    case BYTES_VALUE -> {
                        return Value.newBuilder().setBytesValue(isNull ? ByteString.copyFromUtf8("") : ((BytesValue) object).getValue()).build();
                    }
                    case STRING_VALUE -> {
                        return Value.newBuilder().setStringValue(isNull ? "" : ((StringValue) object).getValue()).build();
                    }
                    case INT32_VALUE -> {
                        return Value.newBuilder().setIntegerValue(isNull ? 0 : ((Int32Value) object).getValue()).build();
                    }
                    case INT64_VALUE -> {
                        return Value.newBuilder().setIntegerValue(isNull ? 0 : ((Int64Value) object).getValue()).build();
                    }
                    case UINT32_VALUE -> {
                        return Value.newBuilder().setIntegerValue(isNull ? 0 : ((UInt32Value) object).getValue()).build();
                    }
                    case UINT64_VALUE -> {
                        return Value.newBuilder().setIntegerValue(isNull ? 0 : ((UInt64Value) object).getValue()).build();
                    }
                    case FLOAT_VALUE -> {
                        return Value.newBuilder().setDoubleValue(isNull ? 0 : ((FloatValue) object).getValue()).build();
                    }
                    case DOUBLE_VALUE -> {
                        return Value.newBuilder().setDoubleValue(isNull ? 0 : ((DoubleValue) object).getValue()).build();
                    }
                    case DATE -> {
                        if (isNull) {
                            return Value.newBuilder().setStringValue("0001-01-01").build();
                        }
                        final Date date = (Date) object;
                        return Value.newBuilder().setStringValue(String
                                        .format("%04d-%02d-%02d", date.getYear(), date.getMonth(), date.getDay()))
                                .build();
                    }
                    case TIME -> {
                        if (isNull) {
                            return Value.newBuilder().setStringValue("00:00:00")
                                    .build();
                        }
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return Value.newBuilder().setStringValue(String
                                        .format("%02d:%02d:%02d", timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds()))
                                .build();
                    }
                    case DATETIME -> {
                        if (isNull) {
                            OffsetDateTime ldt = LocalDateTime.of(
                                            1, 1, 1,
                                            0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC);
                            return Value.newBuilder()
                                    .setTimestampValue(Timestamp.newBuilder()
                                            .setSeconds(ldt.toInstant().getEpochSecond())
                                            .setNanos(ldt.toInstant().getNano()).build())
                                    .build();
                        }
                        final DateTime dt = (DateTime) object;
                        Instant instant = LocalDateTime.of(
                                        dt.getYear(), dt.getMonth(), dt.getDay(),
                                        dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int) dt.getUtcOffset().getSeconds()))
                                .toInstant();
                        return Value.newBuilder()
                                .setTimestampValue(Timestamp.newBuilder()
                                        .setSeconds(instant.getEpochSecond())
                                        .setNanos(instant.getNano()).build())
                                .build();
                    }
                    case TIMESTAMP -> {
                        if (isNull) {
                            OffsetDateTime ldt = LocalDateTime.of(
                                            1, 1, 1,
                                            0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC);
                            return Value.newBuilder()
                                    .setTimestampValue(Timestamp.newBuilder()
                                            .setSeconds(ldt.toInstant().getEpochSecond())
                                            .setNanos(ldt.toInstant().getNano()).build())
                                    .build();
                        }
                        return Value.newBuilder().setTimestampValue((Timestamp) object).build();
                    }
                    case ANY -> {
                        if (isNull) {
                            return Value.newBuilder().setStringValue("").build();
                        }
                        final Any any = (Any) object;
                        try {
                            return Value.newBuilder().setStringValue(printer.print(any)).build();
                        } catch (InvalidProtocolBufferException e) {
                            return Value.newBuilder().setStringValue(any.getValue().toStringUtf8()).build();
                        }
                    }
                    case EMPTY, NULL_VALUE -> {
                        return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                    }
                    default -> {
                        final Document child = convert(null, field.getMessageType(), (DynamicMessage) value, printer);
                        final MapValue mapValue = MapValue.newBuilder().putAllFields(child.getFieldsMap()).build();
                        return Value.newBuilder()
                                .setMapValue(mapValue)
                                .build();
                    }
                }
            }
            default -> {
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            }
        }
    }

}
