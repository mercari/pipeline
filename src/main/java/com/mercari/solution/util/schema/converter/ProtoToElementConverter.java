package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToElementConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToElementConverter.class);

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        return Schema.of(convertFields(messageType));
    }

    public static List<Schema.Field> convertFields(final Descriptors.Descriptor messageType) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            fields.add(Schema.Field.of(field.getName(), convertSchema(field)));
        }
        return fields;
    }

    public static List<Schema.Field> convertFields(final List<Descriptors.FieldDescriptor> fieldDescriptors) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final Descriptors.FieldDescriptor field : fieldDescriptors) {
            fields.add(Schema.Field.of(field.getName(), convertSchema(field)));
        }
        return fields;
    }

    public static Map<String, Object> convert(
            final List<Schema.Field> fields,
            final Descriptors.Descriptor messageDescriptor,
            final byte[] bytes,
            final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(fields, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> convert(
            final List<Schema.Field> fields,
            final Descriptors.Descriptor messageDescriptor,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        if(message == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>();
        for(final Schema.Field field : fields) {
            final Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(field.getName());
            if(fieldDescriptor == null) {
                continue;
            }
            final Object value = convertValue(fieldDescriptor, message.getField(fieldDescriptor), printer);
            values.put(field.getName(), value);
        }
        return values;
    }

    public static Map<String, Object> convert(
            final Descriptors.Descriptor messageDescriptor,
            final byte[] bytes) {

        return convert(messageDescriptor, bytes, null);
    }

    public static Map<String, Object> convert(
            final Descriptors.Descriptor messageDescriptor,
            final byte[] bytes,
            final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> convert(final DynamicMessage message) {
        final Map<String, Object> values = new HashMap<>();
        for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            final Object value = convertValue(entry.getKey(), entry.getValue(), null);
            values.put(entry.getKey().getName(), value);
        }
        return values;
    }

    public static Map<String, Object> convert(
            final Descriptors.Descriptor messageDescriptor,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        final Map<String, Object> values = new HashMap<>();
        for(final Descriptors.FieldDescriptor fieldDescriptor : messageDescriptor.getFields()) {
            final Object value = convertValue(fieldDescriptor, message.getField(fieldDescriptor), printer);
            values.put(fieldDescriptor.getName(), value);
        }
        return values;
    }

    private static Schema.FieldType convertSchema(Descriptors.FieldDescriptor field) {
        //final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema.FieldType fieldType = switch (field.getJavaType()) {
            case BOOLEAN -> Schema.FieldType.BOOLEAN;
            case ENUM -> {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                yield Schema.FieldType.enumeration(enumNames);
            }
            case STRING -> Schema.FieldType.STRING;
            case BYTE_STRING -> Schema.FieldType.BYTES;
            case INT -> Schema.FieldType.INT32;
            case LONG -> Schema.FieldType.INT64;
            case FLOAT -> Schema.FieldType.FLOAT32;
            case DOUBLE -> Schema.FieldType.FLOAT64;
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
                    if(keyField == null || valueField == null) {
                        throw new IllegalArgumentException("Illegal protobuf map type. keyField: " + keyField + ", valueField: " + valueField);
                    }
                    yield Schema.FieldType.map(convertSchema(valueField));
                }
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> Schema.FieldType.BOOLEAN;
                    case STRING_VALUE -> Schema.FieldType.STRING;
                    case BYTES_VALUE -> Schema.FieldType.BYTES;
                    case INT32_VALUE, UINT32_VALUE -> Schema.FieldType.INT32;
                    case INT64_VALUE, UINT64_VALUE -> Schema.FieldType.INT64;
                    case FLOAT_VALUE -> Schema.FieldType.FLOAT32;
                    case DOUBLE_VALUE -> Schema.FieldType.FLOAT64;
                    case DATE -> Schema.FieldType.DATE;
                    case TIME -> Schema.FieldType.TIME;
                    case DATETIME, TIMESTAMP -> Schema.FieldType.TIMESTAMP;
                    case ANY -> Schema.FieldType.STRING;
                    case NULL_VALUE, EMPTY -> Schema.FieldType.STRING;
                    default -> Schema.FieldType.element(convertSchema(field.getMessageType()));
                };
            }
        };

        if(field.isRepeated() && !field.isMapField()) {
            return Schema.FieldType.array(fieldType);
        } else {
            return fieldType.withNullable(true);
        }

    }

    private static Object convertValue(
            final Descriptors.FieldDescriptor field,
            final Object value,
            final JsonFormat.Printer printer) {

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
        } else if(field.isRepeated()) {
            if(value == null) {
                return new ArrayList<>();
            }
            return ((List<Object>) value).stream()
                    .map(v -> convertPrimitiveValue(field, v, printer))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else {
            return convertPrimitiveValue(field, value, printer);
        }

        /*
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
                    .map(v -> convertPrimitiveValue(field, v, printer))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return convertPrimitiveValue(field, value, printer);

         */
    }

    private static Object convertPrimitiveValue(
            final Descriptors.FieldDescriptor field,
            final Object value,
            final JsonFormat.Printer printer) {

        if(value == null) {
            return null;
        }

        return switch (field.getJavaType()) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING -> value;
            case ENUM -> ((Descriptors.EnumValueDescriptor)value).getIndex();
            case BYTE_STRING -> ByteBuffer.wrap(((ByteString) value).toByteArray());
            case MESSAGE -> {
                final Object object = ProtoSchemaUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                if(object == null) {
                    yield null;
                }
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> ((BoolValue) object).getValue();
                    case STRING_VALUE -> ((StringValue) object).getValue();
                    case BYTES_VALUE -> ByteBuffer.wrap(((BytesValue) object).getValue().toByteArray());
                    case INT32_VALUE -> ((Int32Value) object).getValue();
                    case INT64_VALUE -> ((Int64Value) object).getValue();
                    case UINT32_VALUE -> ((UInt32Value) object).getValue();
                    case UINT64_VALUE -> ((UInt64Value) object).getValue();
                    case FLOAT_VALUE -> ((FloatValue) object).getValue();
                    case DOUBLE_VALUE -> ((DoubleValue) object).getValue();
                    case DATE -> DateTimeUtil.toEpochDay((Date) object);
                    case TIME -> DateTimeUtil.toMicroOfDay((TimeOfDay) object);
                    case DATETIME -> {
                        final DateTime dt = (DateTime) object;
                        long epochMilli = LocalDateTime.of(
                                        dt.getYear(), dt.getMonth(), dt.getDay(),
                                        dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli();
                        yield org.joda.time.Instant.ofEpochMilli(epochMilli);
                    }
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond((Timestamp) object);
                    case ANY -> {
                        final Any any = (Any) object;
                        try {
                            yield printer == null ? null : printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            yield any.getValue().toStringUtf8();
                        }
                    }
                    case CUSTOM -> {
                        final Schema schema = convertSchema(field.getMessageType());
                        yield convert(schema.getFields(), field.getMessageType(), (DynamicMessage) value, printer);
                    }
                    case EMPTY, NULL_VALUE -> null;
                };
            }
        };
    }

}
