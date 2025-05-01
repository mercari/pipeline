package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProtoToAvroConverter {

    public static Schema convertSchema(
            final Descriptors.Descriptor messageType) {

        final Set<String> messageNames = new HashSet<>();
        return convertSchema(messageType, messageNames);
    }

    private static Schema convertSchema(
            final Descriptors.Descriptor messageType,
            final Set<String> messageNames) {

        final SchemaBuilder.FieldAssembler<Schema> schemaFields = convertSchemaBuilder(messageType, messageNames);
        return schemaFields.endRecord();
    }

    public static SchemaBuilder.FieldAssembler<Schema> convertSchemaBuilder(
            final Descriptors.Descriptor messageType) {

        final Set<String> messageNames = new HashSet<>();
        return convertSchemaBuilder(messageType, messageNames);
    }

    private static SchemaBuilder.FieldAssembler<Schema> convertSchemaBuilder(
            final Descriptors.Descriptor messageType,
            final Set<String> messageNames) {

        /*
        if(messageNames.contains(messageType.getFullName())) {
            return SchemaBuilder
                    .record(messageType.getFullName())
                    .fields();
        }
         */
        messageNames.add(messageType.getFullName());

        final String namespace = messageType
                .getFullName()
                .substring(0, messageType.getFullName().length() - messageType.getName().length() - 1);

        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder
                .record(messageType.getName())
                .namespace(namespace)
                .fields();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            schemaFields.name(field.getName()).type(convertSchema(field, messageNames)).noDefault();
        }
        return schemaFields;
    }

    public static GenericRecord convert(final Schema schema,
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

    public static GenericRecord convert(final Schema schema,
                                        final Descriptors.Descriptor messageDescriptor,
                                        final DynamicMessage message,
                                        final JsonFormat.Printer printer) {

        final GenericRecordBuilder builder = convertBuilder(schema, messageDescriptor, message, printer);
        return builder.build();
    }

    public static GenericRecordBuilder convertBuilder(
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

    public static GenericRecordBuilder convertBuilder(
            final Schema schema,
            final Descriptors.Descriptor messageDescriptor,
            final DynamicMessage message,
            final JsonFormat.Printer printer) {

        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(field.name());
            if(fieldDescriptor == null) {
                continue;
            }
            boolean isNull = fieldDescriptor == null;
            builder.set(field.name(), convertValue(field.schema(), fieldDescriptor, message.getField(fieldDescriptor), printer, isNull));
        }
        return builder;
    }

    private static Schema convertSchema(
            final Descriptors.FieldDescriptor field,
            final Set<String> messageNames) {

        //final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema elementSchema = switch (field.getJavaType()) {
            case BOOLEAN -> AvroSchemaUtil.REQUIRED_BOOLEAN;
            case ENUM -> {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                yield Schema.createEnum(field.getName(), "", "", enumNames);
            }
            case STRING ->
                // TODO Fix when handle NULL schema for all avro converter
                field.isRepeated() ? AvroSchemaUtil.REQUIRED_STRING : AvroSchemaUtil.NULLABLE_STRING;
            case BYTE_STRING -> AvroSchemaUtil.REQUIRED_BYTES;
            case INT -> AvroSchemaUtil.REQUIRED_INT;
            case LONG -> AvroSchemaUtil.REQUIRED_LONG;
            case FLOAT -> AvroSchemaUtil.REQUIRED_FLOAT;
            case DOUBLE -> AvroSchemaUtil.REQUIRED_DOUBLE;
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
                    yield AvroSchemaUtil.createMapRecordSchema(
                            field.getName(),
                            convertSchema(keyField, messageNames),
                            convertSchema(valueField, messageNames));
                }
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> AvroSchemaUtil.REQUIRED_BOOLEAN;
                    case STRING_VALUE -> AvroSchemaUtil.REQUIRED_STRING;
                    case BYTES_VALUE -> AvroSchemaUtil.REQUIRED_BYTES;
                    case INT32_VALUE -> AvroSchemaUtil.REQUIRED_INT;
                    case INT64_VALUE -> AvroSchemaUtil.REQUIRED_LONG;
                    case FLOAT_VALUE -> AvroSchemaUtil.REQUIRED_FLOAT;
                    case DOUBLE_VALUE -> AvroSchemaUtil.REQUIRED_DOUBLE;
                    case UINT32_VALUE -> AvroSchemaUtil.REQUIRED_INT;
                    case UINT64_VALUE -> AvroSchemaUtil.REQUIRED_LONG;
                    case DATE -> AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
                    case TIME -> AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
                    case DATETIME, TIMESTAMP -> AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
                    case ANY -> AvroSchemaUtil.REQUIRED_STRING;
                    case NULL_VALUE, EMPTY -> AvroSchemaUtil.NULLABLE_STRING;
                    default -> {
                        final Schema customSchema = convertSchema(field.getMessageType(), messageNames);
                        yield Schema.createUnion(customSchema, Schema.create(Schema.Type.NULL));
                    }
                };
            }
            default -> throw new IllegalArgumentException(field.getName() + " is not supported for bigquery.");
        };

        if(field.isRepeated()) {
            return Schema.createArray(elementSchema);
        } else {
            return elementSchema;
        }

    }

    private static Object convertValue(final Schema schema,
                                       final Descriptors.FieldDescriptor field,
                                       final Object value,
                                       final JsonFormat.Printer printer,
                                       final boolean isNull) {

        if(field.isRepeated()) {
            if(value == null) {
                return new ArrayList<>();
            }
            if(field.isMapField()) {
                return ((List<DynamicMessage>) value).stream()
                        .map(e -> {
                            final GenericRecordBuilder builder = new GenericRecordBuilder(schema.getElementType());
                            builder.set("key", ProtoSchemaUtil.getValue(e, "key", printer));
                            builder.set("value", ProtoSchemaUtil.getValue(e, "value", printer));
                            return builder.build();
                        })
                        .collect(Collectors.toList());
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(schema.getElementType(), field, v, printer, isNull))
                    .collect(Collectors.toList());
        }
        return getValue(schema, field, value, printer, isNull);
    }

    private static Object getValue(final Schema schema,
                                   final Descriptors.FieldDescriptor field,
                                   final Object value,
                                   final JsonFormat.Printer printer,
                                   boolean isNull) {

        return switch (field.getJavaType()) {
            case BOOLEAN -> !isNull && (Boolean)value;
            case LONG -> isNull ? 0L : value;
            case INT -> isNull ? 0 : value;
            case FLOAT -> isNull ? 0f : value;
            case DOUBLE -> isNull ? 0d : value;
            case STRING -> isNull ? "" : value;
            case ENUM -> {
                final Schema enumSchema = AvroSchemaUtil.unnestUnion(schema);
                if(isNull) {
                    yield new GenericData.EnumSymbol(
                            enumSchema,
                            enumSchema.getEnumSymbols().get(0));
                } else {
                    yield new GenericData.EnumSymbol(
                            AvroSchemaUtil.unnestUnion(schema),
                            ((Descriptors.EnumValueDescriptor) value).getName());
                }
            }
            case BYTE_STRING -> ByteBuffer.wrap(isNull ? ByteArray.copyFrom("").toByteArray() : ((ByteString) value).toByteArray());
            case MESSAGE -> {
                final Object object = ProtoSchemaUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                isNull = object == null;
                yield switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE -> !isNull && ((BoolValue) object).getValue();
                    case BYTES_VALUE -> ByteBuffer.wrap(isNull ? ByteArray.copyFrom ("").toByteArray() : ((BytesValue) object).getValue().toByteArray());
                    case STRING_VALUE -> isNull ? "" : ((StringValue) object).getValue();
                    case INT32_VALUE -> isNull ? 0 : ((Int32Value) object).getValue();
                    case INT64_VALUE -> isNull ? 0L : ((Int64Value) object).getValue();
                    case UINT32_VALUE -> isNull ? 0 : ((UInt32Value) object).getValue();
                    case UINT64_VALUE -> isNull ? 0L : ((UInt64Value) object).getValue();
                    case FLOAT_VALUE -> isNull ? 0f : ((FloatValue) object).getValue();
                    case DOUBLE_VALUE -> isNull ? 0d : ((DoubleValue) object).getValue();
                    case DATE -> {
                        if(isNull) {
                            yield (int)(LocalDate.of(1, 1, 1).toEpochDay());
                        }
                        final Date date = (Date) object;
                        yield (int)(LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).toEpochDay());
                    }
                    case TIME -> {
                        if(isNull) {
                            yield LocalTime.of(0, 0, 0, 0).toSecondOfDay() * 1000_000L;
                        }
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        yield LocalTime.of(
                                timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos())
                                .toSecondOfDay() * 1000_000L;
                    }
                    case DATETIME -> {
                        if(isNull) {
                            yield LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli() * 1000L;
                        }
                        final DateTime dt = (DateTime) object;
                        yield LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli() * 1000L;
                    }
                    case TIMESTAMP -> {
                        if (isNull) {
                            yield LocalDateTime.of(
                                            1, 1, 1,
                                            0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli() * 1000;
                        }
                        yield Timestamps.toMicros((Timestamp) object);
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
                    default -> convert(AvroSchemaUtil.unnestUnion(schema), field.getMessageType(), (DynamicMessage) value, printer);
                };
            }
            default -> throw new IllegalStateException("Not support data type: " + field);
        };
    }

}
