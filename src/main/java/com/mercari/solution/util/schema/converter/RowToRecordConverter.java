package com.mercari.solution.util.schema.converter;

import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RowToRecordConverter {

    public static GenericRecord convert(final AvroWriteRequest<Row> request) {
        return convert(request.getSchema(), request.getElement());
    }

    public static GenericRecord convert(final Schema schema, final Row row) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Object value = convertRecordValue(field.name(), field.schema(), row.getValue(field.name()));
            builder.set(field, value);
        }
        return builder.build();
    }

    public static Schema convertSchema(final org.apache.beam.sdk.schemas.Schema schema) {
        return convertSchema(schema, "root");
    }

    public static Schema convertSchema(final org.apache.beam.sdk.schemas.Schema schema, String name) {
        final String schemaName;
        final org.apache.beam.sdk.schemas.Schema.Options options = schema.getOptions();
        if(options.hasOption("name")) {
            schemaName = options.getValue("name", String.class);
        } else {
            schemaName = name;
        }
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(schemaName).fields();
        for(final org.apache.beam.sdk.schemas.Schema.Field field : schema.getFields()) {
            SchemaBuilder.FieldBuilder<Schema> fieldBuilder = schemaFields
                    .name(field.getName())
                    .doc(field.getDescription())
                    .orderIgnore();

            // set altName
            if(field.getOptions().hasOption(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)) {
                fieldBuilder = fieldBuilder.prop(SourceConfig.OPTION_ORIGINAL_FIELD_NAME, field.getOptions().getValue(SourceConfig.OPTION_ORIGINAL_FIELD_NAME));
            }

            final Schema fieldSchema = convertFieldSchema(field.getType(), field.getOptions(), field.getName(), null);

            // set default
            final SchemaBuilder.GenericDefault<Schema> fieldAssembler = fieldBuilder.type(fieldSchema);
            if(field.getOptions().hasOption(RowSchemaUtil.OPTION_NAME_DEFAULT_VALUE)) {
                final String defaultValue = field.getOptions().getValue(RowSchemaUtil.OPTION_NAME_DEFAULT_VALUE, String.class);
                schemaFields = fieldAssembler.withDefault(AvroSchemaUtil.convertDefaultValue(fieldSchema, defaultValue));
            } else {
                schemaFields = fieldAssembler.noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    private static Schema convertFieldSchema(
            final org.apache.beam.sdk.schemas.Schema.FieldType fieldType,
            final org.apache.beam.sdk.schemas.Schema.Options fieldOptions,
            final String fieldName,
            final String parentNamespace) {

        final Schema fieldSchema;
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                fieldSchema = Schema.create(Schema.Type.BOOLEAN);
                break;
            case STRING:
                fieldSchema = Schema.create(Schema.Type.STRING);
                break;
            case BYTES:
                fieldSchema = Schema.create(Schema.Type.BYTES);
                break;
            case DECIMAL:
                fieldSchema = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
                break;
            case BYTE:
            case INT16:
            case INT32:
                fieldSchema = Schema.create(Schema.Type.INT);
                break;
            case INT64:
                fieldSchema = Schema.create(Schema.Type.LONG);
                break;
            case FLOAT:
                fieldSchema = Schema.create(Schema.Type.FLOAT);
                break;
            case DOUBLE:
                fieldSchema = Schema.create(Schema.Type.DOUBLE);
                break;
            case DATETIME:
                fieldSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                break;
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    fieldSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    fieldSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    fieldSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    fieldSchema = (new LogicalType("local-timestamp-micros")).addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String doc = fieldType.getLogicalType(EnumerationType.class).getValues().stream().collect(Collectors.joining(","));
                    fieldSchema = Schema.createEnum(fieldName, doc, parentNamespace, fieldType.getLogicalType(EnumerationType.class).getValues());
                    break;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                fieldSchema = convertSchema(fieldType.getRowSchema(), fieldName);
                break;
            case ITERABLE:
            case ARRAY:
                fieldSchema = Schema.createArray(convertFieldSchema(fieldType.getCollectionElementType(), fieldOptions, fieldName, parentNamespace));
                break;
            case MAP: {
                final Schema mapValueSchema = convertFieldSchema(fieldType.getMapValueType(), fieldOptions, fieldName, parentNamespace);
                fieldSchema = Schema.createMap(mapValueSchema);
                break;
            }
            default:
                throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        }

        if(fieldOptions != null
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY.equals(fieldType.getTypeName())
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ITERABLE.equals(fieldType.getTypeName())) {
            for(final String optionName : fieldOptions.getOptionNames()) {
                final Object optionValue = fieldOptions.getValue(optionName);
                fieldSchema.addProp(optionName, optionValue);
            }
        }

        if(fieldType.getNullable()) {
            return Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
        } else {
            return fieldSchema;
        }
    }

    private static Object convertRecordValue(final String name, final Schema schema, final Object value) {
        if(value == null) {
            return null;
        }
        switch (schema.getType()) {
            case STRING:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case ENUM: {
                final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                final int index;
                if(enumValue.getValue() >= schema.getEnumSymbols().size()) {
                    index = 0;
                } else {
                    index = enumValue.getValue();
                }
                return AvroSchemaUtil.createEnumSymbol(name, schema.getEnumSymbols(), schema.getEnumSymbols().get(index));
            }
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    if(value instanceof BigDecimal) {
                        return AvroSchemaUtil.toByteBuffer((BigDecimal) value);
                    }
                } else {
                    return ByteBuffer.wrap((byte[]) value);
                }
            }
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalDate localDate = DateTimeUtil.toLocalDate(value.toString());
                        return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                    } else if(value instanceof LocalDate) {
                        return ((Long)((LocalDate) value).toEpochDay()).intValue();
                    } else if(value instanceof Long) {
                        return ((Long)value).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else if(value instanceof Short) {
                        return ((Short)value).intValue();
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value.toString());
                        return DateTimeUtil.toMilliOfDay(localTime);
                    } else if(value instanceof LocalTime) {
                        return ((Long)(((LocalTime) value).toNanoOfDay() / 1000_000L)).intValue();
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for time");
                    }
                } else {
                    if(value instanceof String) {
                        return Integer.valueOf((String)value);
                    } else if(value instanceof Long) {
                        return ((Long)value).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else if(value instanceof Short) {
                        return ((Short)value).intValue();
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for int");
                    }
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return java.time.Instant.parse(value.toString()).toEpochMilli();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return java.time.Instant.parse(value.toString()).toEpochMilli() * 1000;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value.toString());
                        return localTime != null ? (Long)(localTime.toNanoOfDay() / 1000L) : null;
                    } else if(value instanceof LocalTime) {
                        return ((LocalTime) value).toNanoOfDay() / 1000L;
                    } else if(value instanceof Long) {
                        return ((Long)value).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else if(value instanceof Short) {
                        return ((Short)value).intValue();
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                } else {
                    if(value instanceof String) {
                        return Long.valueOf((String)value);
                    } else if(value instanceof Long) {
                        return value;
                    } else if(value instanceof Integer) {
                        return ((Integer)value).longValue();
                    } else if(value instanceof Short) {
                        return ((Short)value).longValue();
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                }
            case RECORD:
                return convert(schema, (Row)value);
            case ARRAY:
                return ((List<Object>)value).stream()
                        .map(v -> convertRecordValue(name, schema.getElementType(), v))
                        .collect(Collectors.toList());
            case MAP: {
                final Map<String, Object> output = new HashMap<>();
                final Map<?,?> map = (Map) value;
                for(Map.Entry<?,?> entry : map.entrySet()) {
                    output.put(entry.getKey().toString(), convertRecordValue(name, schema.getValueType(), entry.getValue()));
                }
                return output;
            }
            case UNION:
                final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                return convertRecordValue(name, childSchema, value);
            case NULL:
            default:
                return null;
        }
    }

}
