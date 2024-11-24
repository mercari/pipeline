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
import java.util.Optional;
import java.util.stream.Collectors;

public class RowToAvroConverter {

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

        final Schema fieldSchema = switch (fieldType.getTypeName()) {
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case STRING -> Schema.create(Schema.Type.STRING);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case DECIMAL -> LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
            case BYTE, INT16, INT32 -> Schema.create(Schema.Type.INT);
            case INT64 -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case DATETIME -> LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case LOGICAL_TYPE -> {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    yield LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    yield (new LogicalType("local-timestamp-micros")).addToSchema(Schema.create(Schema.Type.LONG));
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String doc = fieldType.getLogicalType(EnumerationType.class).getValues().stream().collect(Collectors.joining(","));
                    yield Schema.createEnum(fieldName, doc, parentNamespace, fieldType.getLogicalType(EnumerationType.class).getValues());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW -> convertSchema(fieldType.getRowSchema(), fieldName);
            case ITERABLE, ARRAY -> Schema.createArray(convertFieldSchema(fieldType.getCollectionElementType(), fieldOptions, fieldName, parentNamespace));
            case MAP -> {
                final Schema mapValueSchema = convertFieldSchema(fieldType.getMapValueType(), fieldOptions, fieldName, parentNamespace);
                yield Schema.createMap(mapValueSchema);
            }
            default -> throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        };

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
        return switch (schema.getType()) {
            case STRING, BOOLEAN, FLOAT, DOUBLE -> value;
            case ENUM -> {
                final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                final int index;
                if(enumValue.getValue() >= schema.getEnumSymbols().size()) {
                    index = 0;
                } else {
                    index = enumValue.getValue();
                }
                yield AvroSchemaUtil.createEnumSymbol(name, schema.getEnumSymbols(), schema.getEnumSymbols().get(index));
            }
            case FIXED, BYTES -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    if(value instanceof BigDecimal) {
                        yield AvroSchemaUtil.toByteBuffer((BigDecimal) value);
                    } else {
                        yield null;
                    }
                } else {
                    yield ByteBuffer.wrap((byte[]) value);
                }
            }
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> Optional.ofNullable(DateTimeUtil.toLocalDate(s.toString()))
                                .map(LocalDate::toEpochDay)
                                .map(Long::intValue)
                                .orElse(null);
                        case Long l -> l.intValue();
                        case Integer i -> i.intValue();
                        case Short s -> s.intValue();
                        case LocalDate localDate -> ((Long) localDate.toEpochDay()).intValue();
                        default -> throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    };
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> Optional.ofNullable(DateTimeUtil.toLocalTime(s.toString()))
                                .map(DateTimeUtil::toMilliOfDay)
                                .orElse(null);
                        case Long l -> l.intValue();
                        case Integer i -> i.intValue();
                        case Short s -> s.intValue();
                        case LocalTime localTime -> Long.valueOf(localTime.toNanoOfDay() / 1000_000L).intValue();
                        default ->
                                throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for time");
                    };
                } else {
                    yield switch (value) {
                        case String s -> Integer.valueOf(s);
                        case Long l -> l.intValue();
                        case Integer i -> i.intValue();
                        case Short s -> s.intValue();
                        default ->
                                throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for int");
                    };
                }
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    yield java.time.Instant.parse(value.toString()).toEpochMilli();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    yield java.time.Instant.parse(value.toString()).toEpochMilli() * 1000;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> Optional.ofNullable(DateTimeUtil.toLocalTime(s))
                                .map(LocalTime::toNanoOfDay)
                                .map(l -> l / 1000L)
                                .orElse(null);
                        case LocalTime localTime -> localTime.toNanoOfDay() / 1000L;
                        case Long l -> l.longValue();
                        case Integer i -> i.longValue();
                        case Short s -> s.longValue();
                        default ->
                                throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    };
                } else {
                    yield switch (value) {
                        case String s -> Long.valueOf(s);
                        case Long l -> l.longValue();
                        case Integer i -> i.longValue();
                        case Short s -> s.longValue();
                        default ->
                                throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for long");
                    };
                }
            }

            case RECORD -> convert(schema, (Row)value);
            case ARRAY -> ((List<Object>)value).stream()
                        .map(v -> convertRecordValue(name, schema.getElementType(), v))
                        .collect(Collectors.toList());
            case MAP -> {
                final Map<String, Object> output = new HashMap<>();
                final Map<?,?> map = (Map) value;
                for(Map.Entry<?,?> entry : map.entrySet()) {
                    output.put(entry.getKey().toString(), convertRecordValue(name, schema.getValueType(), entry.getValue()));
                }
                yield output;
            }
            case UNION -> {
                final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                yield convertRecordValue(name, childSchema, value);
            }
            case NULL -> null;
        };
    }

}
