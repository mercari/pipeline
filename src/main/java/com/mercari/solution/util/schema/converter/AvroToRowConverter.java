package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class AvroToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToRowConverter.class);

    public static Row convert(final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        Row.FieldValueBuilder fieldBuilder = builder.withFieldValues(new HashMap<>());
        for(final org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
            fieldBuilder = fieldBuilder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), value));
        }
        return fieldBuilder.build();
    }

    public static Row convert(final org.apache.avro.Schema avroSchema, final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        Row.FieldValueBuilder fieldBuilder = null;
        for(final org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
            if(fieldBuilder == null) {
                fieldBuilder = builder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), value));
            } else {
                fieldBuilder = fieldBuilder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), value));
            }
        }
        return fieldBuilder.build();
    }

    public static Schema convertSchema(final String avroSchema) {
        return convertSchema(AvroSchemaUtil.convertSchema(avroSchema));
    }

    public static Schema convertSchema(final org.apache.avro.Schema avroSchema) {
        final Schema.Builder builder = convertSchemaBuilder(avroSchema);
        final Schema.Options.Builder optionBuilder = convertOptionBuilder(avroSchema);
        builder.setOptions(optionBuilder);
        return builder.build();
    }

    public static Schema.Builder convertSchemaBuilder(final org.apache.avro.Schema avroSchema) {
        final Schema.Builder schemaBuilder = Schema.builder();
        avroSchema.getFields().stream()
                .map(avroField -> Schema.Field
                        .of(avroField.name(), convertFieldType(avroField.schema()))
                        .withOptions(convertFieldOption(avroField))
                        .withDescription(avroField.doc() == null ? "" : avroField.doc()))
                .forEach(schemaBuilder::addField);
        return schemaBuilder;
    }

    public static Schema.Options.Builder convertOptionBuilder(final org.apache.avro.Schema avroSchema) {
        final Schema.Options.Builder optionBuilder = Schema.Options.builder();
        avroSchema.getObjectProps().keySet()
                .forEach(propName -> optionBuilder.setOption(propName, Schema.FieldType.STRING, avroSchema.getProp(propName)));
        return optionBuilder;
    }

    private static Object convertValue(final org.apache.avro.Schema schema, final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        switch (schema.getType()) {
            case BOOLEAN, FLOAT, DOUBLE -> {
                return value;
            }
            case STRING -> {
                return value.toString();
            }
            case ENUM -> {
                return RowSchemaUtil.toEnumerationTypeValue(fieldType, value.toString());
            }
            case FIXED, BYTES -> {
                final byte[] bytes = ((ByteBuffer) value).array();
                if (AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = schema.getObjectProp("scale") != null ?
                            Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                }
                return Arrays.copyOf(bytes, bytes.length);
            }
            case INT -> {
                final Integer intValue = (Integer) value;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return LocalDate.ofEpochDay(intValue.longValue());
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(intValue * 1000_1000L);
                }
                return intValue;
            }
            case LONG -> {
                final long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue / 1000);
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(longValue * 1000L);
                }
                return longValue;
            }
            case RECORD -> {
                return convert(schema, fieldType.getRowSchema(), (GenericRecord) value);
            }
            case ARRAY -> {
                return ((List<Object>) value).stream()
                        .map(o -> convertValue(schema.getElementType(), fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            }
            case UNION -> {
                return convertValue(AvroSchemaUtil.unnestUnion(schema), fieldType, value);
            }
            default -> {
                return null;
            }
        }
    }

    private static Schema.FieldType convertFieldType(final org.apache.avro.Schema avroSchema) {
        return switch (avroSchema.getType()) {
            case BOOLEAN -> Schema.FieldType.BOOLEAN;
            case FLOAT -> Schema.FieldType.FLOAT;
            case DOUBLE -> Schema.FieldType.DOUBLE;
            case ENUM -> Schema.FieldType.logicalType(EnumerationType.create(avroSchema.getEnumSymbols()));
            case STRING -> {
                final String sqlType = avroSchema.getProp("sqlType");
                if (sqlType == null) {
                    yield Schema.FieldType.STRING;
                }
                if ("DATETIME".equalsIgnoreCase(sqlType)) {
                    yield Schema.FieldType.DATETIME;
                } else if ("JSON".equalsIgnoreCase(sqlType)) {
                    yield Schema.FieldType.STRING;
                } else if ("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
                    yield Schema.FieldType.STRING;
                }
                yield Schema.FieldType.STRING;
            }
            case FIXED, BYTES -> {
                if (AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    yield Schema.FieldType.DECIMAL;
                }
                yield Schema.FieldType.BYTES;
            }
            case INT -> {
                if (LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    yield CalciteUtils.DATE;
                } else if (LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    yield CalciteUtils.TIME;
                }
                yield Schema.FieldType.INT32;
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                    yield Schema.FieldType.DATETIME;
                } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                    yield Schema.FieldType.DATETIME;
                } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                    yield CalciteUtils.TIME;
                }
                yield Schema.FieldType.INT64;
            }
            case RECORD -> Schema.FieldType.row(convertSchema(avroSchema));
            case ARRAY -> Schema.FieldType.array(convertFieldType(avroSchema.getElementType()));
            case MAP -> Schema.FieldType.map(Schema.FieldType.STRING, convertFieldType(avroSchema.getValueType()));
            case UNION -> {
                final boolean nullable = avroSchema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
                final org.apache.avro.Schema unnested = avroSchema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                yield convertFieldType(unnested).withNullable(nullable);
            }
            default -> Schema.FieldType.STRING;
        };
    }

    private static Schema.Options convertFieldOption(final org.apache.avro.Schema.Field field) {
        Schema.Options.Builder builder = Schema.Options.builder();
        if(field.defaultVal() != null && !JsonProperties.NULL_VALUE.equals(field.defaultVal())) {
            try {
                builder.setOption("defaultVal", convertFieldType(field.schema()), field.defaultVal());
            } catch (ClassCastException e) {
                LOG.warn("Failed to convert defaultVal cause: {}", e.getMessage());
            }
        }
        if(field.order() != null) {
            builder.setOption("order", Schema.FieldType.STRING, field.order().name());
        }
        builder.setOption("pos", Schema.FieldType.INT32, field.pos());

        final org.apache.avro.Schema unnestFieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
        if(org.apache.avro.Schema.Type.ARRAY.equals(unnestFieldSchema.getType())) {
            AvroSchemaUtil.unnestUnion(unnestFieldSchema.getElementType()).getObjectProps()
                    .forEach((key, value) -> {
                        switch (key) {
                            case "sqlType" -> builder.setOption(key, Schema.FieldType.STRING, value);
                            case "scale", "precision" -> builder.setOption(key, Schema.FieldType.INT32, value);
                            default -> builder.setOption(key, Schema.FieldType.STRING, value.toString());
                        }
                    });
        } else {
            unnestFieldSchema.getObjectProps()
                    .forEach((key, value) -> {
                        switch (key) {
                            case "sqlType" -> builder.setOption(key, Schema.FieldType.STRING, value);
                            case "scale", "precision" -> builder.setOption(key, Schema.FieldType.INT32, value);
                            default -> builder.setOption(key, Schema.FieldType.STRING, value.toString());
                        }
                    });
        }
        return builder.build();
    }

}
