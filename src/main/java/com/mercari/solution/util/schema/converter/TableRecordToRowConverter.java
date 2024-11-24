package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class TableRecordToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TableRecordToRowConverter.class);

    public static Row converts(final SchemaAndRecord schemaAndRecord) {
        final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(schemaAndRecord.getTableSchema());
        return convert(avroSchema, schemaAndRecord.getRecord());
    }

    public static Row convert( final org.apache.avro.Schema avroSchema, final GenericRecord record) {
        final Schema schema = convertSchema(avroSchema);
        return convert(schema, record);
    }

    public static Row convert(final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        final List<Object> values = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            try {
                values.add(convertValue(field.getType(), record.get(field.getName())));
            } catch (Throwable e) {
                LOG.error("Error : " + field.getName() + " : " + field.getType());
                throw e;
            }
        }
        return builder.addValues(values).build();
    }

    public static Schema convertSchema(final org.apache.avro.Schema avroSchema) {
        final Schema.Builder builder = Schema.builder();
        for(final org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
            final Schema.Field field = Schema.Field.of(avroField.name(), convertFieldType(avroField.schema()));
            builder.addField(field);
        }
        return builder.build();
    }

    public static Schema convertSchema(final TableSchema tableSchema) {
        return convertSchema(tableSchema, null);
    }

    public static Schema convertSchema(final TableSchema tableSchema, final Collection<String> fields) {
        return convertSchema(tableSchema.getFields(), fields);
    }

    public static Schema convertSchema(final List<TableFieldSchema> fields) {
        return convertSchema(fields, null);
    }

    public static Schema convertSchema(final List<TableFieldSchema> fields, final Collection<String> includedFields) {
        Schema.Builder builder = Schema.builder();
        for(final TableFieldSchema field : fields) {
            if(includedFields != null && !includedFields.contains(field.getName())) {
                continue;
            }
            builder.addField(field.getName(), getFieldType(field.getType(), field.getMode(), field.getFields()));
        }
        return builder.build();
    }

    private static Object convertValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            switch (fieldType.getTypeName()){
                case ARRAY:
                case ITERABLE:
                    return new ArrayList<>();
            }
            return null;
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return value;
            case STRING:
                return value.toString();
            case BYTES:
                final byte[] bytes = ((ByteBuffer)value).array();
                return Arrays.copyOf(bytes, bytes.length);
            case DECIMAL:
                //fieldType.getLogicalType().
                //final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                return BigDecimal.valueOf(0, 0);
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
                return value;
            case DATETIME:
                if(value instanceof Utf8) {
                    return Instant.parse(value.toString());
                } else if(value instanceof Long) {
                    return Instant.ofEpochMilli((Long) value / 1000);
                }
                return value;
            case ROW:
                return convert(fieldType.getRowSchema(), (GenericRecord) value);
            case ARRAY:
                return ((List<Object>) value).stream()
                        .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case ITERABLE:
                return Collections.singletonList(value).stream()
                        .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return LocalDate.ofEpochDay((Integer) value);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return Instant.ofEpochMilli((Integer)value);
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Instant.ofEpochMilli((Long) value / 1000);
                } else {

                }
                return value;
            case BYTE:
            case MAP:
            default:
                return null;
        }
    }

    private static Schema.FieldType convertFieldType(final org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case BOOLEAN:
                return Schema.FieldType.BOOLEAN;
            case ENUM:
                return Schema.FieldType.logicalType(EnumerationType.create(avroSchema.getEnumSymbols()));
            case STRING:
                return Schema.FieldType.STRING;
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    return Schema.FieldType.DECIMAL;
                }
                return Schema.FieldType.BYTES;
            case INT:
                if(LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.DATE;
                } else if(LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.TIME;
                }
                return Schema.FieldType.INT32;
            case LONG:
                if(LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                    return Schema.FieldType.DATETIME;
                } else if(LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                    return Schema.FieldType.DATETIME;
                } else if(LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.TIME;
                }
                return Schema.FieldType.INT64;
            case FLOAT:
                return Schema.FieldType.FLOAT;
            case DOUBLE:
                return Schema.FieldType.DOUBLE;
            case RECORD:
                return Schema.FieldType.row(convertSchema(avroSchema));
            case ARRAY:
                return Schema.FieldType.array(convertFieldType(avroSchema.getElementType()));
            case MAP:
                return Schema.FieldType.map(Schema.FieldType.STRING, convertFieldType(avroSchema.getValueType()));
            case UNION:
                final boolean nullable = avroSchema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
                final org.apache.avro.Schema unnested = avroSchema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                return convertFieldType(unnested).withNullable(nullable);
            case NULL:
            default:
                return Schema.FieldType.STRING;
        }

    }

    private static Schema.FieldType getFieldType(final String type, final String mode, final List<TableFieldSchema> fields) {
        if("REPEATED".equals(mode)) {
            return Schema.FieldType.array(getFieldType(type, "REQUIRED", fields)).withNullable(false);
        }
        final boolean nullable = !"REQUIRED".equals(mode);
        switch (type) {
            case "STRING":
                return Schema.FieldType.STRING.withNullable(nullable);
            case "BYTES":
                return Schema.FieldType.BYTES.withNullable(nullable);
            case "INT64":
            case "INTEGER":
                return Schema.FieldType.INT64.withNullable(nullable);
            case "FLOAT64":
            case "FLOAT":
                return Schema.FieldType.DOUBLE.withNullable(nullable);
            case "BOOL":
            case "BOOLEAN":
                return Schema.FieldType.BOOLEAN.withNullable(nullable);
            case "DATE":
                return CalciteUtils.DATE.withNullable(nullable);
            case "TIME":
                return CalciteUtils.TIME.withNullable(nullable);
            case "DATETIME":
                return Schema.FieldType.DATETIME.withNullable(nullable);
            case "TIMESTAMP":
                return Schema.FieldType.DATETIME.withNullable(nullable);
            case "STRUCT":
            case "RECORD":
                return Schema.FieldType.row(convertSchema(fields)).withNullable(nullable);
            case "NUMERIC":
                return Schema.FieldType.DECIMAL.withNullable(nullable);
            case "GEOGRAPHY":
                return Schema.FieldType.STRING.withNullable(nullable);
            default:
                return Schema.FieldType.STRING.withNullable(nullable);
        }
    }


}
