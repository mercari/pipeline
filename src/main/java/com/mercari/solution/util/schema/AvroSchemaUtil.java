package com.mercari.solution.util.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.*;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.schema.converter.JsonToAvroConverter;
import com.mercari.solution.util.schema.converter.RowToRecordConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AvroSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaUtil.class);

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        INTEGER,
        FLOAT64,
        FLOAT,
        NUMERIC,
        BOOLEAN,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        JSON,
        GEOGRAPHY,
        STRUCT,
        RECORD
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

    public static final Schema REQUIRED_STRING = Schema.create(Schema.Type.STRING);
    public static final Schema REQUIRED_BYTES = Schema.create(Schema.Type.BYTES);
    public static final Schema REQUIRED_BOOLEAN = Schema.create(Schema.Type.BOOLEAN);
    public static final Schema REQUIRED_INT = Schema.create(Schema.Type.INT);
    public static final Schema REQUIRED_LONG = Schema.create(Schema.Type.LONG);
    public static final Schema REQUIRED_FLOAT = Schema.create(Schema.Type.FLOAT);
    public static final Schema REQUIRED_DOUBLE = Schema.create(Schema.Type.DOUBLE);
    public static final Schema REQUIRED_JSON = SchemaBuilder.builder().stringBuilder().prop("sqlType", "JSON").endString();

    public static final Schema REQUIRED_LOGICAL_UUID_TYPE = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    public static final Schema REQUIRED_LOGICAL_DATE_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema REQUIRED_LOGICAL_TIME_MILLI_TYPE = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema REQUIRED_LOGICAL_TIME_MICRO_TYPE = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_TIMESTAMP_MILLI_TYPE = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_DECIMAL_TYPE = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
    public static final Schema REQUIRED_SQL_DATETIME_TYPE = SchemaBuilder.builder().stringBuilder().prop("sqlType", "DATETIME").endString();
    public static final Schema REQUIRED_SQL_GEOGRAPHY_TYPE = SchemaBuilder.builder().stringBuilder().prop("sqlType", "GEOGRAPHY").endString();
    public static final Schema REQUIRED_ARRAY_DOUBLE_TYPE = Schema.createArray(Schema.create(Schema.Type.DOUBLE));

    public static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    public static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    public static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    public static final Schema NULLABLE_INT = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    public static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    public static final Schema NULLABLE_FLOAT = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
    public static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
    public static final Schema NULLABLE_JSON = SchemaBuilder.unionOf()
            .stringBuilder().prop("sqlType", "JSON").endString().and()
            .nullType()
            .endUnion();

    public static final Schema NULLABLE_LOGICAL_UUID_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)));
    public static final Schema NULLABLE_LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    public static final Schema NULLABLE_LOGICAL_TIME_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
    public static final Schema NULLABLE_LOGICAL_TIME_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_TIMESTAMP_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_DECIMAL_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)));
    public static final Schema NULLABLE_SQL_DATETIME_TYPE = SchemaBuilder.unionOf()
            .stringBuilder().prop("sqlType", "DATETIME").endString().and()
            .nullType()
            .endUnion();
    public static final Schema NULLABLE_SQL_GEOGRAPHY_TYPE = SchemaBuilder.unionOf()
            .stringBuilder().prop("sqlType", "GEOGRAPHY").endString().and()
            .nullType()
            .endUnion();
    public static final Schema NULLABLE_ARRAY_INT_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.INT)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_LONG_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.LONG)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_FLOAT_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.FLOAT)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_DOUBLE_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_DECIMAL_TYPE = Schema.createUnion(
            Schema.createArray(LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES))),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_BOOLEAN_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.BOOLEAN)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_TIME_TYPE = Schema.createUnion(
            Schema.createArray(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_DATE_TYPE = Schema.createUnion(
            Schema.createArray(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_TIMESTAMP_TYPE = Schema.createUnion(
            Schema.createArray(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_STRING_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.STRING)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_JSON_TYPE = Schema.createUnion(
            Schema.createArray(SchemaBuilder.builder().stringBuilder().prop("sqlType", "JSON").endString()),
            Schema.create(Schema.Type.NULL));

    public static final Schema NULLABLE_MAP_STRING = Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.createMap(
                    Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.create(Schema.Type.STRING))));

    private static final Pattern PATTERN_FIELD_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /**
     * Convert BigQuery {@link TableSchema} object to Avro {@link Schema} object.
     *
     * @param tableSchema BigQuery TableSchema object.
     * @return Avro Schema object.
     */
    public static Schema convertSchema(TableSchema tableSchema) {
        return convertSchema(tableSchema, null, "root");
    }

    public static Schema convertSchema(final TableSchema tableSchema, final String name) {
        return convertSchema(tableSchema, null, name);
    }

    public static Schema convertSchema(TableSchema tableSchema, final Collection<String> includedFields, final String name) {
        return convertSchema(tableSchema, includedFields, name, "root");
    }

    public static Schema convertSchema(TableSchema tableSchema, final Collection<String> includedFields, final String name, final String namespace) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(name).fields();
        for(final TableFieldSchema fieldSchema : tableSchema.getFields()) {
            if(includedFields != null && !includedFields.contains(fieldSchema.getName())) {
                continue;
            }
            schemaFields.name(fieldSchema.getName()).type(convertSchema(fieldSchema, namespace)).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(Entity entity) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            schemaFields.name(entry.getKey()).type(convertSchema(entry.getKey(), entry.getValue().getValueTypeCase(), entry.getValue())).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(final JsonArray sobjectDescribeFields, final List<String> fieldNames) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        sobjectDescribeFields.forEach(element -> {
            final JsonObject field = element.getAsJsonObject();
            if(fieldNames == null || fieldNames.contains(field.get("name").getAsString())) {
                schemaFields
                        .name(field.get("name").getAsString())
                        .type(convertSchema(field))
                        .noDefault();
            }
        });
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(final String jsonAvroSchema) {
        return new Schema.Parser().parse(jsonAvroSchema);
    }


    /**
     * Extract logical type decimal from {@link Schema}.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical decimal type.
     */
    public static LogicalTypes.Decimal getLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return getLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale);
    }

    /**
     * Check Avro {@link Schema} is logical type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical type.
     */
    public static boolean isLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType());
    }

    /**
     * Check Avro {@link Schema} is logical type local-timestamp or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical type local-timestamp.
     */
    public static boolean isLogicalTypeLocalTimestampMillis(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isLogicalTypeLocalTimestampMillis(childSchema);
        }
        if(schema.getLogicalType() == null && schema.getProp("logicalType") == null) {
            return false;
        } else if(schema.getLogicalType() != null) {
            switch (schema.getLogicalType().getName()) {
                case "local-timestamp-millis":
                    return true;
                default:
                    return false;
            }
        } else {
            switch (schema.getProp("logicalType")) {
                case "local-timestamp-millis":
                    return true;
                default:
                    return false;
            }
        }
    }

    /**
     * Check Avro {@link Schema} is logical type local-timestamp or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical type local-timestamp.
     */
    public static boolean isLogicalTypeLocalTimestampMicros(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isLogicalTypeLocalTimestampMicros(childSchema);
        }
        if(schema.getLogicalType() == null && schema.getProp("logicalType") == null) {
            return false;
        } else if(schema.getLogicalType() != null) {
            switch (schema.getLogicalType().getName()) {
                case "local-timestamp-micros":
                    return true;
                default:
                    return false;
            }
        } else {
            switch (schema.getProp("logicalType")) {
                case "local-timestamp-micros":
                    return true;
                default:
                    return false;
            }
        }
    }

    /**
     * Check Avro {@link Schema} is sql datetime type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is sql datetime type.
     */
    public static boolean isSqlTypeDatetime(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeDatetime(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "DATETIME".equals(sqlType);
    }

    /**
     * Check Avro {@link Schema} is sql geography type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is sql geography type.
     */
    public static boolean isSqlTypeGeography(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeGeography(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "GEOGRAPHY".equals(sqlType);
    }

    public static boolean isSqlTypeJson(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeJson(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "JSON".equals(sqlType);
    }

    /**
     * Extract child Avro schema from nullable union Avro {@link Schema}.
     *
     * @param schema Avro Schema object.
     * @return Child Avro schema or input schema if not union schema.
     */
    public static Schema unnestUnion(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            return schema.getTypes().stream()
                    .filter(s -> !s.getType().equals(Schema.Type.NULL))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
        }
        return schema;
    }

    public static GenericRecordBuilder copy(final GenericRecord record, Schema targetSchema) {
        final Schema sourceSchema = record.getSchema();
        final GenericRecordBuilder builder = new GenericRecordBuilder(targetSchema);
        for(final Schema.Field field : targetSchema.getFields()) {
            if(sourceSchema.getField(field.name()) == null) {
                continue;
            }
            builder.set(field.name(), record.get(field.name()));
        }
        return builder;
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema) {
        return toBuilder(schema, schema.getNamespace(), null);
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema, final Collection<String> fieldNames) {
        return toBuilder(schema, schema.getNamespace(), fieldNames);
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema, final String namespace, final Collection<String> fieldNames) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").namespace(namespace).fields();
        schema.getFields().forEach(f -> {
            if(fieldNames == null || fieldNames.contains(f.name())) {
                schemaFields.name(f.name()).type(f.schema()).noDefault();
            }
        });
        return schemaFields;
    }

    public static GenericRecordBuilder toBuilder(final GenericRecord record) {
        return toBuilder(record.getSchema(), record);
    }

    public static GenericRecordBuilder toBuilder(final Schema schema, final GenericRecord record) {
        return toBuilder(schema, record, new HashMap<>());
    }

    public static GenericRecordBuilder toBuilder(final Schema schema, final GenericRecord record, final Map<String, String> renameFields) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Schema.Field recordField = record.getSchema().getField(field.name());
            final String getFieldName = renameFields.getOrDefault(field.name(), field.name());
            final String setFieldName = field.name();
            if(recordField != null) {
                final Schema fieldSchema = unnestUnion(field.schema());
                final Object fieldValue = record.get(field.name());
                if(fieldValue == null) {
                    builder.set(field.name(), null);
                    continue;
                }
                if(!unnestUnion(recordField.schema()).getType().equals(fieldSchema.getType())) {
                    builder.set(field.name(), null);
                    continue;
                }

                switch (fieldSchema.getType()) {
                    case ARRAY -> {
                        final Schema elementSchema = unnestUnion(fieldSchema.getElementType());
                        if (elementSchema.getType().equals(Schema.Type.RECORD)) {
                            final List<GenericRecord> children = new ArrayList<>();
                            for (final GenericRecord child : (List<GenericRecord>) fieldValue) {
                                if (child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(elementSchema, child).build());
                                }
                            }
                            builder.set(field.name(), children);
                        } else {
                            builder.set(field.name(), fieldValue);
                        }
                    }
                    case RECORD -> {
                        final GenericRecord child = toBuilder(fieldSchema, (GenericRecord) fieldValue).build();
                        builder.set(field.name(), child);
                    }
                    default -> builder.set(field.name(), fieldValue);
                }
            } else if(renameFields.containsValue(setFieldName)) {
                final String getOuterFieldName = renameFields.entrySet().stream()
                        .filter(e -> e.getValue().equals(setFieldName))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .orElse(setFieldName);
                if(record.get(getOuterFieldName) == null) {
                    builder.set(setFieldName, null);
                    continue;
                }
                final Schema.Field rowField = record.getSchema().getField(getOuterFieldName);
                if(!field.schema().getType().getName().equals(rowField.schema().getType().getName())) {
                    builder.set(setFieldName, null);
                    continue;
                }

                switch (field.schema().getType()) {
                    case ARRAY -> {
                        if(field.schema().getElementType().equals(Schema.Type.RECORD)) {
                            final List<GenericRecord> children = new ArrayList<>();
                            for(final GenericRecord child : (Collection<GenericRecord>)record.get(getOuterFieldName)) {
                                if(child != null) {
                                    children.add(toBuilder(field.schema().getElementType(), child).build());
                                }
                            }
                            builder.set(setFieldName, children);
                        } else {
                            builder.set(setFieldName, record.get(getOuterFieldName));
                        }
                    }
                    case RECORD -> {
                        final GenericRecord child = toBuilder(field.schema(), (GenericRecord) record.get(getOuterFieldName)).build();
                        builder.set(setFieldName, child);
                    }
                    default -> builder.set(setFieldName, record.get(getOuterFieldName));
                }
            } else if(renameFields.containsKey(field.name())) {
                final String oldFieldName = renameFields.get(field.name());
                builder.set(field, record.get(oldFieldName));
            } else {
                if(isNullable(field.schema())) {
                    builder.set(field, null);
                }
            }
        }
        return builder;
    }

    public static SchemaBuilder.FieldAssembler<Schema> toSchemaBuilder(
            final Schema schema,
            final Collection<String> includeFields,
            final Collection<String> excludeFields) {

        return toSchemaBuilder(schema, includeFields, excludeFields, schema.getName());
    }

    public static SchemaBuilder.FieldAssembler<Schema> toSchemaBuilder(
            final Schema schema,
            final Collection<String> includeFields,
            final Collection<String> excludeFields,
            final String name) {

        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(name);
        SchemaBuilder.FieldAssembler<Schema> schemaFields = builder.fields();
        for(final Schema.Field field : schema.getFields()) {
            if(includeFields != null && !includeFields.contains(field.name())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(field.name())) {
                continue;
            }
            schemaFields = schemaFields.name(field.name()).type(field.schema()).noDefault();
        }
        return schemaFields;
    }

    public static Schema renameFields(final Schema schema, final Map<String, String> renameFields) {

        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(schema.getName());
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = builder.fields();
        for(final Schema.Field field : schema.getFields()) {
            if(renameFields.containsKey(field.name())) {
                schemaFields.name(renameFields.get(field.name())).type(field.schema()).noDefault();
            } else {
                schemaFields.name(field.name()).type(field.schema()).noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord merge(final Schema schema, final GenericRecord record, final Map<String, ? extends Object> values) {
        final GenericRecordBuilder builder = toBuilder(schema, record);
        for(Schema.Field field : schema.getFields()) {
            if(values.containsKey(field.name())) {
                builder.set(field.name(), values.get(field.name()));
            } else if(record.getSchema().getField(field.name()) != null) {
                builder.set(field.name(), record.get(field.name()));
            } else {
                builder.set(field.name(), null);
            }
        }
        return builder.build();
    }

    public static Schema merge(final Schema base, final Schema addition) {
        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(base.getName());
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = builder.fields();
        for(final Schema.Field field : base.getFields()) {
            schemaFields.name(field.name()).type(field.schema()).noDefault();
        }
        for(final Schema.Field field : addition.getFields()) {
            if(base.getField(field.name()) != null) {
                continue;
            }
            schemaFields.name(field.name()).type(field.schema()).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord create(final Schema schema, final Map<String, ? extends Object> values) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Schema fieldSchema = unnestUnion(field.schema());
            final Object fieldValue = values.get(field.name());
            if (fieldValue == null) {
                builder.set(field.name(), null);
                continue;
            }
            switch (fieldSchema.getType()) {
                case ARRAY -> {
                    final Schema elementSchema = unnestUnion(fieldSchema.getElementType());
                    if (elementSchema.getType().equals(Schema.Type.RECORD)) {
                        final List<GenericRecord> children = new ArrayList<>();
                        for (final Object child : (List<Object>) fieldValue) {
                            if (child == null) {
                                continue;
                            }
                            if (child instanceof Map) {
                                children.add(create(elementSchema, (Map<String, ? extends Object>) child));
                            } else if (child instanceof GenericRecord) {
                                children.add((GenericRecord) child);
                            }
                        }
                        builder.set(field.name(), children);
                    } else {
                        builder.set(field.name(), fieldValue);
                    }
                }
                case RECORD -> {
                    final GenericRecord child;
                    if (fieldValue instanceof Map) {
                        child = create(fieldSchema, (Map<String, ? extends Object>) fieldValue);
                    } else if (fieldValue instanceof GenericRecord) {
                        child = (GenericRecord) fieldValue;
                    } else {
                        child = null;
                    }
                    builder.set(field.name(), child);
                }
                default -> builder.set(field.name(), fieldValue);
            }
        }
        return builder.build();
    }

    public static Schema selectFields(final Schema schema, final List<String> fields) {
        return selectFieldsBuilder(schema, fields, "root").endRecord();
    }

    public static SchemaBuilder.FieldAssembler<Schema> selectFieldsBuilder(final Schema schema, final List<String> fields, final String name) {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.builder().record(name).fields();
        final Map<String, List<String>> childFields = new HashMap<>();
        for(String field : fields) {
            if(field.contains(".")) {
                final String[] strs = field.split("\\.", 2);
                if(childFields.containsKey(strs[0])) {
                    childFields.get(strs[0]).add(strs[1]);
                } else {
                    childFields.put(strs[0], new ArrayList<>(Arrays.asList(strs[1])));
                }
            } else if(schema.getField(field) != null) {
                builder.name(field).type(schema.getField(field).schema()).noDefault();
            } else {
                throw new IllegalStateException("Field not found: " + field);
            }
        }

        if(!childFields.isEmpty()) {
            for(var entry : childFields.entrySet()) {
                final Schema.Field childField = schema.getField(entry.getKey());
                Schema unnestedChildSchema = unnestUnion(childField.schema());
                final String childName = name + "." + entry.getKey();
                switch (unnestedChildSchema.getType()) {
                    case RECORD: {
                        final Schema selectedChildSchema = selectFieldsBuilder(unnestedChildSchema, entry.getValue(), childName).endRecord();
                        builder.name(entry.getKey()).type(toNullable(selectedChildSchema)).noDefault();
                        break;
                    }
                    case ARRAY: {
                        if(!unnestUnion(unnestedChildSchema.getElementType()).getType().equals(Schema.Type.RECORD)) {
                            throw new IllegalStateException();
                        }
                        final Schema childSchema = selectFieldsBuilder(unnestUnion(unnestedChildSchema.getElementType()), entry.getValue(), childName).endRecord();
                        builder.name(entry.getKey()).type(toNullable(Schema.createArray(toNullable(childSchema)))).noDefault();
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        return builder;
    }

    public static GenericData.EnumSymbol createEnumSymbol(final String name, final List<String> symbols, final String symbol) {
        if(symbol == null || !symbols.contains(symbol)) {
            return null;
        }
        final Schema enumSchema = Schema.createEnum(name, null, null, symbols);
        return new GenericData.EnumSymbol(enumSchema, symbol);
    }

    public static Schema createMapRecordSchema(final String name, final Schema keySchema, final Schema valueSchema) {

        final Schema.Field keyField = new Schema.Field("key", keySchema, null, (Object)null);
        final Schema.Field valueField = new Schema.Field("value", valueSchema, null, (Object)null);
        return Schema.createRecord(name, null, null, false, List.of(keyField, valueField));
    }

    public static Schema createMapRecordsSchema(final String name, final Schema keySchema, final Schema valueSchema) {
        return SchemaBuilder.builder().array().items(
                SchemaBuilder.builder().record("map")
                        .fields()
                        .name("key").type(Schema.create(Schema.Type.STRING)).noDefault()
                        .endRecord());
    }

    public static Object getRawValue(final GenericRecord record, final String fieldName) {
        if(record == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }
        if(!record.hasField(fieldName)) {
            return null;
        }
        return record.get(fieldName);
    }

    public static Object getValue(final GenericRecord record, final String fieldName) {
        if(record == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }
        if(!record.hasField(fieldName)) {
            return null;
        }

        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN, FLOAT, DOUBLE -> {
                return value;
            }
            case ENUM, STRING -> {
                return value.toString();
            }
            case BYTES, FIXED -> {
                final byte[] bytes = ((ByteBuffer) value).array();
                return Arrays.copyOf(bytes, bytes.length);
            }
            case INT -> {
                final Integer intValue = (Integer) value;
                if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    return LocalDate.ofEpochDay(intValue.longValue());
                } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofSecondOfDay(intValue / 1000);
                }
                return intValue;
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue / 1000);
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofSecondOfDay(longValue / 1000_000);
                }
                return longValue;
            }
            case RECORD, MAP -> {
                return value;
            }
            case ARRAY -> {
                return ((List<Object>) value).stream()
                        .map(v -> {
                            if (v == null) {
                                return null;
                            }
                            final Schema arraySchema = unnestUnion(fieldSchema.getElementType());
                            switch (arraySchema.getType()) {
                                case BOOLEAN, FLOAT, DOUBLE -> {
                                    return v;
                                }
                                case ENUM, STRING -> {
                                    return v.toString();
                                }
                                case FIXED, BYTES -> {
                                    final byte[] b = ((ByteBuffer) v).array();
                                    return Arrays.copyOf(b, b.length);
                                }
                                case INT -> {
                                    final Integer intValue = (Integer) v;
                                    if (LogicalTypes.date().equals(arraySchema.getLogicalType())) {
                                        return LocalDate.ofEpochDay(intValue.longValue());
                                    } else if (LogicalTypes.timeMillis().equals(arraySchema.getLogicalType())) {
                                        return LocalTime.ofSecondOfDay(intValue / 1000);
                                    }
                                    return intValue;
                                }
                                case LONG -> {
                                    final Long longValue = (Long) v;
                                    if (LogicalTypes.timestampMillis().equals(arraySchema.getLogicalType())) {
                                        return Instant.ofEpochMilli(longValue);
                                    } else if (LogicalTypes.timestampMicros().equals(arraySchema.getLogicalType())) {
                                        return Instant.ofEpochMilli(longValue / 1000);
                                    } else if (LogicalTypes.timeMicros().equals(arraySchema.getLogicalType())) {
                                        return LocalTime.ofSecondOfDay(longValue / 1000_000);
                                    }
                                    return longValue;
                                }
                                case MAP, RECORD -> {
                                    return v;
                                }
                                default -> {
                                    return null;
                                }
                            }
                        })
                        .collect(Collectors.toList());
            }
            default -> {
                return null;
            }
        }
    }

    public static byte[] getBytes(final GenericRecord record, final String fieldName) {
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema schema = unnestUnion(field.schema());
        switch (schema.getType()) {
            case BYTES:
                return ((ByteBuffer)value).array();
            default:
                return null;
        }
    }

    public static ByteBuffer getAsBytes(final GenericRecord record, final String fieldName) {
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema schema = unnestUnion(field.schema());
        return switch (schema.getType()) {
            case BYTES -> (ByteBuffer)value;
            case STRING -> ByteBuffer.wrap(Base64.getDecoder().decode(value.toString()));
            default -> null;
        };
    }

    public static org.joda.time.Instant getTimestamp(final GenericRecord record, final String fieldName) {
        return getTimestamp(record, fieldName, Instant.ofEpochSecond(0L));
    }

    public static org.joda.time.Instant getTimestamp(final GenericRecord record, final String fieldName, final Instant defaultTimestamp) {
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return defaultTimestamp;
        }
        if(!record.hasField(fieldName)) {
            return defaultTimestamp;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return defaultTimestamp;
        }
        final Schema schema = unnestUnion(field.schema());
        switch (schema.getType()) {
            case ENUM, STRING -> {
                final String stringValue = value.toString().trim();
                try {
                    java.time.Instant instant = DateTimeUtil.toInstant(stringValue);
                    return DateTimeUtil.toJodaInstant(instant);
                } catch (Exception e) {
                    return defaultTimestamp;
                }
            }
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay((int) value);
                    return new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                            0, 0, DateTimeZone.UTC).toInstant();
                }
                return defaultTimestamp;
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue / 1000);
                }
                return defaultTimestamp;
            }
            default -> {
                return defaultTimestamp;
            }
        }
    }

    public static String getAsString(final Object record, final String fieldName) {
        if(record == null) {
            return null;
        }
        return getAsString((GenericRecord) record, fieldName);
    }

    public static String getAsString(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN, FLOAT, DOUBLE, ENUM, STRING -> {
                return value.toString();
            }
            case FIXED, BYTES -> {
                return Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
            }
            case INT -> {
                if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    return LocalDate.ofEpochDay((int) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                } else {
                    return value.toString();
                }
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue));
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue / 1000));
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(longValue * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                } else {
                    return longValue.toString();
                }
            }
            case RECORD, MAP, ARRAY -> {
                return value.toString();
            }
            default -> {
                return null;
            }
        }
    }

    public static Long getAsLong(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = getRecordValue(record, fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN -> {
                return (Boolean) value ? 1L : 0L;
            }
            case FLOAT -> {
                return ((Float) value).longValue();
            }
            case DOUBLE -> {
                return ((Double) value).longValue();
            }
            case ENUM, STRING -> {
                try {
                    return Long.valueOf(value.toString());
                } catch (Exception e) {
                    return null;
                }
            }
            case FIXED, BYTES -> {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if (isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale).longValue();
                }
                return null;
            }
            case INT -> {
                return ((Integer) value).longValue();
            }
            case LONG -> {
                return (Long) value;
            }
            default -> {
                return null;
            }
        }
    }

    public static Double getAsDouble(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> (Boolean) value ? 1D : 0D;
            case FLOAT -> ((Float) value).doubleValue();
            case DOUBLE -> ((Double) value);
            case ENUM, STRING -> {
                try {
                    yield Double.valueOf(value.toString());
                } catch (Exception e) {
                    yield null;
                }
            }
            case FIXED, BYTES -> {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if (isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    yield BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale).doubleValue();
                }
                yield null;
            }
            case INT -> ((Integer) value).doubleValue();
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    yield longValue.doubleValue();
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    yield longValue.doubleValue() / 1000;
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    yield longValue.doubleValue() / 1000;
                } else {
                    yield longValue.doubleValue();
                }
            }
            default -> null;
        };
    }

    public static BigDecimal getAsBigDecimal(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> BigDecimal.valueOf((Boolean) value ? 1D : 0D);
            case FLOAT -> BigDecimal.valueOf(((Float) value).doubleValue());
            case DOUBLE -> BigDecimal.valueOf((Double) value);
            case ENUM, STRING -> {
                try {
                    yield BigDecimal.valueOf(Double.valueOf(value.toString()));
                } catch (Exception e) {
                    yield null;
                }
            }
            case FIXED, BYTES -> {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if (isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    yield BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale);
                }
                yield null;
            }
            case INT -> BigDecimal.valueOf(((Integer) value).longValue());
            case LONG -> BigDecimal.valueOf((Long) value);
            default -> null;
        };
    }

    // for bigtable
    public static ByteString getAsByteString(final GenericRecord record, final String fieldName) {
        Object value;
        if(record == null || fieldName == null) {
            value = null;
        } else if(!record.hasField(fieldName)) {
            value = null;
        } else {
            final Schema.Field field = record.getSchema().getField(fieldName);
            if(field == null) {
                value = null;
            } else {
                value = record.get(fieldName);
                value = getAsPrimitive(field.schema(), value);
            }
        }
        return BigtableSchemaUtil.toByteString(value);
    }

    public static BigDecimal getAsBigDecimal(final Schema fieldSchema, final ByteBuffer byteBuffer) {
        return getAsBigDecimal(fieldSchema, byteBuffer.array());
    }

    public static BigDecimal getAsBigDecimal(final Schema fieldSchema, final byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        if(isLogicalTypeDecimal(fieldSchema)) {
            final int scale = fieldSchema.getObjectProp("scale") != null ?
                    Integer.parseInt(fieldSchema.getObjectProp("scale").toString()) : 0;
            return BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        }
        return null;
    }

    public static Map<String,Object> asStandardMap(final GenericRecord record, final Collection<String> fieldNames) {
        final Map<String, Object> standardValues = new HashMap<>();
        if(record == null) {
            return standardValues;
        }
        for(final Schema.Field field : record.getSchema().getFields()) {
            if(fieldNames != null && !fieldNames.isEmpty() && !fieldNames.contains(field.name())) {
                continue;
            }
            final Object standardValue = getAsStandard(field.schema(), record.get(field.name()));
            standardValues.put(field.name(), standardValue);
        }
        return standardValues;
    }

    public static Object getAsStandard(final Object record, final String fieldName) {
        return getAsStandard((GenericRecord) record, fieldName);
    }

    public static Object getAsStandard(final GenericRecord record, final String fieldName) {
        if(record == null || fieldName == null) {
            return null;
        }
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        final Object value = record.get(fieldName);
        return getAsStandard(field.schema(), value);
    }

    public static Object getAsStandard(final Schema fieldSchema, final Object value) {
        if(value == null) {
            return null;
        }
        return switch (fieldSchema.getType()) {
            case UNION -> getAsStandard(unnestUnion(fieldSchema), value);
            case BOOLEAN -> switch (value) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (value) {
                case Utf8 utf8 -> utf8.toString();
                case String s -> s;
                case Object o -> o.toString();
            };
            case BYTES -> {
                final ByteBuffer byteBuffer = switch (value) {
                    case ByteBuffer bf -> bf;
                    case byte[] b -> ByteBuffer.wrap(b);
                    case String s -> ByteBuffer.wrap(Base64.getDecoder().decode(s));
                    default -> throw new IllegalArgumentException();
                };
                if(LogicalTypes.decimal(38, 9).equals(fieldSchema.getLogicalType())) {
                    yield BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), 9);
                } else if(LogicalTypes.decimal(76, 38).equals(fieldSchema.getLogicalType())) {
                    yield BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), 38);
                } else {
                    yield byteBuffer;
                }
            }
            case ENUM -> switch (value) {
                case Utf8 utf8 -> utf8.toString();
                case String s -> s;
                default -> throw new IllegalArgumentException("Could not convert enum value: " + value);
            };
            case INT -> {
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> DateTimeUtil.toLocalDate(s);
                        case Number i -> LocalDate.ofEpochDay(i.longValue());
                        default -> throw new IllegalArgumentException();
                    };
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> DateTimeUtil.toLocalTime(s);
                        case Number i -> LocalTime.ofNanoOfDay(i.intValue() * 1000L * 1000L);
                        default -> throw new IllegalArgumentException();
                    };
                } else {
                    yield switch (value) {
                        case String s -> Integer.parseInt(s);
                        case Number i -> i.intValue();
                        case Boolean b -> b ? 1 : 0;
                        default -> throw new IllegalArgumentException();
                    };
                }
            }
            case LONG -> {
                if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> DateTimeUtil.toInstant(s);
                        case Number i -> DateTimeUtil.toInstant(i.longValue());
                        default -> throw new IllegalArgumentException();
                    };
                } else if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> DateTimeUtil.toInstant(s);
                        case Number i -> java.time.Instant.ofEpochMilli(i.longValue());
                        default -> throw new IllegalArgumentException();
                    };
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    yield switch (value) {
                        case String s -> DateTimeUtil.toLocalTime(s);
                        case Number i -> LocalTime.ofNanoOfDay(i.longValue() * 1000L);
                        default -> throw new IllegalArgumentException();
                    };
                } else {
                    yield switch (value) {
                        case String s -> Long.parseLong(s);
                        case Number i -> i.longValue();
                        case Boolean b -> b ? 1L : 0L;
                        default -> throw new IllegalArgumentException();
                    };
                }
            }
            case FLOAT -> switch (value) {
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                case Boolean b -> b ? 1F : 0F;
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (value) {
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                case Boolean b -> b ? 1D : 0D;
                default -> throw new IllegalArgumentException();
            };
            case RECORD -> switch (value) {
                case GenericRecord record -> asStandardMap(record, null);
                default -> throw new IllegalArgumentException();
            };
            case ARRAY -> switch (value) {
                case List list -> {
                    final List<Object> standardValues = new ArrayList<>();
                    for(final Object v : list) {
                        final Object standardValue = getAsStandard(fieldSchema.getElementType(), v);
                        standardValues.add(standardValue);
                    }
                    yield  standardValues;
                }
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException();
        };
    }

    public static Object getAsPrimitive(final GenericRecord record, final String fieldName) {
        if(record == null) {
            return null;
        }
        if(fieldName.contains(".")) {
            final String[] fields = fieldName.split("\\.", 2);
            final String parentField = fields[0];
            if(!record.hasField(parentField)) {
                return null;
            }
            final GenericRecord child = (GenericRecord) record.get(parentField);
            return getAsPrimitive(child, fields[1]);
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }

        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        return getAsPrimitive(field.schema(), value);
    }

    public static Object getAsPrimitive(final Object record, final org.apache.beam.sdk.schemas.Schema.FieldType fieldType, final String field) {

        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final String parentField = fields[0];
            if(!((GenericRecord) record).hasField(parentField)) {
                return null;
            }
            final Object child = ((GenericRecord) record).get(parentField);
            return getAsPrimitive(child, fieldType, fields[1]);
        }

        if(!((GenericRecord) record).hasField(field)) {
            return null;
        }

        final Object value = ((GenericRecord) record).get(field);
        if(value == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> {
                return value;
            }
            case STRING -> {
                return value.toString();
            }
            case DATETIME -> {
                final Schema fieldSchema = unnestUnion(((GenericRecord) record).getSchema().getField(field).schema());
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return (Long) value * 1000L;
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return value;
                }
                throw new IllegalStateException("field: " + field + " is illegal timestamp logicalType: " + fieldSchema + ", value: " + value);
            }
            case LOGICAL_TYPE -> {
                final Schema fieldSchema = unnestUnion(((GenericRecord) record).getSchema().getField(field).schema());
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    if(value instanceof LocalDate) {
                        return Long.valueOf(((LocalDate) value).toEpochDay()).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else {
                        throw new IllegalArgumentException();
                    }
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                        return (Long) value * 1000L;
                    } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                        return value;
                    }
                    throw new IllegalStateException();
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return fieldSchema.getEnumSymbols().get((Integer) value);
                } else {
                    throw new IllegalStateException("field: " + field + " is illegal logicalType: " + fieldSchema + ", value: " + value);
                }
            }
            case ITERABLE, ARRAY -> {
                final Schema elementSchema = unnestUnion(unnestUnion(((GenericRecord) record).getSchema().getField(field).schema()).getElementType());
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> {
                        return value;
                    }
                    case STRING -> {
                        return ((List<Object>) value).stream().map(Object::toString).collect(Collectors.toList());
                    }
                    case DATETIME -> {
                        final long m;
                        if (LogicalTypes.timestampMillis().equals(elementSchema.getLogicalType())) {
                            m = 1000L;
                        } else if (LogicalTypes.timestampMicros().equals(elementSchema.getLogicalType())) {
                            m = 1L;
                        } else {
                            m = 1L;
                        }
                        return ((List<Long>) value).stream()
                                .map(l -> l * m)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            return value;
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                            final long m;
                            if (LogicalTypes.timeMillis().equals(elementSchema.getLogicalType())) {
                                m = 1000L;
                            } else if (LogicalTypes.timeMicros().equals(elementSchema.getLogicalType())) {
                                m = 1L;
                            } else {
                                m = 1L;
                            }
                            return ((List<Long>) value).stream()
                                    .map(l -> l * m)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                            throw new IllegalStateException();
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    case ROW, MAP -> {
                        return ((List<Object>) value).stream()
                                .map(o -> {
                                    if(o instanceof GenericRecord) {
                                        return asPrimitiveMap((GenericRecord) o);
                                    } else if(o instanceof Map<?,?>) {
                                        return o;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    default -> throw new IllegalStateException("Not supported primitive type: " + fieldType.getCollectionElementType());
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object getAsPrimitive(final org.apache.beam.sdk.schemas.Schema.FieldType fieldType, final Object fieldValue) {
        if(fieldValue == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN, DATETIME -> {
                return fieldValue;
            }
            case STRING -> {
                return fieldValue.toString();
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return fieldValue;
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return fieldValue;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return fieldValue;
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT32, INT64, FLOAT, DOUBLE, BOOLEAN, DATETIME -> {
                        return fieldValue;
                    }
                    case STRING -> {
                        return ((List<Object>) fieldValue).stream()
                                .map(Object::toString)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        return ((List<Object>) fieldValue).stream()
                                .map(o -> {
                                    if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                                        return o;
                                    } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                                        return o;
                                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                                        return o;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    case ROW, MAP -> {
                        return ((List<Object>) fieldValue).stream()
                                .map(o -> {
                                    if(o instanceof GenericRecord) {
                                        return asPrimitiveMap((GenericRecord) o);
                                    } else if(o instanceof Map<?,?>) {
                                        return o;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    default -> throw new IllegalStateException("Not supported primitive type: " + fieldType.getCollectionElementType());
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object getAsPrimitive(final Schema fieldSchema, final Object fieldValue) {
        if(fieldValue == null) {
            return null;
        }
        return switch (fieldSchema.getType()) {
            case FLOAT, DOUBLE, BOOLEAN, BYTES, MAP -> fieldValue;
            case ENUM -> switch (fieldValue) {
                case Number i -> i.intValue();
                case String s -> fieldSchema.getEnumSymbols().indexOf(s);
                case GenericData.EnumSymbol s -> fieldSchema.getEnumSymbols().indexOf(s.toString());
                default -> throw new IllegalArgumentException();
            };
            case STRING -> fieldValue.toString();
            case INT -> {
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    yield fieldValue;
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    yield ((Integer) fieldValue).longValue() * 1000L;
                } else {
                    yield fieldValue;
                }
            }
            case LONG -> {
                if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    yield fieldValue;
                } else if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    yield ((Long) fieldValue) * 1000L;
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    yield ((Integer) fieldValue).longValue() * 1000L;
                } else {
                    yield fieldValue;
                }
            }
            case RECORD -> {
                final GenericRecord record = (GenericRecord) fieldValue;
                yield asPrimitiveMap(record);
            }
            case ARRAY -> switch (fieldSchema.getElementType().getType()) {
                case FLOAT, DOUBLE, BOOLEAN, BYTES -> fieldValue;
                case STRING -> ((List<Object>) fieldValue).stream()
                            .map(Object::toString)
                            .collect(Collectors.toList());
                case INT -> ((List<Integer>) fieldValue).stream()
                        .map(i -> {
                            if(LogicalTypes.date().equals(fieldSchema.getElementType().getLogicalType())) {
                                return i;
                            } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                                return i.longValue() * 1000L;
                            } else {
                                return i;
                            }
                        })
                        .collect(Collectors.toList());
                case LONG -> ((List<Long>) fieldValue).stream()
                        .map(l -> {
                            if(LogicalTypes.timestampMicros().equals(fieldSchema.getElementType().getLogicalType())) {
                                return l;
                            } else if(LogicalTypes.timestampMillis().equals(fieldSchema.getElementType().getLogicalType())) {
                                return l * 1000L;
                            } else if(LogicalTypes.timeMillis().equals(fieldSchema.getElementType().getLogicalType())) {
                                return l * 1000L;
                            } else {
                                return l;
                            }
                        })
                        .collect(Collectors.toList());
                case MAP -> ((List<Object>) fieldValue).stream()
                            .map(o -> {
                                if(o instanceof GenericRecord) {
                                    return asPrimitiveMap((GenericRecord) o);
                                } else if(o instanceof Map<?,?>) {
                                    return o;
                                } else {
                                    throw new IllegalStateException();
                                }
                            })
                            .collect(Collectors.toList());
                case RECORD -> ((List<GenericRecord>) fieldValue).stream()
                        .map(AvroSchemaUtil::asPrimitiveMap)
                        .collect(Collectors.toList());
                default -> throw new IllegalStateException("Not supported primitive type: " + fieldSchema.getElementType());
            };
            case NULL -> null;
            case UNION -> getAsPrimitive(unnestUnion(fieldSchema), fieldValue);
            default -> throw new IllegalStateException("Nut supported type: " + fieldSchema.getType());
        };
    }

    public static List<Float> getAsFloatList(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return new ArrayList<>();
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return new ArrayList<>();
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return new ArrayList<>();
        }
        final Schema fieldSchema = unnestUnion(field.schema());
        if(!fieldSchema.getType().equals(Schema.Type.ARRAY)) {
            throw new IllegalStateException();
        }

        final List<?> list = (List) record.get(fieldName);
        return switch (fieldSchema.getElementType().getType()) {
            case BOOLEAN -> list.stream()
                    .map(v -> (Boolean) v)
                    .map(v -> v ? 1.0F : 0.0F)
                    .collect(Collectors.toList());
            case FLOAT -> list.stream()
                    .map(v -> (Float) v)
                    .collect(Collectors.toList());
            case DOUBLE -> list.stream()
                    .map(v -> (Double) v)
                    .map(Double::floatValue)
                    .collect(Collectors.toList());
            case ENUM, STRING -> list.stream()
                    .map(Object::toString)
                    .map(Float::valueOf)
                    .collect(Collectors.toList());
            case INT -> list.stream()
                    .map(v -> (Integer) v)
                    .map(Integer::floatValue)
                    .collect(Collectors.toList());
            case LONG -> list.stream()
                    .map(v -> (Long) v)
                    .map(Long::floatValue)
                    .collect(Collectors.toList());
            default -> throw new IllegalStateException();
        };
    }

    public static Object convertPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType fieldType, Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getTypeName()) {
            case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> primitiveValue;
            case DATETIME -> (Long) primitiveValue;
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield (Integer) primitiveValue;
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield (Long) primitiveValue;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final int index = (Integer) primitiveValue;
                    yield fieldType.getLogicalType(EnumerationType.class).valueOf(index);
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> switch (fieldType.getCollectionElementType().getTypeName()) {
                case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> primitiveValue;
                case DATETIME -> ((List<Long>) primitiveValue).stream()
                        .map(l -> l)
                        .collect(Collectors.toList());
                case LOGICAL_TYPE -> {
                    if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                        yield primitiveValue;
                    } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                        yield ((List<Long>) primitiveValue).stream()
                                .map(l -> l)
                                .collect(Collectors.toList());
                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                        yield ((List<Integer>) primitiveValue).stream()
                                .map(index -> fieldType.getLogicalType(EnumerationType.class).valueOf(index))
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }
                case MAP -> ((List<Object>) primitiveValue).stream()
                        .map(o -> {
                            if(o instanceof Map<?,?>) {
                                return o;
                            } else {
                                throw new IllegalStateException("Could not convert value: " + o + " to map");
                            }
                        })
                        .collect(Collectors.toList());
                case ROW -> ((List<Object>) primitiveValue).stream()
                        .map(o -> {
                            if(o instanceof Map<?,?>) {
                                return create(RowToRecordConverter.convertSchema(fieldType.getCollectionElementType().getRowSchema()), (Map) o);
                            } else if(o instanceof GenericRecord) {
                                return o;
                            } else {
                                throw new IllegalStateException("Could not convert value: " + o + " to record");
                            }
                        })
                        .collect(Collectors.toList());
                default -> throw new IllegalStateException("Not supported array element type: " + fieldType.getCollectionElementType());
            };
            default -> throw new IllegalStateException();
        };
    }

    public static Map<String, Object> asPrimitiveMap(final GenericRecord record) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(record == null) {
            return primitiveMap;
        }
        for(final Schema.Field field : record.getSchema().getFields()) {
            final Object value;
            if(record.hasField(field.name())) {
                value = getAsPrimitive(record, field.name());
            } else {
                value = null;
            }
            primitiveMap.put(field.name(), value);
        }
        return primitiveMap;
    }

    public static Instant toInstant(final Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Long) {
            return Instant.ofEpochMilli(DateTimeUtil.assumeEpochMilliSecond((Long)value));
        }if(value instanceof Integer) {
            return Instant.ofEpochMilli(DateTimeUtil.assumeEpochMilliSecond(((Integer)value).longValue()));
        } else if(value instanceof String) {
            return Instant.parse((String) value);
        }
        return null;
    }

    public static ByteBuffer toByteBuffer(final BigDecimal decimal) {
        final BigDecimal newDecimal = decimal
                .setScale(9, RoundingMode.HALF_UP)
                .scaleByPowerOfTen(9);
        return ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray());
    }

    public static Object toTimestampValue(final Instant timestamp) {
        if(timestamp == null) {
            return null;
        }
        return timestamp.getMillis() * 1000L;
    }

    public static String convertNumericBytesToString(final byte[] bytes, final int scale) {
        if(bytes == null) {
            return null;
        }
        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        final StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static boolean isNullable(final Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            return schema.getTypes().stream().map(Schema::getType).anyMatch(Schema.Type.NULL::equals);
        }
        return false;
    }

    public static boolean isNestedSchema(final Schema schema) {
        if(schema == null) {
            return false;
        }
        return schema.getFields().stream()
                .map(f -> unnestUnion(f.schema()))
                .anyMatch(s -> {
                    if(Schema.Type.RECORD.equals(s.getType())) {
                        return true;
                    } else if(Schema.Type.ARRAY.equals(s.getType())) {
                        return Schema.Type.RECORD.equals(unnestUnion(s.getElementType()).getType());
                    }
                    return false;
                });
    }

    public static byte[] encode(final GenericRecord record) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static byte[] encode(final Schema schema, final List<GenericRecord> records) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            for(final GenericRecord record : records) {
                datumWriter.write(record, binaryEncoder);
                binaryEncoder.flush();
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static byte[] encode(Object primitiveValue) throws IOException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            write(encoder, primitiveValue);
            encoder.flush();
            return outputStream.toByteArray();
        }
    }

    public static <T> byte[] encode(Class<T> clazz, T object) throws IOException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            AvroCoder.of(clazz).encode(object, outputStream);
            return outputStream.toByteArray();
        }
    }

    public static byte[] encodeUnion(Object object) throws IOException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            UnionMapCoder.unionValueCoder().encode(object, outputStream);
            return outputStream.toByteArray();
        }
    }

    public static GenericRecord decode(final Schema schema, final byte[] bytes) throws IOException {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = new GenericData.Record(schema);
        return datumReader.read(record, decoder);
    }

    public static List<GenericRecord> decodeFile(final byte[] bytes) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try(final InputStream is = new ByteArrayInputStream(bytes);
            final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(is, datumReader)) {
            final List<GenericRecord> records = new ArrayList<>();
            while(dataFileReader.hasNext()) {
                records.add(dataFileReader.next());
            }
            return records;
        } catch (final Exception e) {
            return new ArrayList<>();
        }
    }

    public static Object decode(com.mercari.solution.module.Schema.FieldType fieldType, byte[] bytes) throws IOException {
        return decode(null, fieldType, bytes);
    }

    public static Object decode(BinaryDecoder decoder, com.mercari.solution.module.Schema.FieldType fieldType, byte[] bytes) throws IOException {
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
            return read(decoder, fieldType);
        }
    }

    public static <T> T decode(Class<T> clazz, byte[] bytes) throws IOException {
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            return AvroCoder.of(clazz).decode(inputStream);
        }
    }

    public static Object decodeUnion(byte[] bytes) throws IOException {
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            return UnionMapCoder.unionValueCoder().decode(inputStream);
        }
    }

    private static void write(BinaryEncoder encoder, Object primitiveValue) throws IOException {
        if(primitiveValue == null) {
            encoder.writeNull();
            return;
        }
        switch (primitiveValue) {
            case String s -> encoder.writeString(s);
            case Boolean b -> encoder.writeBoolean(b);
            case Integer i -> encoder.writeInt(i);
            case Long l -> encoder.writeLong(l);
            case Float f -> encoder.writeFloat(f);
            case Double d -> encoder.writeDouble(d);
            case ByteBuffer bb -> encoder.writeBytes(bb);
            case byte[] b -> encoder.writeBytes(b);
            case Map<?,?> m -> {
                encoder.writeMapStart();
                encoder.setItemCount(m.size());
                for(Map.Entry<?,?> entry : m.entrySet()) {
                    encoder.startItem();
                    write(encoder, entry.getKey());
                    write(encoder, entry.getValue());
                }
                encoder.writeMapEnd();
            }
            case Collection<?> c -> {
                encoder.writeArrayStart();
                encoder.setItemCount(c.size());
                for(Object v : c) {
                    encoder.startItem();
                    write(encoder, v);
                }
                encoder.writeArrayEnd();
            }
            default -> {}

        }
    }

    private static Object read(BinaryDecoder decoder, com.mercari.solution.module.Schema.FieldType fieldType) throws IOException {
        return switch (fieldType.getType()) {
            case string, json -> decoder.readString();
            case bool -> decoder.readBoolean();
            case bytes, decimal -> decoder.readBytes(null);
            case int32, date, enumeration -> decoder.readInt();
            case int64, time, timestamp -> decoder.readLong();
            case float32 -> decoder.readFloat();
            case float64 -> decoder.readDouble();
            case map -> {
                final Map<String, Object> map = new HashMap<>();
                long len = decoder.readMapStart();
                if(len == 0) {
                    yield map;
                }
                do {
                    for(long i=0; i<len; i++) {
                        final String key = decoder.readString(null).toString();
                        final Object value = read(decoder, fieldType.getMapValueType());
                        map.put(key, value);
                    }
                } while((len = decoder.mapNext()) != 0);
                yield map;
            }
            case element -> decode(fieldType.getElementSchema().getAvroSchema(), decoder.readBytes(null).array());
            case array -> {
                final List<Object> list = new ArrayList<>();
                for(long i=decoder.readArrayStart(); i != 0; i = decoder.arrayNext()) {
                    for(long j=0; j<i; j++) {
                        final Object v = read(decoder, fieldType.getArrayValueType());
                        list.add(v);
                    }
                }
                yield list;
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static Object read(BinaryDecoder decoder, Schema fieldType) throws IOException {
        return switch (fieldType.getType()) {
            case STRING -> decoder.readString();
            case BOOLEAN -> decoder.readBoolean();
            case BYTES -> decoder.readBytes(null);
            case ENUM -> decoder.readEnum();
            case INT -> decoder.readInt();
            case LONG -> decoder.readLong();
            case FLOAT -> decoder.readFloat();
            case DOUBLE -> decoder.readDouble();
            case MAP -> {
                final Map<String, Object> map = new HashMap<>();
                decoder.readMapStart();
                yield map;
            }
            case ARRAY -> {
                List<?> list = new ArrayList<>();
                long count = decoder.readArrayStart();
                for(long i=0; i<count; i++) {
                    decoder.readArrayStart();
                }
                yield list;
            }
            case RECORD -> {
                final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(fieldType);
                GenericRecord record = new GenericData.Record(fieldType);
                yield datumReader.read(record, decoder);
            }
            case UNION -> read(decoder, unnestUnion(fieldType));
            case NULL -> null;
            default -> throw new IllegalArgumentException();
        };
    }

    public static Schema getAvroSchema(final byte[] bytes) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try(final InputStream is = new ByteArrayInputStream(bytes);
            final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(is, datumReader)) {
            return dataFileReader.getSchema();
        } catch (Exception e) {
            return null;
        }
    }

    public static Schema toNullable(final Schema schema) {
        if(isNullable(schema)) {
            return schema;
        }
        return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
    }

    public static boolean isValidFieldName(final String name) {
        if(name == null) {
            return false;
        }
        if(name.isEmpty()) {
            return false;
        }
        return PATTERN_FIELD_NAME.matcher(name).find();
    }

    public static Schema rename(final Schema schema, final String name) {
        return AvroSchemaUtil
                .toSchemaBuilder(schema, null, null, name)
                .endRecord();
    }

    public static Schema getChildSchema(final Schema schema, final String fieldName) {
        if(schema == null) {
            return null;
        }
        final Schema.Field field = schema.getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case RECORD -> {
                return fieldSchema;
            }
            case ARRAY -> {
                final Schema elementSchema = unnestUnion(fieldSchema.getElementType());
                switch (elementSchema.getType()) {
                    case RECORD:
                        return elementSchema;
                    default:
                        throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object convertDefaultValue(final Schema fieldSchema, final String defaultValue) {
        if(defaultValue == null) {
            return null;
        }
        return switch (fieldSchema.getType()) {
            case ENUM, STRING -> defaultValue;
            case BOOLEAN -> Boolean.valueOf(defaultValue);
            case BYTES -> Base64.getDecoder().decode(defaultValue);
            case FLOAT -> Float.valueOf(defaultValue);
            case DOUBLE -> Double.valueOf(defaultValue);
            case INT -> {
                if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    final LocalDate localDate = DateTimeUtil.toLocalDate(defaultValue);
                    yield Long.valueOf(localDate.toEpochDay()).intValue();
                } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    final LocalTime localTime = DateTimeUtil.toLocalTime(defaultValue);
                    final int epochMillis = Long.valueOf(localTime.toNanoOfDay() / 1000_000).intValue();
                    yield epochMillis;
                } else {
                    yield Integer.valueOf(defaultValue);
                }
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    long epochMillis = DateTimeUtil.toEpochMicroSecond(defaultValue) / 1000L;
                    yield epochMillis;
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    yield DateTimeUtil.toEpochMicroSecond(defaultValue);
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    final LocalTime localTime = DateTimeUtil.toLocalTime(defaultValue);
                    yield localTime.toNanoOfDay() / 1000L;
                } else {
                    yield Long.valueOf(defaultValue);
                }
            }
            case RECORD -> {
                final JsonElement element = new Gson().fromJson(defaultValue, JsonElement.class);
                yield JsonToAvroConverter.convert(fieldSchema, element);
            }
            case UNION -> convertDefaultValue(unnestUnion(fieldSchema), defaultValue);
            default -> throw new IllegalStateException("Not supported default value type: " + fieldSchema);
        };
    }

    private static Object getRecordValue(final GenericRecord record, final String fieldName) {
        if(!record.hasField(fieldName)) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        return value;
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema, final String namespace) {
        return convertSchema(
                fieldSchema,
                TableRowFieldMode.valueOf(fieldSchema.getMode() == null ? TableRowFieldMode.NULLABLE.name() : fieldSchema.getMode()),
                namespace,
                false);
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema, final TableRowFieldMode mode, final String parentNamespace, final boolean inArray) {
        if(mode.equals(TableRowFieldMode.REPEATED)) {
            //return Schema.createUnion(
            //        Schema.create(Schema.Type.NULL),
            //        Schema.createArray(convertSchema(fieldSchema, TableRowFieldMode.NULLABLE, parentNamespace)));
            return Schema.createArray(convertSchema(fieldSchema, TableRowFieldMode.REQUIRED, parentNamespace, true));
        }
        switch (TableRowFieldType.valueOf(fieldSchema.getType())) {
            case DATETIME -> {
                final Schema datetimeSchema = Schema.create(Schema.Type.STRING);
                datetimeSchema.addProp("sqlType", "DATETIME");
                return TableRowFieldMode.NULLABLE.equals(mode) ? Schema.createUnion(Schema.create(Schema.Type.NULL), datetimeSchema) : datetimeSchema;
            }
            case GEOGRAPHY -> {
                final Schema geoSchema = Schema.create(Schema.Type.STRING);
                geoSchema.addProp("sqlType", "GEOGRAPHY");
                return TableRowFieldMode.NULLABLE.equals(mode) ?
                        Schema.createUnion(Schema.create(Schema.Type.NULL), geoSchema) :
                        geoSchema;
            }
            case JSON -> {
                final Schema jsonSchema = Schema.create(Schema.Type.STRING);
                jsonSchema.addProp("sqlType", "JSON");
                return TableRowFieldMode.NULLABLE.equals(mode) ?
                        Schema.createUnion(Schema.create(Schema.Type.NULL), jsonSchema) :
                        jsonSchema;
            }
            case STRING -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_STRING : REQUIRED_STRING;
            }
            case BYTES -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_BYTES : REQUIRED_BYTES;
            }
            case INT64, INTEGER -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LONG : REQUIRED_LONG;
            }
            case FLOAT64, FLOAT -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_DOUBLE : REQUIRED_DOUBLE;
            }
            case BOOL, BOOLEAN -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_BOOLEAN : REQUIRED_BOOLEAN;
            }
            case DATE -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_DATE_TYPE : REQUIRED_LOGICAL_DATE_TYPE;
            }
            case TIME -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_TIME_MICRO_TYPE : REQUIRED_LOGICAL_TIME_MICRO_TYPE;
            }
            case TIMESTAMP -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            }
            case NUMERIC -> {
                return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_DECIMAL_TYPE : REQUIRED_LOGICAL_DECIMAL_TYPE;
            }
            case STRUCT, RECORD -> {
                final String namespace = parentNamespace == null ? "root" : parentNamespace + "." + fieldSchema.getName().toLowerCase();
                final List<Schema.Field> fields = fieldSchema.getFields().stream()
                        .map(f -> new Schema.Field(f.getName(), convertSchema(f, TableRowFieldMode.valueOf(f.getMode()), namespace, false), null, (Object) null))
                        .collect(Collectors.toList());
                final String capitalName = fieldSchema.getName().substring(0, 1).toUpperCase()
                        + fieldSchema.getName().substring(1).toLowerCase();
                if (TableRowFieldMode.NULLABLE.equals(mode)) {
                    return Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.createRecord(capitalName, fieldSchema.getDescription(), namespace, false, fields));
                } else {
                    return Schema.createRecord(inArray ? capitalName : fieldSchema.getName(), fieldSchema.getDescription(), namespace, false, fields);
                }
            }
            //final Schema recordSchema = Schema.createRecord(fieldSchema.getName(), fieldSchema.getDescription(), namespace, false, fields);
            //return TableRowFieldMode.NULLABLE.equals(mode) ? Schema.createUnion(Schema.create(Schema.Type.NULL), recordSchema) : recordSchema;
            //return recordSchema;
            default -> throw new IllegalArgumentException();
        }
    }

    private static Schema convertSchema(final String name, final Value.ValueTypeCase valueTypeCase, final Value value) {
        return switch (valueTypeCase) {
            case STRING_VALUE -> NULLABLE_STRING;
            case BLOB_VALUE -> NULLABLE_BYTES;
            case INTEGER_VALUE -> NULLABLE_LONG;
            case DOUBLE_VALUE -> NULLABLE_DOUBLE;
            case BOOLEAN_VALUE -> NULLABLE_BOOLEAN;
            case TIMESTAMP_VALUE -> NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case ENTITY_VALUE -> {
                final List<Schema.Field> fields = value.getEntityValue().getPropertiesMap().entrySet().stream()
                        .map(s -> new Schema.Field(s.getKey(), convertSchema(s.getKey(), s.getValue().getValueTypeCase(), s.getValue()), null, (Object) null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                yield Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createRecord(name, null, null, false, fields));
            }
            case ARRAY_VALUE -> {
                final Value av = value.getArrayValue().getValues(0);
                yield Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(convertSchema(name, av.getValueTypeCase(), av)));
            }
            case VALUETYPE_NOT_SET, NULL_VALUE -> convertSchema(name, value.getDefaultInstanceForType().getValueTypeCase(), value);
            default -> throw new IllegalArgumentException(String.format("%s %s is not supported!", valueTypeCase.name(), name));
        };
    }

    private static Schema convertSchema(final JsonObject jsonObject) {
        final boolean nullable;
        if(!jsonObject.has("nillable") || jsonObject.get("nillable").isJsonNull()) {
            nullable = true;
        } else {
            nullable = jsonObject.get("nillable").getAsBoolean();
        }

        return switch (jsonObject.get("type").getAsString().toLowerCase()) {
            case "id" -> REQUIRED_STRING;
            case "reference", "string", "base64", "textarea", "url", "phone", "email", "address", "location", "picklist" ->
                    nullable ? NULLABLE_STRING : REQUIRED_STRING;
            case "byte" -> nullable ? NULLABLE_BYTES : REQUIRED_BYTES;
            case "boolean" -> nullable ? NULLABLE_BOOLEAN : REQUIRED_BOOLEAN;
            case "int" -> nullable ? NULLABLE_LONG : REQUIRED_LONG;
            case "currency", "percent", "double" -> nullable ? NULLABLE_DOUBLE : REQUIRED_DOUBLE;
            case "date" -> nullable ? NULLABLE_LOGICAL_DATE_TYPE : REQUIRED_LOGICAL_DATE_TYPE;
            case "time" -> nullable ? NULLABLE_LOGICAL_TIME_MICRO_TYPE : REQUIRED_LOGICAL_TIME_MICRO_TYPE;
            case "datetime" -> nullable ? NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case "junctionidlist", "multipicklist" -> nullable ? Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    Schema.createArray(REQUIRED_STRING)) : Schema.createArray(REQUIRED_STRING);
            default -> nullable ? NULLABLE_STRING : REQUIRED_STRING;
        };
    }

}