package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.io.BaseEncoding;
import com.google.gson.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TableRowToAvroConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TableRowToAvroConverter.class);

    public static Schema convertSchema(final TableSchema tableSchema) {
        try {
            return convertSchema(tableSchema.getFields(), "root");
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse: " + tableSchema.toString(), e);
        }
    }

    public static GenericRecord convert(final TableSchema tableSchema, final TableRow tableRow) {
        final Schema schema = convertSchema(tableSchema);
        return convert(schema, tableRow);
    }

    public static GenericRecord convert(final Schema schema, final TableRow tableRow) {
        if(tableRow == null) {
            return null;
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            builder.set(field.name(), convertRecordValue(field.schema(), tableRow.get(field.name())));
        }
        return builder.build();
    }

    public static GenericRecord convertQueryResult(final Schema schema, final TableRow tableRow) {
        if(tableRow == null) {
            return null;
        }
        final JsonObject jsonObject = new Gson().fromJson(tableRow.toString(), JsonObject.class);
        return convertQueryResult(schema, jsonObject);
    }

    public static GenericRecord convertQueryResult(final Schema schema, final JsonObject tableRow) {
        if(tableRow == null) {
            return null;
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        final JsonArray fieldValues = new Gson().fromJson(tableRow.get("f").toString(), JsonArray.class);
        for(int i=0; i<schema.getFields().size(); i++) {
            final Schema.Field field = schema.getFields().get(i);
            final JsonElement fieldValue = fieldValues.get(i).getAsJsonObject().get("v");
            builder.set(field.name(), convertRecordValueQueryResult(field.schema(), fieldValue));
        }
        return builder.build();
    }

    private static Schema convertSchema(final List<TableFieldSchema> fields, final String name) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(name).fields();
        for (final TableFieldSchema field : fields) {
            schemaFields = schemaFields.name(field.getName()).type(convertFieldType(field, false)).noDefault();
        }
        return schemaFields.endRecord();
    }

    private static Schema convertFieldType(final TableFieldSchema schema, final boolean inarray) {
        final boolean nullable = !"REQUIRED".equals(schema.getMode());
        if(!inarray && "REPEATED".equals(schema.getMode())) {
            final Schema arraySchema = Schema.createArray(convertFieldType(schema, true));
            return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema) : arraySchema;
        }
        return switch (schema.getType()) {
            case "BYTES" -> nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
            case "STRING" -> nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
            case "JSON" -> nullable ? AvroSchemaUtil.NULLABLE_JSON : AvroSchemaUtil.REQUIRED_JSON;
            case "INT64", "INTEGER" -> {
                if (schema.containsKey("avroSchema") && "INT".equals(schema.get("avroSchema"))) {
                    yield nullable ? AvroSchemaUtil.NULLABLE_INT : AvroSchemaUtil.REQUIRED_INT;
                }
                yield nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
            }
            case "FLOAT64", "FLOAT" -> {
                if (schema.containsKey("avroSchema") && "FLOAT".equals(schema.get("avroSchema"))) {
                    yield nullable ? AvroSchemaUtil.NULLABLE_FLOAT : AvroSchemaUtil.REQUIRED_FLOAT;
                }
                yield nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
            }
            case "BOOLEAN" -> nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
            case "DATE" -> nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
            case "TIME" -> nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
            case "DATETIME" -> nullable ? AvroSchemaUtil.NULLABLE_SQL_DATETIME_TYPE : AvroSchemaUtil.REQUIRED_SQL_DATETIME_TYPE;
            case "TIMESTAMP" -> nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case "NUMERIC" -> {
                int precision = schema.get("precision") == null ? 38 : (int)schema.get("precision");
                int scale = schema.get("scale") == null ? 9 : (int)schema.get("scale");
                final Schema numeric = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
                yield nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), numeric) : numeric;
            }
            case "RECORD", "STRUCT" -> {
                final Schema structSchema = convertSchema(schema.getFields(), schema.getName());
                yield nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), structSchema) : structSchema;
            }
            default -> throw new IllegalArgumentException("BigQuery TableSchema type: " + schema.toString() + " is not supported!");
        };
    }

    private static Object convertRecordValue(final Schema schema, final Object value) {

        if(value == null) {
            return null;
        }

        switch (schema.getType()) {
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    final BigDecimal decimal = new BigDecimal((String)value).setScale(scale);
                    return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
                } else {
                    final byte[] bytes = BaseEncoding.base64().decode((String) value);
                    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                    return byteBuffer;
                }
            }
            case ENUM:
            case STRING: {
                return value.toString();
            }
            case INT: {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalDate localDate = DateTimeUtil.toLocalDate(value.toString());
                        return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                    } else if(value instanceof LocalDate) {
                        return ((Long)((LocalDate) value).toEpochDay()).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value.toString());
                        return DateTimeUtil.toMilliOfDay(localTime);
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for time");
                    }
                } else {
                    return Integer.valueOf(value.toString());
                }
            }
            case LONG: {
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant.parse(value.toString()).getMillis();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final java.time.Instant instant = java.time.Instant.parse(Instant.parse(value.toString()).toString());
                    final long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
                    return micros;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return Long.valueOf(value.toString());
                } else {
                    return Long.valueOf(value.toString());
                }
            }
            case FLOAT:
                return (Float) value;
            case DOUBLE:
                return (Double) value;
            case BOOLEAN:
                return (Boolean) value;
            case RECORD:
                return convert(schema, ((TableRow)value));
            case UNION: {
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return convertRecordValue(childSchema, value);
                }
                throw new IllegalStateException("");
            }
            case ARRAY:
                return ((List<Object>)value).stream()
                        .map(v -> convertRecordValue(schema.getElementType(), v))
                        .collect(Collectors.toList());
            case NULL:
            case MAP:
            default:
                return null;
        }
    }

    private static Object convertRecordValueQueryResult(final Schema schema, final JsonElement value) {

        if(value == null || value.isJsonNull()) {
            return null;
        }

        switch (schema.getType()) {
            case FIXED:
            case BYTES: {
                return ByteBuffer.wrap(Base64.getDecoder().decode(value.getAsString()));
            }
            case ENUM:
            case STRING: {
                return value.getAsString();
            }
            case INT: {
                final JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = DateTimeUtil.toLocalDate(value.toString());
                    return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final LocalTime localTime = DateTimeUtil.toLocalTime(value.getAsString());
                    return DateTimeUtil.toMilliOfDay(localTime);
                } else {
                    if (primitive.isString()) {
                        return parseInt(primitive.getAsString());
                    } else if (primitive.isNumber()) {
                        return primitive.getAsInt();
                    }
                    throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
                }
            }
            case LONG: {
                final JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    if(primitive.isString()) {
                        return parseLong(primitive.getAsString()) * 1000;
                    } else if(primitive.isNumber()) {
                        return primitive.getAsLong();
                    }
                    throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    if(primitive.isString()) {
                        return parseLong(primitive.getAsString()) * 1000000;
                    } else if(primitive.isNumber()) {
                        return primitive.getAsLong();
                    }
                    throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return value.getAsLong();
                } else {
                    if(primitive.isString()) {
                        return parseLong(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        return primitive.getAsLong();
                    }
                    throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
                }
            }
            case FLOAT: {
                final JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isString()) {
                    return parseFloat(primitive.getAsString());
                } else if (primitive.isNumber()) {
                    return primitive.getAsFloat();
                }
                throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
            }
            case DOUBLE: {
                final JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isString()) {
                    return parseDouble(primitive.getAsString());
                } else if (primitive.isNumber()) {
                    return primitive.getAsDouble();
                }
                throw new IllegalStateException("Illegal json primitive value: " + primitive.toString() + " for schema: " + schema.getType().getName());
            }
            case BOOLEAN:
                return value.getAsBoolean();
            case RECORD:
                return convertQueryResult(schema, value.getAsJsonObject());
            case UNION: {
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return convertRecordValueQueryResult(childSchema, value);
                }
                throw new IllegalStateException("");
            }
            case ARRAY: {
                final List<Object> list = new ArrayList<>();
                for (final JsonElement element : value.getAsJsonArray()) {
                    final Object elementValue = convertRecordValueQueryResult(schema.getElementType(), element);
                    list.add(elementValue);
                }
                return list;
            }
            case NULL:
            case MAP:
            default:
                return null;
        }
    }

    private static Float parseFloat(String str) {
        if(str.contains("E") || str.contains("e")) {
            try {
                return DecimalFormat.getNumberInstance().parse(str).floatValue();
            } catch (ParseException e) {
                return null;
            }
        }
        return Float.valueOf(str);
    }

    private static Double parseDouble(String str) {
        if(str.contains("E") || str.contains("e")) {
            try {
                return DecimalFormat.getNumberInstance().parse(str).doubleValue();
            } catch (ParseException e) {
                return null;
            }
        }
        return Double.valueOf(str);
    }

    private static Integer parseInt(String str) {
        if(str.contains("E") || str.contains("e")) {
            try {
                return DecimalFormat.getNumberInstance().parse(str).intValue();
            } catch (ParseException e) {
                return null;
            }
        }
        return Integer.valueOf(str);
    }

    private static Long parseLong(String str) {
        if(str.contains("E") || str.contains("e")) {
            try {
                return DecimalFormat.getNumberInstance().parse(str).longValue();
            } catch (ParseException e) {
                return null;
            }
        }
        return Long.parseLong(str);
    }

}
