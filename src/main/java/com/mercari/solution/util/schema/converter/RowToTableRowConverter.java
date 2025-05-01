package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.io.BaseEncoding;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RowToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RowToTableRowConverter.class);

    private static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATTER_HH_MM_SS   = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final org.joda.time.format.DateTimeFormatter FORMATTER_TIMESTAMP = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static TableRow convert(final Row row) {
        return convert(row.getSchema(), row);
    }

    public static TableRow convert(final Schema schema, final Row row) {
        if(row == null) {
            return null;
        }
        final TableRow tableRow = new TableRow();
        for(final Schema.Field field : schema.getFields()) {
            tableRow.set(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
        }
        return tableRow;
    }

    public static TableSchema convertTableSchema(final Schema schema) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    private static Object convertValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING:
            case BOOLEAN:
            case INT64:
            case DOUBLE:
                return value;
            case DECIMAL: {
                return value.toString();
            }
            case BYTES:
                return BaseEncoding.base64().encode(((byte[]) value));
            case INT32:
                return ((Integer) value).longValue();
            case FLOAT:
                return ((Float) value).doubleValue();
            case DATETIME:
                return ((Instant) value).toString(FORMATTER_TIMESTAMP);
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return ((LocalDate) value).format(FORMATTER_YYYY_MM_DD);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return ((LocalTime) value).format(FORMATTER_HH_MM_SS);
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    final Row base = (Row) value;
                    final LocalDateTime localDateTime = SqlTypes.DATETIME.toInputType(base);
                    return localDateTime.format(FORMATTER_HH_MM_SS);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                    return RowSchemaUtil.toString(fieldType, enumValue);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                return convert(fieldType.getRowSchema(), (Row) value);
            case ITERABLE:
            case ARRAY:
                return ((List<?>) value).stream()
                        .map(v -> convertValue(fieldType.getCollectionElementType(), v))
                        .collect(Collectors.toList());
            case MAP: {
                final Map<Object, Object> map = (Map)value;
                return map.entrySet().stream()
                        .map(entry -> new TableRow()
                                .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                .set("value", convertValue(fieldType.getMapValueType(), entry.getValue())))
                        .collect(Collectors.toList());
            }
            case BYTE:
            case INT16:
            default:
                return value;
        }
    }

    private static TableFieldSchema convertTableFieldSchema(final Schema.Field field) {
        return convertTableFieldSchema(field.getType(), field.getOptions(), field.getName());
    }

    private static TableFieldSchema convertTableFieldSchema(final Schema.FieldType fieldType, final Schema.Options fieldOptions, final String fieldName) {
        final String mode = fieldType.getNullable() ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setMode(mode);

        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return tableFieldSchema.setName(fieldName).setType("BOOLEAN");
            case STRING: {
                if(RowSchemaUtil.isSqlTypeJson(fieldOptions)) {
                    return tableFieldSchema.setName(fieldName).setType("JSON");
                }
                return tableFieldSchema.setName(fieldName).setType("STRING");
            }
            case BYTES:
                return tableFieldSchema.setName(fieldName).setType("BYTES");
            case DECIMAL:
                return tableFieldSchema.setName(fieldName).setType("NUMERIC");
            case INT16:
            case INT32:
            case INT64:
                return tableFieldSchema.setName(fieldName).setType("INTEGER");
            case FLOAT:
            case DOUBLE:
                return tableFieldSchema.setName(fieldName).setType("FLOAT");
            case DATETIME:
                return tableFieldSchema.setName(fieldName).setType("TIMESTAMP");
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return tableFieldSchema.setName(fieldName).setType("DATE");
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return tableFieldSchema.setName(fieldName).setType("TIME");
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    return tableFieldSchema.setName(fieldName).setType("DATETIME");
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String values = fieldType.getLogicalType(EnumerationType.class).getValues().stream().collect(Collectors.joining(","));
                    return tableFieldSchema.setName(fieldName).setType("STRING").setDescription(values);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW: {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType.getRowSchema().getFields().stream()
                        .map(RowToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                return tableFieldSchema.setName(fieldName).setType("RECORD").setFields(childTableFieldSchemas);
            }
            case ITERABLE:
            case ARRAY: {
                if(Schema.TypeName.ROW.equals(fieldType.getCollectionElementType().getTypeName())) {
                    final List<TableFieldSchema> childTableFieldSchemas = fieldType
                            .getCollectionElementType()
                            .getRowSchema()
                            .getFields()
                            .stream()
                            .map(RowToTableRowConverter::convertTableFieldSchema)
                            .collect(Collectors.toList());
                    return tableFieldSchema
                            .setName(fieldName)
                            .setType("RECORD")
                            .setFields(childTableFieldSchemas)
                            .setMode("REPEATED");
                } else {
                    return tableFieldSchema
                            .setName(fieldName)
                            .setType(convertTableFieldSchema(fieldType.getCollectionElementType(), fieldOptions, "").getType())
                            .setMode("REPEATED");
                }
            }
            case MAP: {
                final List<TableFieldSchema> fields = new ArrayList<>();
                fields.add(new TableFieldSchema()
                        .setName("key")
                        .setMode("REQUIRED")
                        .setType("STRING"));
                fields.add(convertTableFieldSchema(fieldType.getMapValueType(), fieldOptions, "value"));
                return tableFieldSchema
                        .setName(fieldName)
                        .setType("RECORD")
                        .setFields(fields)
                        .setMode("REPEATED");
            }
            case BYTE:
            default:
                throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        }
    }

}
