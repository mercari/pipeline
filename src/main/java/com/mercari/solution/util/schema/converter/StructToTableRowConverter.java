package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructToTableRowConverter {

    public static TableSchema convertSchema(final Type type) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Type.StructField field : type.getStructFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableRow convert(final Struct struct) {
        TableRow row = new TableRow();
        for (final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            if ("f".equals(fieldName)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            row = setFieldValue(row, fieldName, field.getType(), struct);
        }
        return row;
    }

    public static Object convertTableRowValue(final Value value) {
        if(value == null || value.isNull()) {
            return null;
        }
        if(value.isCommitTimestamp()) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case BOOL -> value.getBool();
            case BYTES -> value.getBytes().toByteArray();
            case STRING -> value.getString();
            case JSON -> value.getJson();
            case INT64 -> value.getInt64();
            case FLOAT32 -> value.getFloat32();
            case FLOAT64 -> value.getFloat64();
            case DATE -> value.getDate().toString();
            case TIMESTAMP -> value.getTimestamp().toString();
            case NUMERIC -> value.getNumeric().toString();
            case STRUCT -> convert(value.getStruct());
            case PG_JSONB -> value.getPgJsonb();
            case PG_NUMERIC -> value.getString();
            case ARRAY ->
                switch (value.getType().getArrayElementType().getCode()) {
                    case BOOL -> value.getBoolArray();
                    case STRING -> value.getStringArray();
                    case JSON -> value.getJsonArray();
                    case BYTES -> value.getBytesArray().stream().map(ByteArray::toByteArray).toList();
                    case INT64 -> value.getInt64Array();
                    case FLOAT32 -> value.getFloat32Array();
                    case FLOAT64 -> value.getFloat64Array();
                    case NUMERIC -> value.getNumericArray().stream().map(BigDecimal::toString).toList();
                    case DATE -> value.getDateArray().stream().map(d -> d.toString()).toList();
                    case TIMESTAMP -> value.getTimestampArray().stream().map(t -> t.toString()).toList();
                    case PG_JSONB -> value.getPgJsonbArray();
                    case PG_NUMERIC -> value.getStringArray();
                    case STRUCT -> value.getStructArray().stream().map(s -> convert(s)).toList();
                    default -> throw new IllegalArgumentException("Not supported struct array type: " + value.getType().getArrayElementType().getCode());
                };

            default -> throw new IllegalArgumentException("Not supported struct type: " + value.getType().getCode());
        };
    }

    private static TableFieldSchema convertTableFieldSchema(Type.StructField field) {
        return convertTableFieldSchema(field.getType(), field.getName());
    }

    private static TableFieldSchema convertTableFieldSchema(Type fieldType, final String fieldName) {
        final String mode = "NULLABLE";//fieldType.getNullable() ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setName(fieldName)
                .setMode(mode);

        return switch (fieldType.getCode()) {
            case BOOL -> tableFieldSchema.setType("BOOLEAN");
            case STRING -> tableFieldSchema.setType("STRING");
            case JSON -> tableFieldSchema.setType("JSON");
            case BYTES -> tableFieldSchema.setType("BYTES");
            case INT64 -> tableFieldSchema.setType("INTEGER");
            case FLOAT32, FLOAT64 -> tableFieldSchema.setType("FLOAT");
            case NUMERIC -> tableFieldSchema.setType("NUMERIC");
            case DATE -> tableFieldSchema.setType("DATE");
            case TIMESTAMP -> tableFieldSchema.setType("TIMESTAMP");
            case STRUCT -> {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType.getStructFields().stream()
                        .map(StructToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                yield tableFieldSchema.setType("RECORD").setFields(childTableFieldSchemas);
            }
            case ARRAY -> {
                if(Type.Code.STRUCT.equals(fieldType.getArrayElementType().getCode())) {
                    final List<TableFieldSchema> childTableFieldSchemas = fieldType.getArrayElementType().getStructFields().stream()
                            .map(StructToTableRowConverter::convertTableFieldSchema)
                            .collect(Collectors.toList());
                    yield tableFieldSchema
                            .setType("RECORD")
                            .setFields(childTableFieldSchemas)
                            .setMode("REPEATED");
                } else {
                    yield tableFieldSchema
                            .setType(convertTableFieldSchema(fieldType.getArrayElementType(), fieldName).getType())
                            .setMode("REPEATED");
                }
            }
            default -> throw new IllegalArgumentException(fieldType + " is not supported for bigquery.");
        };
    }

    private static TableRow setFieldValue(TableRow row, final String fieldName, final Type type,final Struct struct) {
        if (struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        return switch (type.getCode()) {
            case JSON -> row.set(fieldName, struct.getJson(fieldName));
            case STRING -> row.set(fieldName, struct.getString(fieldName));
            case BYTES -> row.set(fieldName, struct.getBytes(fieldName).toByteArray());
            case BOOL -> row.set(fieldName, struct.getBoolean(fieldName));
            case INT64 -> row.set(fieldName, struct.getLong(fieldName));
            case FLOAT32 -> row.set(fieldName, struct.getFloat(fieldName));
            case FLOAT64 -> row.set(fieldName, struct.getDouble(fieldName));
            case NUMERIC -> row.set(fieldName, struct.getBigDecimal(fieldName));
            case DATE -> {
                final Date date = struct.getDate(fieldName);
                final LocalDate localDate = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                yield row.set(fieldName, localDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
            }
            case TIMESTAMP -> row.set(fieldName, struct.getTimestamp(fieldName).toString());
            case STRUCT -> {
                final Struct childStruct = struct.getStruct(fieldName);
                TableRow childRow = new TableRow();
                for (Type.StructField field : childStruct.getType().getStructFields()) {
                    childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                }
                yield row.set(fieldName, childRow);
            }
            case ARRAY -> setArrayFieldValue(row, fieldName, type.getArrayElementType(), struct);
            default -> row;
        };
    }

    private static TableRow setArrayFieldValue(TableRow row, final String fieldName, final Type type, final Struct struct) {
        if (struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        return switch (type.getCode()) {
            case STRING -> row.set(fieldName, struct.getStringList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case JSON -> row.set(fieldName, struct.getJsonList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case BYTES -> row.set(fieldName, struct.getBytesList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.toByteArray())
                        .collect(Collectors.toList()));
            case BOOL -> row.set(fieldName, struct.getBooleanList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case INT64 -> row.set(fieldName, struct.getLongList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case FLOAT32 -> row.set(fieldName, struct.getFloatList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case FLOAT64 -> row.set(fieldName, struct.getDoubleList(fieldName).stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
            case NUMERIC -> row.set(fieldName, struct.getBigDecimalList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case DATE -> row.set(fieldName, struct.getDateList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(date -> LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                        .map(localDate -> localDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                        .collect(Collectors.toList()));
            case TIMESTAMP -> {
                final List<String> timestampList = struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(timestamp -> timestamp.toString())
                        .collect(Collectors.toList());
                yield row.set(fieldName, timestampList);
            }
            case STRUCT -> {
                final List<TableRow> childRows = new ArrayList<>();
                for (Struct childStruct : struct.getStructList(fieldName)) {
                    if (childStruct == null) {
                        continue;
                    }
                    TableRow childRow = new TableRow();
                    for (final Type.StructField field : childStruct.getType().getStructFields()) {
                        childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                    }
                    childRows.add(childRow);
                }
                yield row.set(fieldName, childRows);
            }
            case ARRAY -> row; // Not support ARRAY in ARRAY
            default -> row;
        };
    }


}
