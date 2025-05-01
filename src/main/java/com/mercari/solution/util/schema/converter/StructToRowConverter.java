package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.text.JsonUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class StructToRowConverter {

    public static Schema convertSchema(final Type type) {
        final Schema.Builder builder = Schema.builder();
        for(final Type.StructField field : type.getStructFields()) {
            builder.addField(Schema.Field.of(
                    field.getName(),
                    convertFieldType(field.getType()))
                    .withNullable(true));
        }
        return builder.build();
    }

    public static Row convert(final Schema schema, final Struct struct) {
        if(struct == null) {
            return null;
        }
        final Row.Builder builder = Row.withSchema(schema);
        final List<Object> values = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            values.add(getValue(field.getName(), field.getType(), struct));
        }
        return builder.addValues(values).build();
    }

    private static Object getValue(final String fieldName, final Schema.FieldType fieldType, final Struct struct) {
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return null;
        }
        if(struct.isNull(fieldName)) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN -> {
                return struct.getBoolean(fieldName);
            }
            case STRING -> {
                return struct.getString(fieldName);
            }
            case DECIMAL -> {
                return struct.getBigDecimal(fieldName);
            }
            case BYTES -> {
                return struct.getBytes(fieldName).toByteArray();
            }
            case INT16, INT32, INT64 -> {
                return struct.getLong(fieldName);
            }
            case FLOAT, DOUBLE -> {
                return struct.getDouble(fieldName);
            }
            case DATETIME -> {
                return Instant.ofEpochMilli(struct.getTimestamp(fieldName).toSqlTimestamp().toInstant().toEpochMilli());
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    final Date date = struct.getDate(fieldName);
                    return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return struct.getString(fieldName);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW -> {
                return convert(fieldType.getRowSchema(), struct);
            }
            case ARRAY, ITERABLE -> {
                return getArray(fieldName, fieldType.getCollectionElementType(), struct);
            }
            default -> {
                return new IllegalArgumentException("Unsupported field type: " + fieldType.getTypeName() + " for field: " + fieldName);
            }
        }
    }

    private static List<?> getArray(final String fieldName, final Schema.FieldType fieldType, final Struct struct) {
        if(struct.isNull(fieldName)) {
            return new ArrayList<>();
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return struct.getBooleanList(fieldName);
            case STRING:
                return struct.getStringList(fieldName);
            case DECIMAL:
                return struct.getBigDecimalList(fieldName);
            case BYTES:
                return struct.getBytesList(fieldName).stream()
                        .map(ByteArray::toByteArray)
                        .collect(Collectors.toList());
            case INT16:
            case INT32:
            case INT64:
                return struct.getLongList(fieldName);
            case FLOAT:
            case DOUBLE:
                return struct.getDoubleList(fieldName);
            case DATETIME:
                return struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(Timestamp::toSqlTimestamp)
                        .map(java.sql.Timestamp::toInstant)
                        .map(java.time.Instant::toEpochMilli)
                        .map(Instant::ofEpochMilli)
                        .collect(Collectors.toList());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return struct.getDateList(fieldName).stream()
                            .map(d -> LocalDate.of(d.getYear(), d.getMonth(), d.getDayOfMonth()))
                            .collect(Collectors.toList());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return struct.getStringList(fieldName);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                return struct.getStructList(fieldName).stream()
                        .map(s -> convert(fieldType.getRowSchema(), s))
                        .collect(Collectors.toList());
            case ARRAY:
            case ITERABLE:
                throw new IllegalStateException("Array in Array is not supported!");
            case BYTE:
            case MAP:
            default:
                return null;
        }
    }

    private static Schema.FieldType convertFieldType(final Type type) {
        return switch (type.getCode()) {
            case BYTES -> Schema.FieldType.BYTES.withNullable(true);
            case JSON, STRING -> Schema.FieldType.STRING;
            case INT64 -> Schema.FieldType.INT64.withNullable(true);
            case FLOAT32 -> Schema.FieldType.FLOAT;
            case FLOAT64 -> Schema.FieldType.DOUBLE;
            case NUMERIC, PG_NUMERIC -> Schema.FieldType.DECIMAL;
            case BOOL -> Schema.FieldType.BOOLEAN;
            case DATE -> CalciteUtils.DATE;
            case TIMESTAMP -> Schema.FieldType.DATETIME;
            case STRUCT -> Schema.FieldType.row(convertSchema(type));
            case ARRAY -> Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            default -> throw new IllegalArgumentException("Spanner type: " + type + " not supported!");
        };
    }

    public static Row convert(final Schema schema, final DataChangeRecord record) {
        final Map<String, Object> values = new HashMap<>();
        for(final Mod mod : record.getMods()) {
            final JsonObject keyJson = JsonUtil.fromJson(mod.getKeysJson()).getAsJsonObject();
            for(final Map.Entry<String, JsonElement> entry : keyJson.entrySet()) {
                final Schema.Field field = schema.getField(entry.getKey());
                values.put(field.getName(), getValue(field.getType(), entry.getValue()));
            }
            final JsonObject valuesJson = JsonUtil.fromJson(mod.getNewValuesJson()).getAsJsonObject();
            for(final Map.Entry<String, JsonElement> entry : valuesJson.entrySet()) {
                final Schema.Field field = schema.getField(entry.getKey());
                values.put(field.getName(), getValue(field.getType(), entry.getValue()));
            }
        }

        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Object getValue(final Schema.FieldType fieldType, final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING -> {
                return element.getAsString();
            }
            case BOOLEAN -> {
                return element.getAsBoolean();
            }
            case BYTE -> {
                return element.getAsByte();
            }
            case INT16 -> {
                return element.getAsShort();
            }
            case INT32 -> {
                return element.getAsInt();
            }
            case INT64 -> {
                return element.getAsLong();
            }
            case FLOAT -> {
                return element.getAsFloat();
            }
            case DOUBLE -> {
                return element.getAsDouble();
            }
            case DECIMAL -> {
                return element.getAsBigDecimal();
            }
            case DATETIME -> {
                return DateTimeUtil.toJodaInstant(element.getAsString());
            }
            case BYTES -> {
                return element.getAsString().getBytes();
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toLocalDate(element.getAsString());
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime(element.getAsString());
                } else if (RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return DateTimeUtil.toJodaInstant(element.getAsString());
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return RowSchemaUtil.toEnumerationTypeValue(fieldType, element.getAsString());
                } else {
                    return element.getAsString();
                }
            }
            case ITERABLE, ARRAY -> {
                final List<Object> array = new ArrayList<>();
                for (final JsonElement e : element.getAsJsonArray()) {
                    array.add(getValue(fieldType.getCollectionElementType(), e));
                }
                return array;
            }
            case ROW -> {
                final Map<String, Object> values = new HashMap<>();
                final JsonObject object = element.getAsJsonObject();
                for (final Map.Entry<String, JsonElement> entry : object.entrySet()) {
                    final Schema.Field field = fieldType.getRowSchema().getField(entry.getKey());
                    values.put(field.getName(), getValue(field.getType(), entry.getValue()));
                }
                return Row
                        .withSchema(fieldType.getRowSchema())
                        .withFieldValues(values)
                        .build();
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Row convertToDataChangeRow(final Schema schema, final DataChangeRecord record) {
        final Map<String, Object> values = new HashMap<>();
        values.put("partitionToken", record.getPartitionToken());
        values.put("commitTimestamp", DateTimeUtil.toJodaInstant(record.getCommitTimestamp()));
        values.put("serverTransactionId", record.getServerTransactionId());
        values.put("isLastRecordInTransactionInPartition", record.isLastRecordInTransactionInPartition());
        values.put("recordSequence", record.getRecordSequence());
        values.put("tableName", record.getTableName());

        final Schema rowTypeSchema = schema.getField("rowType").getType().getCollectionElementType().getRowSchema();
        final List<Row> rowTypes = new ArrayList<>();
        for(final ColumnType columnType : record.getRowType()) {
            final Map<String, Object> rowTypeValues = new HashMap<>();
            rowTypeValues.put("name", columnType.getName());
            final String code = StructSchemaUtil.convertChangeRecordTypeCode(columnType.getType().getCode());
            rowTypeValues.put("Type", RowSchemaUtil.toEnumerationTypeValue(rowTypeSchema
                    .getField("Type")
                    .getType(), code));
            rowTypeValues.put("isPrimaryKey", columnType.isPrimaryKey());
            rowTypeValues.put("ordinalPosition", columnType.getOrdinalPosition());
            final Row rowType = Row.withSchema(rowTypeSchema)
                    .withFieldValues(rowTypeValues)
                    .build();
            rowTypes.add(rowType);
        }
        values.put("rowType", rowTypes);

        final Schema modSchema = schema.getField("mods").getType().getCollectionElementType().getRowSchema();
        final List<Row> mods = new ArrayList<>();
        for(final Mod mod : record.getMods()) {
            final Map<String, Object> modValues = new HashMap<>();
            modValues.put("keysJson", mod.getKeysJson());
            modValues.put("oldValuesJson", mod.getOldValuesJson());
            modValues.put("newValuesJson", mod.getNewValuesJson());
            mods.add(Row.withSchema(modSchema)
                    .withFieldValues(modValues)
                    .build());
        }
        values.put("mods", mods);

        values.put("modType", RowSchemaUtil.toEnumerationTypeValue(
                schema.getField("modType").getType(),
                record.getModType().name()));
        values.put("valueCaptureType", RowSchemaUtil.toEnumerationTypeValue(schema
                .getField("valueCaptureType")
                .getType(), record.getValueCaptureType().name()));
        values.put("numberOfRecordsInTransaction", record.getNumberOfRecordsInTransaction());
        values.put("numberOfPartitionsInTransaction", record.getNumberOfPartitionsInTransaction());

        final ChangeStreamRecordMetadata metadata = record.getMetadata();
        if(metadata == null) {
            values.put("metadata", null);
        } else {
            final Schema metadataSchema = schema.getField("metadata").getType().getRowSchema();
            final Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("partitionToken", metadata.getPartitionToken());
            metadataValues.put("recordTimestamp", DateTimeUtil.toJodaInstant(metadata.getRecordTimestamp()));
            metadataValues.put("partitionStartTimestamp", DateTimeUtil.toJodaInstant(metadata.getPartitionStartTimestamp()));
            metadataValues.put("partitionEndTimestamp", DateTimeUtil.toJodaInstant(metadata.getPartitionEndTimestamp()));
            metadataValues.put("partitionCreatedAt", DateTimeUtil.toJodaInstant(metadata.getPartitionCreatedAt()));
            metadataValues.put("partitionScheduledAt", DateTimeUtil.toJodaInstant(metadata.getPartitionScheduledAt()));
            metadataValues.put("partitionRunningAt", DateTimeUtil.toJodaInstant(metadata.getPartitionRunningAt()));
            metadataValues.put("queryStartedAt", DateTimeUtil.toJodaInstant(metadata.getQueryStartedAt()));
            metadataValues.put("recordStreamStartedAt", DateTimeUtil.toJodaInstant(metadata.getRecordStreamStartedAt()));
            metadataValues.put("recordStreamEndedAt", DateTimeUtil.toJodaInstant(metadata.getRecordStreamEndedAt()));
            metadataValues.put("recordReadAt", DateTimeUtil.toJodaInstant(metadata.getRecordReadAt()));
            metadataValues.put("totalStreamTimeMillis", metadata.getTotalStreamTimeMillis());
            metadataValues.put("numberOfRecordsRead", metadata.getNumberOfRecordsRead());
            values.put("metadata", Row.withSchema(metadataSchema)
                    .withFieldValues(metadataValues)
                    .build());
        }

        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

}
