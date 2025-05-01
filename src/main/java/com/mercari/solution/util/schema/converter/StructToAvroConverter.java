package com.mercari.solution.util.schema.converter;

/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.text.JsonUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to Avro GenericRecord
 */
public class StructToAvroConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StructToAvroConverter.class);

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    private StructToAvroConverter() {}

    public static GenericRecord convert(final AvroWriteRequest<Struct> request) {
        return convert(request.getSchema(), request.getElement());
    }

    /**
     * Convert Spanner {@link Struct} object to Avro {@link GenericRecord} object.
     *
     * @param schema Avro schema object.
     * @param struct Spanner Struct to be converted to GenericRecord object.
     * @return Avro GenericRecord object.
     */
    public static GenericRecord convert(final Schema schema, Struct struct) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(builder, field, struct);
        }
        return builder.build();
    }

    public static GenericRecord convert(KV<String, Struct> kv, Schema schema) {
        return convert(schema, kv.getValue());
    }

    public static Schema convertSchema(final Type type) {
        return convertSchema(type, "root");
    }

    public static Schema convertSchema(final Type type, final String recordName) {
        try {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(recordName).fields();
            type.getStructFields().forEach(field -> schemaFields
                    .name(field.getName())
                    .type(convertFieldType(field.getType(), field.getName(), true))
                    .noDefault());
            return schemaFields.endRecord();
        } catch (RuntimeException e) {
            final String message = "Failed to convert schema from type: " + type;
            LOG.error(message);
            throw new IllegalStateException(message, e);
        }
    }

    public static GenericRecord convert(final Schema schema, final DataChangeRecord record) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Mod mod : record.getMods()) {
            final JsonObject keyJson = JsonUtil.fromJson(mod.getKeysJson()).getAsJsonObject();
            for(final Map.Entry<String, JsonElement> entry : keyJson.entrySet()) {
                final Schema.Field field = schema.getField(entry.getKey());
                builder.set(field.name(), JsonToAvroConverter.convertValue(field.schema(), entry.getValue()));
            }
            final JsonObject valuesJson = JsonUtil.fromJson(mod.getNewValuesJson()).getAsJsonObject();
            for(final Map.Entry<String, JsonElement> entry : valuesJson.entrySet()) {
                final Schema.Field field = schema.getField(entry.getKey());
                builder.set(field.name(), JsonToAvroConverter.convertValue(field.schema(), entry.getValue()));
            }
        }
        return builder.build();
    }

    public static GenericRecord convertToDataChangeRecord(final Schema schema, final DataChangeRecord record) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("partitionToken", record.getPartitionToken());
        builder.set("commitTimestamp", DateTimeUtil.toEpochMicroSecond(record.getCommitTimestamp()));
        builder.set("serverTransactionId", record.getServerTransactionId());
        builder.set("isLastRecordInTransactionInPartition", record.isLastRecordInTransactionInPartition());
        builder.set("recordSequence", record.getRecordSequence());
        builder.set("tableName", record.getTableName());

        final Schema rowTypeSchema = schema.getField("rowType").schema().getElementType();
        final List<GenericRecord> rowTypes = new ArrayList<>();
        for(final ColumnType columnType : record.getRowType()) {
            final GenericRecordBuilder rowTypeValues = new GenericRecordBuilder(rowTypeSchema);
            final String type = columnType.getType().getCode()
                    .replaceAll("\\{\"code\":","")
                    .replaceAll("}", "")
                    .replaceAll("\"", "");
            final GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(rowTypeSchema.getField("Type").schema(), type);
            rowTypeValues.set("name", columnType.getName());
            rowTypeValues.set("Type", symbol);
            rowTypeValues.set("isPrimaryKey", columnType.isPrimaryKey());
            rowTypeValues.set("ordinalPosition", columnType.getOrdinalPosition());
            rowTypes.add(rowTypeValues.build());
        }
        builder.set("rowType", rowTypes);

        final Schema modSchema = schema.getField("mods").schema().getElementType();
        final List<GenericRecord> mods = new ArrayList<>();
        for(final Mod mod : record.getMods()) {
            final GenericRecordBuilder modValues = new GenericRecordBuilder(modSchema);
            modValues.set("keysJson", mod.getKeysJson());
            modValues.set("oldValuesJson", mod.getOldValuesJson());
            modValues.set("newValuesJson", mod.getNewValuesJson());
            mods.add(modValues.build());
        }
        builder.set("mods", mods);

        final String modType = record.getModType().name();
        final String valueCaptureType = record.getValueCaptureType().name();
        final GenericData.EnumSymbol modTypeSymbol = new GenericData.EnumSymbol(schema.getField("modType").schema(), modType);
        final GenericData.EnumSymbol valueCaptureTypeSymbol = new GenericData.EnumSymbol(schema.getField("valueCaptureType").schema(), valueCaptureType);

        builder.set("modType", modTypeSymbol);
        builder.set("valueCaptureType", valueCaptureTypeSymbol);
        builder.set("numberOfRecordsInTransaction", record.getNumberOfRecordsInTransaction());
        builder.set("numberOfPartitionsInTransaction", record.getNumberOfPartitionsInTransaction());

        final ChangeStreamRecordMetadata metadata = record.getMetadata();
        if(metadata == null) {
            builder.set("metadata", null);
        } else {
            final Schema metadataSchema = AvroSchemaUtil.unnestUnion(schema.getField("metadata").schema());
            final GenericRecordBuilder metadataValues = new GenericRecordBuilder(metadataSchema);
            metadataValues.set("partitionToken", metadata.getPartitionToken());
            metadataValues.set("recordTimestamp", DateTimeUtil.toEpochMicroSecond(metadata.getRecordTimestamp()));
            metadataValues.set("partitionStartTimestamp", DateTimeUtil.toEpochMicroSecond(metadata.getPartitionStartTimestamp()));
            metadataValues.set("partitionEndTimestamp", DateTimeUtil.toEpochMicroSecond(metadata.getPartitionEndTimestamp()));
            metadataValues.set("partitionCreatedAt", DateTimeUtil.toEpochMicroSecond(metadata.getPartitionCreatedAt()));
            metadataValues.set("partitionScheduledAt", DateTimeUtil.toEpochMicroSecond(metadata.getPartitionScheduledAt()));
            metadataValues.set("partitionRunningAt", DateTimeUtil.toEpochMicroSecond(metadata.getPartitionRunningAt()));
            metadataValues.set("queryStartedAt", DateTimeUtil.toEpochMicroSecond(metadata.getQueryStartedAt()));
            metadataValues.set("recordStreamStartedAt", DateTimeUtil.toEpochMicroSecond(metadata.getRecordStreamStartedAt()));
            metadataValues.set("recordStreamEndedAt", DateTimeUtil.toEpochMicroSecond(metadata.getRecordStreamEndedAt()));
            metadataValues.set("recordReadAt", DateTimeUtil.toEpochMicroSecond(metadata.getRecordReadAt()));
            metadataValues.set("totalStreamTimeMillis", metadata.getTotalStreamTimeMillis());
            metadataValues.set("numberOfRecordsRead", metadata.getNumberOfRecordsRead());
            builder.set("metadata", metadataValues.build());
        }

        return builder.build();
    }

    //
    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Struct struct) {
        setFieldValue(builder, field.name(), field.schema(), struct);
    }

    private static void setFieldValue(final GenericRecordBuilder builder,
                                      final String fieldName,
                                      final Schema schema,
                                      final Struct struct) {

        final Type type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(Type.StructField::getType)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Missing field %s", fieldName)));

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }

        switch (schema.getType()) {
            case BOOLEAN -> builder.set(fieldName, struct.getBoolean(fieldName));
            case FLOAT -> builder.set(fieldName, struct.getFloat(fieldName));
            case DOUBLE -> builder.set(fieldName, struct.getDouble(fieldName));
            case ENUM, STRING -> builder.set(fieldName, struct.getString(fieldName));
            case FIXED, BYTES -> builder.set(fieldName, struct.getBytes(fieldName).asReadOnlyByteBuffer());
            case INT -> {
                if (Type.date().equals(type)) {
                    final Date date = struct.getDate(fieldName);
                    final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    builder.set(fieldName, days.getDays());
                } else {
                    builder.set(fieldName, struct.getLong(fieldName));
                }
            }
            case LONG -> {
                if (Type.timestamp().equals(type)) {
                    builder.set(fieldName, Timestamps.toMicros(struct.getTimestamp(fieldName).toProto()));
                } else {
                    builder.set(fieldName, struct.getLong(fieldName));
                }
            }
            case RECORD -> {
                final GenericRecord chileRecord = convert(schema, struct.getStruct(fieldName));
                builder.set(fieldName, chileRecord);
            }
            case UNION -> {
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setFieldValue(builder, fieldName, childSchema, struct);
                }
            }
            case ARRAY -> setArrayFieldValue(builder, fieldName, schema.getElementType(), struct);
            case NULL -> builder.set(fieldName, null);
        }
    }

    private static void setArrayFieldValue(final GenericRecordBuilder builder,
                                           final String fieldName,
                                           final Schema schema,
                                           final Struct struct) {

        final Type type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(Type.StructField::getType)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Missing field %s", fieldName)));

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }
        switch (schema.getType()) {
            case BOOLEAN -> builder.set(fieldName, struct.getBooleanList(fieldName));
            case FLOAT -> builder.set(fieldName, struct.getFloatList(fieldName));
            case DOUBLE -> builder.set(fieldName, struct.getDoubleList(fieldName));
            case ENUM, STRING -> builder.set(fieldName, struct.getStringList(fieldName));
            case FIXED, BYTES -> builder.set(fieldName, struct.getBytesList(fieldName)
                        .stream()
                        .map(ByteArray::asReadOnlyByteBuffer)
                        .collect(Collectors.toList()));
            case INT -> {
                if (Type.array(Type.date()).equals(type)) {
                    builder.set(fieldName, struct.getDateList(fieldName).stream()
                            .map(date -> new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC))
                            .map(datetime -> Days.daysBetween(EPOCH_DATETIME, datetime).getDays())
                            .collect(Collectors.toList()));
                } else {
                    builder.set(fieldName, struct.getLongList(fieldName));
                }
            }
            case LONG -> {
                if (Type.array(Type.timestamp()).equals(type)) {
                    builder.set(fieldName, struct.getTimestampList(fieldName).stream()
                            .map(Timestamp::toProto)
                            .map(Timestamps::toMicros)
                            .collect(Collectors.toList()));
                } else {
                    builder.set(fieldName, struct.getLongList(fieldName));
                }
            }
            case RECORD -> builder.set(fieldName, struct.getStructList(fieldName).stream()
                        .map(childStruct -> convert(schema, childStruct))
                        .collect(Collectors.toList()));
            case UNION -> {
                for (final Schema childSchema : schema.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setArrayFieldValue(builder, fieldName, childSchema, struct);
                }
            }
            case ARRAY -> setArrayFieldValue(builder, fieldName, schema.getElementType(), struct);
            case NULL -> builder.set(fieldName, null);
        }
    }

    private static Schema convertFieldType(final Type type, final String name, final boolean nullable) {
        switch (type.getCode()) {
            case BYTES:
                return nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
            case STRING:
                return nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
            case JSON: {
                final Schema jsonSchema = Schema.create(Schema.Type.STRING);
                jsonSchema.addProp("sqlType", "JSON");
                return nullable ? Schema.createUnion(jsonSchema, Schema.create(Schema.Type.NULL)) : jsonSchema;
            }
            case INT64:
                return nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
            case FLOAT64:
                return nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
            case NUMERIC:
            case PG_NUMERIC:
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE;
            case BOOL:
                return nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
            case DATE:
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
            case TIMESTAMP:
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case STRUCT:
                final Schema structSchema = convertSchema(type, name);
                return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), structSchema) : structSchema;
            case ARRAY:
                final Schema arraySchema = Schema.createArray(convertFieldType(type.getArrayElementType(), name, false));
                //return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema) : arraySchema;
                return arraySchema;
            default:
                throw new IllegalArgumentException("Spanner type: " + type.toString() + " not supported!");
        }
    }

}