package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.firestore.v1.Document;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.sql.stmt.PreparedStatementTemplate;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

public class ToStatementConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ToStatementConverter.class);

    public static void convertRecord(final GenericRecord record, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertRecordWithKeys(record, statement, null);
    }

    public static void convertRecordWithKeys(
            final GenericRecord record, final PreparedStatementTemplate.PlaceholderSetterProxy statement,
            final List<String> keyFields) throws SQLException {

        int index = 1;
        for(final org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            final boolean isNull = record.get(field.name()) == null;
            final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.name())) {
                continue;
            }
            switch (fieldSchema.getType()) {
                case BOOLEAN -> {
                    if (isNull) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, ((Boolean) record.get(field.name())));
                    }
                }
                case FIXED, BYTES -> {
                    if (AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                        if (isNull) {
                            statement.setNull(index, Types.DECIMAL);
                        } else {
                            final byte[] bytes = ((ByteBuffer) record.get(field.name())).array();
                            statement.setBigDecimal(index, new BigDecimal(new BigInteger(1, bytes)));
                        }
                    } else {
                        if (isNull) {
                            statement.setNull(index, Types.BINARY);
                        } else {
                            final byte[] bytes = ((ByteBuffer) record.get(field.name())).array();
                            statement.setBytes(index, bytes);
                        }
                    }
                }
                case ENUM, STRING -> {
                    if (isNull) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, (record.get(field.name())).toString());
                    }
                }
                case INT -> {
                    final Integer i = (Integer) record.get(field.name());
                    if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                        if (isNull) {
                            statement.setNull(index, Types.DATE);
                        } else {
                            statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay(i)));
                        }
                    } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                        if (isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            statement.setTime(index, Time.valueOf(LocalTime.ofNanoOfDay(i * 1000 * 1000)));
                        }
                    } else {
                        if (isNull) {
                            statement.setNull(index, Types.INTEGER);
                        } else {
                            statement.setInt(index, i);
                        }
                    }
                }
                case LONG -> {
                    final Long i = (Long) record.get(field.name());
                    if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                        if (isNull) {
                            statement.setNull(index, Types.TIMESTAMP);
                        } else {
                            statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(i)));
                        }
                    } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                        if (isNull) {
                            statement.setNull(index, Types.TIMESTAMP);
                        } else {
                            statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(i / 1000)));
                        }
                    } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                        if (isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            statement.setTime(index, Time.valueOf(LocalTime.ofNanoOfDay(i * 1000)));
                        }
                    } else {
                        if (isNull) {
                            statement.setNull(index, Types.BIGINT);
                        } else {
                            statement.setLong(index, i);
                        }
                    }
                }
                case FLOAT -> {
                    if (isNull) {
                        statement.setNull(index, Types.REAL);
                    } else {
                        statement.setFloat(index, ((Float) record.get(field.name())));
                    }
                }
                case DOUBLE -> {
                    if (isNull) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, ((Double) record.get(field.name())));
                    }
                }
                default -> {
                }
            }
            index++;
        }
    }
    public static void convertRow(final Row row, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertRowWithKeys(row, statement, null);
    }

    public static void convertRowWithKeys(final Row row, final PreparedStatementTemplate.PlaceholderSetterProxy statement,
                                          final List<String> keyFields) throws SQLException {

        int index = 1;
        for(final Schema.Field field : row.getSchema().getFields()) {
            final boolean isNull = row.getValue(field.getName()) == null;
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getTypeName()) {
                case BOOLEAN -> {
                    if (isNull) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, row.getBoolean(field.getName()));
                    }
                }
                case INT16 -> {
                    if (isNull) {
                        statement.setNull(index, Types.SMALLINT);
                    } else {
                        statement.setShort(index, row.getInt16(field.getName()));
                    }
                }
                case INT32 -> {
                    if (isNull) {
                        statement.setNull(index, Types.INTEGER);
                    } else {
                        statement.setInt(index, row.getInt32(field.getName()));
                    }
                }
                case INT64 -> {
                    if (isNull) {
                        statement.setNull(index, Types.BIGINT);
                    } else {
                        statement.setLong(index, row.getInt64(field.getName()));
                    }
                }
                case FLOAT -> {
                    if (isNull) {
                        statement.setNull(index, Types.REAL);
                    } else {
                        statement.setFloat(index, row.getFloat(field.getName()));
                    }
                }
                case DOUBLE -> {
                    if (isNull) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, row.getDouble(field.getName()));
                    }
                }
                case BYTES -> {
                    if (isNull) {
                        statement.setNull(index, Types.BINARY);
                    } else {
                        statement.setBytes(index, row.getBytes(field.getName()));
                    }
                }
                case STRING -> {
                    if (isNull) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, row.getString(field.getName()));
                    }
                }
                case DATETIME -> {
                    if (isNull) {
                        statement.setNull(index, Types.TIMESTAMP);
                    } else {
                        statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(row.getDateTime(field.getName()).getMillis())));
                    }
                }
                case LOGICAL_TYPE -> {
                    if (RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                        if (isNull) {
                            statement.setNull(index, Types.DATE);
                        } else {
                            final LocalDate localDate = row.getLogicalTypeValue(field.getName(), LocalDate.class);
                            statement.setDate(index, Date.valueOf(localDate));
                        }
                    } else if (RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                        if (isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            org.joda.time.Instant instant = row.getLogicalTypeValue(field.getName(), org.joda.time.Instant.class);
                            statement.setTime(index, Time.valueOf(LocalTime.ofSecondOfDay(instant.toDateTime().getSecondOfDay())));
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                    }
                }
                default -> {
                }
            }
            index++;
        }
    }

    public static void convertStruct(final Struct struct, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertStructWithKeys(struct, statement, null);
    }

    public static void convertStructWithKeys(final Struct struct, final PreparedStatementTemplate.PlaceholderSetterProxy statement,
                                             final List<String> keyFields) throws SQLException {
        int index = 1;
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, struct.getBoolean(field.getName()));
                    }
                }
                case STRING -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, struct.getString(field.getName()));
                    }
                }
                case BYTES -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BINARY);
                    } else {
                        statement.setBytes(index, struct.getBytes(field.getName()).toByteArray());
                    }
                }
                case INT64 -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BIGINT);
                    } else {
                        statement.setLong(index, struct.getLong(field.getName()));
                    }
                }
                case FLOAT64 -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, struct.getDouble(field.getName()));
                    }
                }
                case DATE -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.DATE);
                    } else {
                        final com.google.cloud.Date date = struct.getDate(field.getName());
                        statement.setDate(index, Date.valueOf(LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth())));
                    }
                }
                case TIMESTAMP -> {
                    if (struct.isNull(field.getName())) {
                        statement.setNull(index, Types.TIMESTAMP);
                    } else {
                        statement.setTimestamp(index, struct.getTimestamp(field.getName()).toSqlTimestamp());
                    }
                }
                default -> {
                }
            }
            index++;
        }
    }

    public static void convertEntity(final Entity entity, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertEntityWithKeys(entity, statement, null);
    }

    public static void convertEntityWithKeys(final Entity entity, final PreparedStatementTemplate.PlaceholderSetterProxy statement,
                                             final List<String> keyFields) throws SQLException {
        int index = 1;
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(entry.getKey())) {
                continue;
            }
            switch (entry.getValue().getValueTypeCase()) {
                case BOOLEAN_VALUE -> {
                    statement.setBoolean(index, entry.getValue().getBooleanValue());
                }
                case STRING_VALUE -> {
                    statement.setString(index, entry.getValue().getStringValue());
                }
                case BLOB_VALUE -> {
                    statement.setBytes(index, entry.getValue().getBlobValue().toByteArray());
                }
                case INTEGER_VALUE -> {
                    statement.setLong(index, entry.getValue().getIntegerValue());
                }
                case DOUBLE_VALUE -> {
                    statement.setDouble(index, entry.getValue().getDoubleValue());
                }
                case TIMESTAMP_VALUE -> {
                    statement.setTimestamp(index, java.sql.Timestamp.valueOf(entry.getValue().getTimestampValue().toString()));
                }
                case NULL_VALUE -> {
                    statement.setNull(index, Types.NULL);
                }
                default -> {
                    throw new IllegalArgumentException("Not supported value: " + entry.getKey() + ", type: " + entry.getValue().getValueTypeCase().name());
                }
            }
            index++;
        }
    }

    public static void convertDocument(final Document document, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertDocumentWithKeys(document, statement, null);
    }

    public static void convertDocumentWithKeys(final Document document, final PreparedStatementTemplate.PlaceholderSetterProxy statement,
                                             final List<String> keyFields) throws SQLException {
        int index = 1;
        for(final Map.Entry<String, com.google.firestore.v1.Value> entry : document.getFieldsMap().entrySet()) {
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(entry.getKey())) {
                continue;
            }
            switch (entry.getValue().getValueTypeCase()) {
                case BOOLEAN_VALUE -> {
                    statement.setBoolean(index, entry.getValue().getBooleanValue());
                }
                case STRING_VALUE -> {
                    statement.setString(index, entry.getValue().getStringValue());
                }
                case BYTES_VALUE -> {
                    statement.setBytes(index, entry.getValue().getBytesValue().toByteArray());
                }
                case INTEGER_VALUE -> {
                    statement.setLong(index, entry.getValue().getIntegerValue());
                }
                case DOUBLE_VALUE -> {
                    statement.setDouble(index, entry.getValue().getDoubleValue());
                }
                case TIMESTAMP_VALUE -> {
                    statement.setTimestamp(index, java.sql.Timestamp.valueOf(entry.getValue().getTimestampValue().toString()));
                }
                case NULL_VALUE -> {
                    statement.setNull(index, Types.NULL);
                }
                default -> {
                    throw new IllegalArgumentException("Not supported value: " + entry.getKey() + ", type: " + entry.getValue().getValueTypeCase().name());
                }
            }
            index++;
        }
    }

    public static void convertElement(final MElement element, final PreparedStatementTemplate.PlaceholderSetterProxy statement) throws SQLException {
        convertElement(element, statement, null);
    }

    public static void convertElement(final MElement element, final PreparedStatementTemplate.PlaceholderSetterProxy statement, final List<String> keyFields) throws SQLException {
        switch (element.getType()) {
            case AVRO -> convertRecordWithKeys((GenericRecord) element.getValue(), statement, keyFields);
            case ROW -> convertRowWithKeys((Row) element.getValue(), statement, keyFields);
            case STRUCT -> convertStructWithKeys((Struct) element.getValue(), statement, keyFields);
            case ENTITY -> convertEntityWithKeys((Entity) element.getValue(), statement, keyFields);
            default -> throw new IllegalArgumentException();
        }
    }

}
