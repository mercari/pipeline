package com.mercari.solution.util.converter;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.compress.utils.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;


public class ResultSetToRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ResultSetToRecordConverter.class);

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);
    private static final java.sql.Timestamp SQL_TIMESTAMP_MIN = java.sql.Timestamp.from(Instant.ofEpochMilli(-62135596800000L));

    public static Schema convertSchema(final ResultSetMetaData meta)  throws SQLException {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        final int columnCount = meta.getColumnCount();
        for (int column = 1; column <= columnCount; ++column) {
            final String fieldName = meta.getColumnName(column);
            final int fieldType = meta.getColumnType(column);
            String fieldTypeName = meta.getColumnTypeName(column);
            if(fieldTypeName.startsWith("_")) {
                fieldTypeName = fieldTypeName.replaceFirst("_", "");
            }
            schemaFields
                    .name(fieldName)
                    .prop("tableName", meta.getTableName(column))
                    .prop("columnTypeName", meta.getColumnTypeName(column))
                    .prop("isCaseSensitive", Boolean.toString(meta.isCaseSensitive(column)))
                    .type(convertFieldSchema(fieldType, fieldTypeName.toUpperCase()))
                    .noDefault();
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord convert(final ResultSet resultSet) throws SQLException, IOException {
        final Schema schema = convertSchema(resultSet.getMetaData());
        return convert(schema, resultSet);
    }

    public static GenericRecord convert(final Schema schema, final ResultSet resultSet) throws SQLException, IOException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int columnCount = meta.getColumnCount();
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (int column = 1; column <= columnCount; ++column) {
            String fieldName = meta.getColumnName(column);
            if(schema.getField(fieldName) == null && schema.getField(fieldName.toLowerCase()) == null) {
                continue;
            }
            if(schema.getField(fieldName) == null) {
                fieldName = fieldName.toLowerCase();
            }
            final Object fieldValue = convertFieldValue(resultSet, column, meta.getColumnType(column));
            builder.set(fieldName, fieldValue);
        }
        return builder.build();
    }

    private static Schema convertFieldSchema(final int type, final String typeName) {
        switch (type) {
            case Types.BIT:
            case Types.BOOLEAN:
                return AvroSchemaUtil.NULLABLE_BOOLEAN;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return Schema
                        .createUnion(
                                LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)),
                                Schema.create(Schema.Type.NULL));
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return AvroSchemaUtil.NULLABLE_INT;
            case Types.BIGINT:
                return AvroSchemaUtil.NULLABLE_LONG;
            case Types.REAL:
                return AvroSchemaUtil.NULLABLE_FLOAT;
            case Types.FLOAT:
            case Types.DOUBLE:
                return AvroSchemaUtil.NULLABLE_DOUBLE;
            case Types.ROWID:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR: {
                if("json".equalsIgnoreCase(typeName)) {
                    return AvroSchemaUtil.NULLABLE_JSON;
                } else if("uuid".equalsIgnoreCase(typeName)) {
                    return AvroSchemaUtil.NULLABLE_LOGICAL_UUID_TYPE;
                }
                return AvroSchemaUtil.NULLABLE_STRING;
            }
            case Types.TIME:
                return AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE;
            case Types.DATE:
                return AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE;
            case Types.TIMESTAMP:
                return AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case Types.JAVA_OBJECT:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return AvroSchemaUtil.NULLABLE_BYTES;
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return AvroSchemaUtil.NULLABLE_SQL_DATETIME_TYPE;
            case Types.OTHER: {
                if("json".equalsIgnoreCase(typeName)) {
                    return AvroSchemaUtil.NULLABLE_JSON;
                }
                LOG.warn("Type Other, TypeName: " + typeName + " as NULLABLE STRING");
                return AvroSchemaUtil.NULLABLE_STRING;
            }
            case Types.ARRAY: {
                switch (typeName) {
                    case "BLOB":
                    case "BINARY":
                    case "VARBINARY":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.BYTES)),
                                Schema.create(Schema.Type.NULL));
                    case "TINYINT":
                    case "SMALLINT":
                    case "INTEGER":
                    case "INT2":
                    case "INT4":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.INT)),
                                Schema.create(Schema.Type.NULL));
                    case "BIGINT":
                    case "INT8":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.LONG)),
                                Schema.create(Schema.Type.NULL));
                    case "REAL":
                    case "FLOAT4":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.FLOAT)),
                                Schema.create(Schema.Type.NULL));
                    case "FLOAT":
                    case "DOUBLE":
                    case "FLOAT8":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                                Schema.create(Schema.Type.NULL));
                    case "NUMERIC":
                    case "DECIMAL":
                    case "BIGDECIMAL":
                        return Schema.createUnion(
                                Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE),
                                Schema.create(Schema.Type.NULL));
                    case "TEXT":
                    case "CHAR":
                    case "MCHAR":
                    case "NCHAR":
                    case "VARCHAR":
                    case "MVARCHAR":
                    case "NVARCHAR":
                    case "CLOB":
                    case "BPCHAR":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.STRING)),
                                Schema.create(Schema.Type.NULL));
                    case "DATE":
                        return Schema.createUnion(
                                Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE),
                                Schema.create(Schema.Type.NULL));
                    case "TIME":
                        return Schema.createUnion(
                                Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE),
                                Schema.create(Schema.Type.NULL));
                    case "TIMESTAMP":
                        return Schema.createUnion(
                                Schema.createArray(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE),
                                Schema.create(Schema.Type.NULL));
                    case "BIT":
                    case "BOOLEAN":
                        return Schema.createUnion(
                                Schema.createArray(Schema.create(Schema.Type.BOOLEAN)),
                                Schema.create(Schema.Type.NULL));
                    case "JSON":
                        return AvroSchemaUtil.NULLABLE_ARRAY_JSON_TYPE;
                    default:
                        throw new IllegalStateException("Not supported ArrayElementType: " + typeName);
                }
            }
            case Types.STRUCT:
            case Types.REF:
            case Types.SQLXML:
            case Types.REF_CURSOR:
            case Types.DISTINCT:
            case Types.DATALINK:
            case Types.NULL:
            default:
                return AvroSchemaUtil.NULLABLE_STRING;
        }
    }

    private static Object convertFieldValue(
            final ResultSet resultSet,
            final int column,
            final int columnType) throws SQLException, IOException {

        switch (columnType) {
            case Types.BIT:
            case Types.BOOLEAN: {
                final Boolean value = resultSet.getBoolean(column);
                if(resultSet.wasNull()) {
                    return null;
                }
                return value;
            }
            case Types.NUMERIC:
            case Types.DECIMAL: {
                final BigDecimal decimal = resultSet.getBigDecimal(column);
                if(resultSet.wasNull() || decimal == null) {
                    return null;
                } else if(decimal.scale() > 9) {
                    final BigDecimal newDecimal = decimal
                            .setScale(9, RoundingMode.HALF_UP)
                            .scaleByPowerOfTen(9);
                    return ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray());
                } else {
                    final BigDecimal newDecimal = decimal.scaleByPowerOfTen(9);
                    return ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray());
                }
            }
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER: {
                final Integer value = resultSet.getInt(column);
                if (resultSet.wasNull()) {
                    return null;
                }
                return value;
            }
            case Types.BIGINT: {
                final Long value = resultSet.getLong(column);
                if(resultSet.wasNull()) {
                    return null;
                }
                return value;
            }
            case Types.REAL: {
                final Float value = resultSet.getFloat(column);
                if(resultSet.wasNull()) {
                    return null;
                }
                return value;
            }
            case Types.FLOAT:
            case Types.DOUBLE: {
                final Double value = resultSet.getDouble(column);
                if(resultSet.wasNull()) {
                    return null;
                }
                return value;
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.OTHER: {
                return resultSet.getString(column);
            }
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
                return resultSet.getNString(column);
            case Types.ROWID: {
                final RowId rowId = resultSet.getRowId(column);
                if (rowId == null) {
                    return null;
                } else {
                    return rowId.toString();
                }
            }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                byte[] binary = resultSet.getBytes(column);
                if(resultSet.wasNull() || binary == null) {
                    return null;
                } else {
                    return ByteBuffer.wrap(binary);
                }
            }
            case Types.BLOB: {
                final Blob blob = resultSet.getBlob(column);
                if(resultSet.wasNull() || blob == null) {
                    return null;
                } else {
                    byte[] bytes = IOUtils.toByteArray(blob.getBinaryStream());
                    return ByteBuffer.wrap(bytes);
                }
            }
            case Types.CLOB:
            case Types.NCLOB: {
                final Clob clob = resultSet.getClob(column);
                if(resultSet.wasNull() || clob == null) {
                    return null;
                } else {
                    return clob.getSubString(1, (int)clob.length());
                }
            }
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE: {
                final Time time = resultSet.getTime(column);
                if (resultSet.wasNull() || time == null) {
                    return null;
                } else {
                    return DateTimeUtil.toMicroOfDay(time.toLocalTime());
                }
            }
            case Types.DATE: {
                final java.sql.Date sqlDate = resultSet.getDate(column);
                if (resultSet.wasNull() || sqlDate == null) {
                    return null;
                } else {
                    final LocalDate localDate = sqlDate.toLocalDate();
                    final DateTime datetime = new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    return days.getDays();
                }
            }
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE: {
                final java.sql.Timestamp timestamp = resultSet.getTimestamp(column);
                if (resultSet.wasNull() || timestamp == null) {
                    return null;
                } else if (timestamp.before(SQL_TIMESTAMP_MIN)) {
                    return SQL_TIMESTAMP_MIN.getTime() * 1000;
                } else {
                    return timestamp.getTime() * 1000;
                }
            }
            case Types.ARRAY: {
                final Array array = resultSet.getArray(column);
                if(resultSet.wasNull() || array.getArray() == null) {
                    return null;
                }
                final List<Object> list = new ArrayList<>();
                switch (array.getBaseType()) {
                    case Types.BIT:
                    case Types.BOOLEAN: {
                        for (Boolean v : (Boolean[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER: {
                        for (Integer v : (Integer[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.BIGINT: {
                        for (Long v : (Long[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.REAL: {
                        for (Float v : (Float[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.FLOAT:
                    case Types.DOUBLE:{
                        for(Double v : (Double[]) array.getArray()) {
                            if(v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.OTHER: {
                        for(String v : (String[]) array.getArray()) {
                            if(v == null) {
                                continue;
                            }
                            list.add(v);
                        }
                        break;
                    }
                    case Types.ROWID: {
                        for(RowId rowId : (RowId[]) array.getArray()) {
                            if(rowId == null) {
                                continue;
                            }
                            list.add(rowId.toString());
                        }
                        break;
                    }
                    case Types.NUMERIC:
                    case Types.DECIMAL: {
                        for (final BigDecimal decimal : (BigDecimal[]) array.getArray()) {
                            if (decimal == null) {
                                continue;
                            }
                            final BigDecimal newDecimal;
                            if(decimal.scale() > 9) {
                                newDecimal = decimal
                                        .setScale(9, RoundingMode.HALF_UP)
                                        .scaleByPowerOfTen(9);
                            } else {
                                newDecimal = decimal.scaleByPowerOfTen(9);
                            }
                            list.add(ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray()));
                        }
                        break;
                    }
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY: {
                        for (final byte[] v : (byte[][]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(ByteBuffer.wrap(v));
                        }
                        break;
                    }
                    case Types.BLOB: {
                        for (final Blob v : (Blob[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(ByteBuffer.wrap(IOUtils.toByteArray(v.getBinaryStream())));
                        }
                        break;
                    }
                    case Types.CLOB:
                    case Types.NCLOB: {
                        for (final Clob v : (Clob[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(v.getSubString(1, (int)v.length()));
                        }
                        break;
                    }
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE: {
                        for (final Time v : (Time[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(DateTimeUtil.toMicroOfDay(v.toLocalTime()));
                        }
                        break;
                    }
                    case Types.DATE: {
                        for (final java.sql.Date v : (java.sql.Date[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            list.add(DateTimeUtil.toEpochDay(v));
                        }
                        break;
                    }
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE: {
                        for (final java.sql.Timestamp v : (java.sql.Timestamp[]) array.getArray()) {
                            if (v == null) {
                                continue;
                            }
                            if(v.before(SQL_TIMESTAMP_MIN)) {
                                list.add(SQL_TIMESTAMP_MIN.getTime() * 1000);
                            } else {
                                list.add(v.getTime() * 1000);
                            }
                        }
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Not supported ArrayType: " + array.getBaseType());
                    }
                }
                return list;
            }
            case Types.JAVA_OBJECT: {
                final Object object = resultSet.getObject(column);
                return null;
            }
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.SQLXML:
            case Types.REF_CURSOR:
            default: {
                return null;
            }
        }
    }

}
