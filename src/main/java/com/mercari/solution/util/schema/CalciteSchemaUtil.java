package com.mercari.solution.util.schema;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.linq4j.function.Function1;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class CalciteSchemaUtil {

    public static RelDataType convertSchema(final Schema schema, final RelDataTypeFactory relDataTypeFactory) {
        final List<RelDataTypeField> fields = new ArrayList<>(schema.countFields());
        for (int i = 0; i < schema.countFields(); i++) {
            final Schema.Field schemaField = schema.getField(i);
            final RelDataType fieldType = createRelDataType(schemaField.getFieldType(), relDataTypeFactory);
            final RelDataTypeField field = new RelDataTypeFieldImpl(schemaField.getName(), i, fieldType);
            fields.add(field);
        }
        return new RelRecordType(StructKind.PEEK_FIELDS, fields, true);
    }

    public static Schema convertSchema(final ResultSetMetaData metaData) {
        try {
            final Schema.Builder builder = Schema.builder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                final String fieldName = metaData.getColumnName(i);
                final Schema.FieldType fieldType = convertFieldType(metaData.getColumnType(i), metaData.getColumnTypeName(i));
                builder.withField(fieldName, fieldType);
            }
            return builder.build();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    public static Schema convertSchema(final RelDataType relDataType) {
        for(final RelDataTypeField field : relDataType.getFieldList()) {
        }
        return Schema.builder().withField("", Schema.FieldType.FLOAT64).build();
    }
     */

    public static List<Map<String, Object>> convert(final ResultSet resultSet) {
        try {
            final List<Map<String, Object>> results = new ArrayList<>();
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                final Map<String, Object> primitiveValues = new HashMap<>();
                for(int i=1; i<=metaData.getColumnCount(); i++) {
                    final String columnName = metaData.getColumnName(i);
                    final int columnType = metaData.getColumnType(i);
                    final String columnTypeName = metaData.getColumnTypeName(i);
                    final Object object = resultSet.getObject(i);
                    final Object columnValue = convertPrimitiveValue(columnType, columnTypeName, object);
                    primitiveValues.put(columnName, columnValue);
                }
                results.add(primitiveValues);
            }
            return results;
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to convert resultSet", e);
        }
    }

    public static Object convertSqlValue(final Schema.FieldType fieldType, final Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool, string, json, int32, int64, float32, float64 -> primitiveValue;
            case date -> Date.valueOf(LocalDate.ofEpochDay((Integer)primitiveValue));
            case time -> Time.valueOf(LocalTime.ofNanoOfDay((Long) primitiveValue * 1000L));
            case timestamp -> Timestamp.valueOf(DateTimeUtil.toLocalDateTime((Long) primitiveValue));
            case array -> {
                final List<Object> values = new ArrayList<>();
                for(final Object v : (Iterable<?>) primitiveValue) {
                    final Object value = convertSqlValue(fieldType.getArrayValueType(), v);
                    values.add(value);
                }
                yield values;
            }
            case element -> switch (primitiveValue) {
                case Map<?,?> map -> {
                    final Map<String, Object> values = new HashMap<>();
                    for(final Schema.Field field : fieldType.getElementSchema().getFields()) {
                        final Object fieldValue = map.get(field.getName());
                        final Object value = convertSqlValue(field.getFieldType(), fieldValue);
                        values.put(field.getName(), value);
                    }
                    yield MElement.builder().withPrimitiveValues(values).build();
                }
                case MElement e -> e;
                default -> throw new IllegalArgumentException("Not supported struct class: " + primitiveValue.getClass());
            };
            default -> throw new IllegalArgumentException();
        };
    }

    private static Object convertPrimitiveValue(final int type, final String typeName, final Object sqlValue) {
        if(sqlValue == null) {
            return null;
        }
        return switch (type) {
            case Types.BIT, Types.BOOLEAN -> sqlValue;
            case Types.DECIMAL, Types.NUMERIC -> sqlValue;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> sqlValue;
            case Types.BIGINT -> sqlValue;
            case Types.REAL -> sqlValue;
            case Types.FLOAT, Types.DOUBLE -> sqlValue;
            case Types.ROWID, Types.CLOB, Types.NCLOB,
                 Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR,
                 Types.LONGVARCHAR, Types.LONGNVARCHAR -> {
                yield sqlValue;
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> DateTimeUtil.toEpochMicroSecond(((Time) sqlValue).toInstant());
            case Types.DATE -> Long.valueOf(((Date) sqlValue).toLocalDate().toEpochDay()).intValue();
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> DateTimeUtil.toEpochMicroSecond(((Timestamp) sqlValue).toInstant());
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> {
                final Blob blob = (Blob) sqlValue;
                try {
                    yield ByteBuffer.wrap(blob.getBytes(0, Long.valueOf(blob.length()).intValue()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            case Types.ARRAY -> {
                final String innerTypeName = typeName.replace("ARRAY", "").trim();
                yield switch (innerTypeName) {
                    case "BLOB", "BINARY", "VARBINARY" -> sqlValue;
                    case "TINYINT", "SMALLINT", "INTEGER", "INT2", "INT4" -> sqlValue;
                    case "BIGINT", "INT8" -> sqlValue;
                    case "REAL", "FLOAT4" -> sqlValue;
                    case "FLOAT", "DOUBLE", "FLOAT8" -> sqlValue;
                    case "NUMERIC", "DECIMAL", "BIGDECIMAL" -> sqlValue;
                    case "TEXT", "CHAR", "MCHAR", "NCHAR",
                         "VARCHAR", "MVARCHAR", "NVARCHAR", "CLOB", "BPCHAR", "JSON" -> sqlValue;
                    case "DATE" -> {
                        List<Integer> list = new ArrayList<>();
                        Date[] dates = (Date[]) sqlValue;
                        for(Date date : dates) {
                            list.add(DateTimeUtil.toEpochDay(date));
                        }
                        yield list;
                    }
                    case "TIME" -> Schema.FieldType.TIME;
                    case "TIMESTAMP" -> Schema.FieldType.TIMESTAMP;
                    case "BIT", "BOOLEAN" -> sqlValue;
                    default -> throw new IllegalStateException("Not supported ArrayElementType: " + typeName);
                };
            }
            case Types.OTHER -> switch (sqlValue) {
                case Object[] ds -> Arrays.asList(ds);
                default -> sqlValue.toString();
            };
            case Types.STRUCT, Types.JAVA_OBJECT -> {
                yield sqlValue;
            }
            case Types.REF, Types.SQLXML,
                 Types.REF_CURSOR, Types.DISTINCT, Types.DATALINK, Types.NULL -> Schema.FieldType.STRING;
            default -> sqlValue;
        };
    }

    public static Object convertPrimitiveValue(final Object object) {
        return switch (object) {
            case Boolean b -> b;
            case String s -> s;
            case Number n -> n;
            case null, default -> null;
        };
    }

    public static FieldSelector createFieldSelector(final Schema schema) {
        return new FieldSelector(schema);
    }

    private static RelDataType createRelDataType(
            final Schema.FieldType fieldType,
            final RelDataTypeFactory relDataTypeFactory) {
        final RelDataType relDataType = switch (fieldType.getType()) {
            case map -> relDataTypeFactory.createMapType(
                        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                        relDataTypeFactory.createSqlType(convertSqlTypeName(fieldType.getMapValueType())));
            case array, matrix -> relDataTypeFactory.createArrayType(
                        createRelDataType(fieldType.getArrayValueType(), relDataTypeFactory), -1L);
            case element -> convertSchema(fieldType.getElementSchema(), relDataTypeFactory);
            default -> relDataTypeFactory.createSqlType(convertSqlTypeName(fieldType));
        };
        if(fieldType.getNullable()) {
            return relDataTypeFactory.createTypeWithNullability(relDataType, true);
        } else {
            // TODO
            return relDataTypeFactory.createTypeWithNullability(relDataType, true);
        }
    }

    private static SqlTypeName convertSqlTypeName(final Schema.FieldType fieldType) {
        return switch (fieldType.getType()) {
            case bool -> SqlTypeName.BOOLEAN;
            case string, json, enumeration -> SqlTypeName.VARCHAR;
            case bytes -> SqlTypeName.BINARY;
            case int16, int8 -> SqlTypeName.SMALLINT;
            case int32 -> SqlTypeName.INTEGER;
            case int64 -> SqlTypeName.BIGINT;
            case float32 -> SqlTypeName.FLOAT;
            case float64 -> SqlTypeName.DOUBLE;
            case decimal -> SqlTypeName.DECIMAL;
            case date -> SqlTypeName.DATE;
            case time -> SqlTypeName.TIME;
            case timestamp, datetime -> SqlTypeName.TIMESTAMP;
            case array, matrix -> SqlTypeName.ARRAY;
            case map -> SqlTypeName.MAP;
            case element -> SqlTypeName.ROW;
            default -> throw new IllegalArgumentException("Not supported calcite data type: " + fieldType.getType());
        };
    }

    private static Schema.FieldType convertFieldType(final int type, final String typeName) {
        return switch (type) {
            case Types.BIT, Types.BOOLEAN -> Schema.FieldType.BOOLEAN;
            case Types.DECIMAL, Types.NUMERIC -> Schema.FieldType.DECIMAL;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Schema.FieldType.INT32;
            case Types.BIGINT -> Schema.FieldType.INT64;
            case Types.REAL -> Schema.FieldType.FLOAT32;
            case Types.FLOAT, Types.DOUBLE -> Schema.FieldType.FLOAT64;
            case Types.ROWID, Types.CLOB, Types.NCLOB,
                 Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR,
                 Types.LONGVARCHAR, Types.LONGNVARCHAR -> {
                if("json".equalsIgnoreCase(typeName)) {
                    yield Schema.FieldType.JSON;
                }
                yield Schema.FieldType.STRING;
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> Schema.FieldType.TIME;
            case Types.DATE -> Schema.FieldType.DATE;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Schema.FieldType.TIMESTAMP;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> Schema.FieldType.BYTES;
            case Types.ARRAY -> {
                final String innerTypeName = typeName.replace("ARRAY","").trim();
                final Schema.FieldType arrayFieldType = switch (innerTypeName) {
                    case "BLOB", "BINARY", "VARBINARY" -> Schema.FieldType.BYTES;
                    case "TINYINT", "SMALLINT", "INTEGER", "INT2", "INT4" -> Schema.FieldType.INT32;
                    case "BIGINT", "INT8" -> Schema.FieldType.INT64;
                    case "REAL", "FLOAT4" -> Schema.FieldType.FLOAT32;
                    case "FLOAT", "DOUBLE", "FLOAT8" -> Schema.FieldType.FLOAT64;
                    case "NUMERIC", "DECIMAL", "BIGDECIMAL" -> Schema.FieldType.DECIMAL;
                    case "TEXT", "CHAR", "MCHAR", "NCHAR",
                         "VARCHAR", "MVARCHAR", "NVARCHAR", "CLOB", "BPCHAR" -> Schema.FieldType.STRING;
                    case "DATE" -> Schema.FieldType.DATE;
                    case "TIME" -> Schema.FieldType.TIME;
                    case "TIMESTAMP" -> Schema.FieldType.TIMESTAMP;
                    case "BIT", "BOOLEAN" -> Schema.FieldType.BOOLEAN;
                    case "JSON" -> Schema.FieldType.JSON;
                    default -> throw new IllegalStateException("Not supported ArrayElementType: " + typeName.replace("ARRAY","").trim());
                };
                yield Schema.FieldType.array(arrayFieldType);
            }
            case Types.STRUCT, Types.JAVA_OBJECT -> {
                yield Schema.FieldType.STRING;
            }
            case Types.REF, Types.SQLXML, Types.OTHER,
                 Types.REF_CURSOR, Types.DISTINCT, Types.DATALINK, Types.NULL -> {
                yield Schema.FieldType.STRING;
            }
            default -> Schema.FieldType.STRING;
        };
    }

    public static class FieldSelector implements Function1<MElement, @Nullable Object[]> {

        private final Schema schema;

        private FieldSelector(final Schema schema) {
            this.schema = schema;
        }

        @Override
        public Object[] apply(final MElement element) {
            return apply(this.schema, element);
        }

        private static Object[] apply(final Schema schema, final MElement element) {
            final Object[] fieldValues = new Object[schema.countFields()];
            if(element == null) {
                for(int i=0; i<schema.countFields(); i++) {
                    fieldValues[i] = null;
                }
                return fieldValues;
            }
            for(int i=0; i<schema.countFields(); i++) {
                final Schema.Field field = schema.getField(i);
                final Object v = element.getPrimitiveValue(field.getName());
                final Object convertedValue = CalciteSchemaUtil.convertSqlValue(field.getFieldType(), v);
                fieldValues[i] = switch (convertedValue) {
                    case MElement e -> apply(field.getFieldType().getElementSchema(), e);
                    case Iterable<?> l -> switch (field.getFieldType().getArrayValueType().getType()) {
                        case element -> {
                            final List oo = new ArrayList<>();
                            for(final Object d : l) {
                                var o = apply(field.getFieldType().getArrayValueType().getElementSchema(), (MElement) d);
                                oo.add(o);
                            }
                            yield oo;
                        }
                        default -> convertedValue;
                    };
                    case null -> null;
                    default -> convertedValue;
                };
            }

            return fieldValues;
        }
    }

    public static CustomReturnTypeInference createCustomReturnTypeInference(final Schema.FieldType fieldType) {
        return new CustomReturnTypeInference(fieldType);
    }

    public static class CustomReturnTypeInference implements SqlReturnTypeInference {

        private final Schema.FieldType fieldType;

        private CustomReturnTypeInference(final Schema.FieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final RelDataType relDataType = switch (this.fieldType.getType()) {
                case bool -> typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case string, json -> typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case bytes ->  typeFactory.createSqlType(SqlTypeName.BINARY);
                case int32 -> typeFactory.createSqlType(SqlTypeName.INTEGER);
                case int64 -> typeFactory.createSqlType(SqlTypeName.BIGINT);
                case float32 -> typeFactory.createSqlType(SqlTypeName.REAL);
                case float64 -> typeFactory.createSqlType(SqlTypeName.FLOAT);
                case date -> typeFactory.createSqlType(SqlTypeName.DATE);
                case time -> typeFactory.createSqlType(SqlTypeName.TIME);
                case timestamp -> typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case element -> {
                    final List<String> fieldNames = new ArrayList<>();
                    final List<RelDataType> fieldTypes = new ArrayList<>();
                    for(final Schema.Field field : fieldType.getElementSchema().getFields()) {
                        fieldNames.add(field.getName());
                        fieldTypes.add(createRelDataType(field.getFieldType(), typeFactory));
                    }
                    yield typeFactory.createStructType(fieldTypes, fieldNames);
                }
                case array -> typeFactory.createArrayType(
                        createRelDataType(fieldType.getArrayValueType(), typeFactory), -1L);
                default -> throw new IllegalArgumentException();
            };

            return typeFactory.createTypeWithNullability(relDataType, true);
        }
    }

}
