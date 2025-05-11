package com.mercari.solution.util.gcp;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.mercari.solution.util.sql.stmt.PreparedStatementTemplate;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class JdbcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    public enum DB {
        MYSQL,
        POSTGRESQL,
        SQLSERVER,
        H2
    }

    public enum OP {
        INSERT,
        INSERT_OR_UPDATE,
        INSERT_OR_DONOTHING,
        DELETE
    }

    private static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "ALL","AND","ANY","ARRAY","AS","ASC","ASSERT_ROWS_MODIFIED","AT",
            "BETWEEN","BY","CASE","CAST","COLLATE","CONTAINS","CREATE","CROSS","CUBE","CURRENT",
            "DEFAULT","DEFINE","DESC","DISTINCT","ELSE","END","ENUM","ESCAPE","EXCEPT","EXCLUDE","EXISTS","EXTRACT",
            "FALSE","FETCH","FOLLOWING","FOR","FROM","FULL","GROUP","GROUPING","GROUPS","HASH","HAVING",
            "IF","IGNORE","IN","INNER","INTERSECT","INTERVAL","INTO","IS","JOIN",
            "LATERAL","LEFT","LIKE","LIMIT","LOOKUP","MERGE","NATURAL","NEW","NO","NOT","NULL","NULLS",
            "OF","ON","OR","ORDER","OUTER","OVER","PARTITION","PRECEDING","PROTO","RANGE");


    public static CloseableDataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password) {

        return createDataSource(driverClassName, url, username, password, false);
    }

    public static CloseableDataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final boolean readOnly) {

        final BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(driverClassName);
        basicDataSource.setUrl(url);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);

        // Wrapping the datasource as a pooling datasource
        final DataSourceConnectionFactory connectionFactory = new DataSourceConnectionFactory(basicDataSource);
        final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMinIdle(0);
        poolConfig.setMinEvictableIdleTimeMillis(10000);
        poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
        final GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory, poolConfig);
        poolableConnectionFactory.setPool(connectionPool);
        poolableConnectionFactory.setDefaultAutoCommit(false);
        poolableConnectionFactory.setDefaultReadOnly(readOnly);
        return new CloseableDataSource(new PoolingDataSource(connectionPool));
    }

    public static Schema createAvroSchemaFromTable(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final String table) throws SQLException {

        try {
            Class.forName(driverClassName);
            try(final Connection connection = DriverManager.getConnection(url, username, password)) {
                final String tableQuery = String.format("SELECT * FROM %s LIMIT 1", table);
                try(final PreparedStatement statement = connection.prepareStatement(tableQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    if(statement.getMetaData() == null) {
                        throw new IllegalArgumentException("Failed to get schema for query: " + tableQuery);
                    }
                    final Schema schema = ResultSetToRecordConverter.convertSchema(statement.getMetaData());
                    final List<String> parameterFieldNames = getPrimaryKeyNames(connection, null, null, table);
                    final String parameterFieldNamesAttr = String.join(",", parameterFieldNames);
                    final SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder
                            .record("root")
                            .prop("primaryKeys", parameterFieldNamesAttr)
                            .fields();
                    schema.getFields().forEach(f -> {
                        if(parameterFieldNames.contains(f.name())) {
                            schemaBuilder.name(f.name()).type(f.schema()).noDefault();
                        }
                    });
                    return schemaBuilder.endRecord();
                }
            }
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("driver class: " + driverClassName + " not found", e);
        }
    }

    public static Schema createAvroSchemaFromQuery(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final String query,
            final List<String> prepareCalls) throws Exception {

        try {
            Class.forName(driverClassName);
            try(final Connection connection = DriverManager.getConnection(url, username, password)) {
                return createAvroSchemaFromQuery(connection, query, prepareCalls);
            }
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("driver class: " + driverClassName + " not found", e);
        }
    }

    public static Schema createAvroSchemaFromQuery(
            final Connection connection,
            final String query,
            final List<String> prepareCalls) throws Exception {

        for(final String prepareCall : prepareCalls) {
            try(final CallableStatement statement = connection
                    .prepareCall(prepareCall, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                final boolean result = statement.execute();
                if(result) {
                    LOG.info("Execute prepareCall: " + prepareCall);
                } else {
                    LOG.error("Failed execute prepareCall: " + prepareCall);
                }
            }
        }

        try(final PreparedStatement statement = connection
                .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            if(statement.getMetaData() == null) {
                throw new IllegalArgumentException("Failed to get schema for query: " + query);
            }

            return ResultSetToRecordConverter.convertSchema(statement.getMetaData());
        }
    }

    public static String buildCreateTableSQL(final Schema schema,
                                             final String table,
                                             final DB db,
                                             final List<String> keyFields) {

        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s (", table));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.schema()))
                .forEach(f -> sb.append(String.format("%s %s%s,",
                        replaceReservedKeyword(f.name()),
                        getColumnType(f.schema(), db, keyFields.contains(f.name())),
                        AvroSchemaUtil.isNullable(f.schema()) ? "" : " NOT NULL")));

        if(keyFields == null || keyFields.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
            sb.append(");");
        } else {
            final String primaryKey = keyFields.stream()
                    .map(JdbcUtil::replaceReservedKeyword)
                    .collect(Collectors.joining(","));
            sb.append(String.format(" PRIMARY KEY ( %s ));", primaryKey));
        }
        return sb.toString();
    }

    public static PreparedStatementTemplate createStatement(final String table, final Schema schema,
                                         final OP op, final DB db,
                                         final List<String> keyFields) {

        if(op.equals(OP.DELETE)) {
            throw new IllegalArgumentException("jdbc module does not support DELETE op.");
        }

        return switch (db) {
            case MYSQL -> createMySQLStatement(table, schema, op, keyFields);
            case POSTGRESQL -> createPostgreSQLStatement(table, schema, op, keyFields);
            case H2 -> createH2Statement(table, schema, op, keyFields);
            case SQLSERVER -> createSQLServerStatement(table, schema, op, keyFields);
            default -> throw new IllegalArgumentException("Not supported database: " + db);
        };
    }

    private static PreparedStatementTemplate createMySQLStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        sb.appendString("INSERT INTO ").appendString(table);

        sb.appendString(" (");
        schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
        sb.removeLast();
        sb.appendString(")");

        sb.appendString(" VALUES (");
        IntStream.range(0, schema.getFields().size()).forEach(
            i -> sb.appendPlaceholder(i + 1).appendString(",")
        );
        sb.removeLast();
        sb.appendString(")");

        if(op.equals(OP.INSERT_OR_UPDATE)) {
            sb.appendString(" ON DUPLICATE KEY UPDATE ");
            schema.getFields().forEach(f -> {
                if (keyFields.contains(f.name())) return;

                sb.appendBackQuoted(f.name()).appendString(" = VALUES(").appendBackQuoted(f.name()).appendString(")");
                sb.appendString(",");
            });
            sb.removeLast();
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            sb.appendString(" ON DUPLICATE KEY UPDATE ");
            keyFields.forEach(f -> {
                sb.appendBackQuoted(f).appendString(" = VALUES(").appendBackQuoted(f).appendString(")");
                sb.appendString(",");
            });
            sb.removeLast();
        }

        return sb.build();
    }

    private static void appendPostgreSQLTypedPlaceholder(PreparedStatementTemplate.Builder sb, int index , Schema.Field field) {
        sb.appendPlaceholder(index);

         if (field.schema().getType() == Schema.Type.UNION) {
             if (
                 field.schema().equals(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE) ||
                 field.schema().equals(AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE)
             ) {
                 sb.appendString("::date");
             } else if (
                 field.schema().equals(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE) ||
                 field.schema().equals(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE) ||
                 field.schema().equals(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MILLI_TYPE) ||
                 field.schema().equals(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MILLI_TYPE)
             ) {
                 sb.appendString("::timestamp");
             }
         } else {
             LogicalType logicalType = field.schema().getLogicalType();
             if (logicalType != null) {
                 if (logicalType.equals(LogicalTypes.date())) {
                     sb.appendString("::date");
                 } else if (logicalType.equals(LogicalTypes.timestampMicros()) ||
                         logicalType.equals(LogicalTypes.timestampMillis())) {
                     sb.appendString("::timestamp");
                 }
             }
         }
    }

    private static PreparedStatementTemplate createPostgreSQLStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if (op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES (");
            IntStream.range(0, schema.getFields().size()).forEach(i -> {
                appendPostgreSQLTypedPlaceholder(sb, i + 1, schema.getFields().get(i));
                sb.appendString(",");
            });
            sb.removeLast();
            sb.appendString(")");
        } else if (op.equals(OP.INSERT_OR_UPDATE) || op.equals(OP.INSERT_OR_DONOTHING)) {
            sb.appendString("MERGE INTO ");
            sb.appendString(table);

            sb.appendString(" USING (VALUES (");
            IntStream.range(0, schema.getFields().size()).forEach(i -> {
                    appendPostgreSQLTypedPlaceholder(sb, i + 1, schema.getFields().get(i));
                    sb.appendString(",");
                }
            );
            sb.removeLast();
            sb.appendString("))");

            sb.appendString(" AS item (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(") ON ");

            keyFields.forEach(f -> {
                sb.appendString("item.").appendString(f).appendString(" = ").appendString(table).appendString(".").appendString(f);
                sb.appendString(" AND ");
            });
            sb.removeLast();

            sb.appendString(" WHEN MATCHED THEN ");

            if (op.equals(OP.INSERT_OR_DONOTHING)) {
                sb.appendString("DO NOTHING");
            } else {
                sb.appendString("UPDATE SET ");
                schema.getFields().forEach(f -> {
                    if (keyFields.contains(f.name())) return;

                    sb.appendString(f.name()).appendString(" = item.").appendString(f.name());
                    sb.appendString(",");
                });
                sb.removeLast();
            }

            sb.appendString(" WHEN NOT MATCHED THEN ");

            sb.appendString("INSERT (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(") VALUES (");
            schema.getFields().forEach(f -> sb.appendString("item.").appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");
        }

        return sb.build();
    }

    private static PreparedStatementTemplate createH2Statement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if(op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES (");
            IntStream.range(0, schema.getFields().size()).forEach(
                i -> sb.appendPlaceholder(i + 1).appendString(",")
            );
            sb.removeLast();
            sb.appendString(")");
        } else if(op.equals(OP.INSERT_OR_UPDATE)) {
            sb.appendString("MERGE INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" KEY (");
            keyFields.forEach(f -> sb.appendString(f).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES (");
            IntStream.range(0, schema.getFields().size()).forEach(
                i -> sb.appendPlaceholder(i + 1).appendString(",")
            );
            sb.removeLast();
            sb.appendString(")");
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            throw new IllegalArgumentException("H2 does not support INSERT_OR_DONOTHING.");
        }

        return sb.build();
    }

    private static PreparedStatementTemplate createSQLServerStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if(op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES (");
            IntStream.range(0, schema.getFields().size()).forEach(
                i -> sb.appendPlaceholder(i + 1).appendString(",")
            );
            sb.removeLast();
            sb.appendString(")");
        } else if(op.equals(OP.INSERT_OR_UPDATE)) {
            throw new IllegalArgumentException("SQLServer does not support INSERT_OR_UPDATE.");
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            throw new IllegalArgumentException("SQLServer does not support INSERT_OR_DONOTHING.");
        }

        return sb.build();
    }

    public static void setStatement(final PreparedStatement statement,
                                    final int parameterIndex,
                                    final Schema fieldSchema,
                                    final Object fieldValue) throws SQLException {

        if(fieldValue == null) {
            return;
        }
        switch (fieldSchema.getType()) {
            case BOOLEAN -> statement.setBoolean(parameterIndex, (Boolean) fieldValue);
            case ENUM, STRING -> statement.setString(parameterIndex, fieldValue.toString());
            case FIXED, BYTES -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                    final ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
                    final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(fieldSchema, byteBuffer);
                    statement.setBigDecimal(parameterIndex, decimal);
                } else {
                    statement.setBytes(parameterIndex, ((ByteBuffer) fieldValue).array());
                }
            }
            case INT -> {
                final Integer value = (Integer) fieldValue;
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    statement.setDate(parameterIndex, java.sql.Date.valueOf(LocalDate.ofEpochDay(Long.valueOf(value))));
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000_000L)));
                } else {
                    statement.setInt(parameterIndex, value);
                }
            }
            case LONG -> {
                final Long value = (Long) fieldValue;
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value * 1000L)));
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value)));
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000L)));
                } else {
                    statement.setLong(parameterIndex, value);
                }
                break;
            }
            case FLOAT -> statement.setFloat(parameterIndex, (Float) fieldValue);
            case DOUBLE -> statement.setDouble(parameterIndex, (Double) fieldValue);
            case NULL -> {}
            case UNION -> setStatement(statement, parameterIndex, AvroSchemaUtil.unnestUnion(fieldSchema), fieldValue);
            default -> throw new IllegalStateException("Not supported prepare parameter type: " + fieldSchema.getType().getName());
        }
    }

    public static List<String> getPrimaryKeyNames(
            final Connection connection,
            final String database,
            final String namespace,
            final String table) throws SQLException {

        final DatabaseMetaData metaData = connection.getMetaData();
        try (final ResultSet resultSet = metaData.getPrimaryKeys(database, namespace, table)) {
            final Map<Integer,String> primaryKeyNames = new HashMap<>();
            while(resultSet.next()) {
                final Integer primaryKeySeq  = resultSet.getInt("KEY_SEQ");
                final String primaryKeyName = resultSet.getString("COLUMN_NAME");
                primaryKeyNames.put(primaryKeySeq, primaryKeyName);
            }
            if(primaryKeyNames.isEmpty()) {
                LOG.warn("No primary key");
                try(final ResultSet resultSetRowKey = metaData.getBestRowIdentifier(database, namespace, table, DatabaseMetaData.bestRowUnknown, true)) {
                    int i = 0;
                    while(resultSetRowKey.next()) {
                        final Integer primaryKeySeq = i++;
                        final String uniqueKeyName = resultSetRowKey.getString("COLUMN_NAME");
                        primaryKeyNames.put(primaryKeySeq, uniqueKeyName);
                    }
                }
                LOG.info("Unique key size: " + primaryKeyNames.size());
            }
            return primaryKeyNames.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
        }
    }

    public static DB extractDbFromDriver(final String driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver must not be null");
        }
        if(driver.contains("mysql")) {
            return DB.MYSQL;
        } else if(driver.contains("postgresql")) {
            return DB.POSTGRESQL;
        } else if(driver.contains("sqlserver")) {
            return DB.SQLSERVER;
        } else {
            throw new IllegalArgumentException("Not supported database: " + driver);
        }
    }

    private static boolean isValidColumnType(final Schema fieldSchema) {
        return switch (fieldSchema.getType()) {
            case MAP, RECORD -> false;
            case ARRAY -> isValidColumnType(fieldSchema.getElementType());
            case UNION -> isValidColumnType(AvroSchemaUtil.unnestUnion(fieldSchema));
            default -> true;
        };
    }

    private static String getColumnType(final Schema schema, final DB db, final boolean isPrimaryKey) {
        final Schema avroSchema = AvroSchemaUtil.unnestUnion(schema);
        return switch (avroSchema.getType()) {
            case BOOLEAN -> switch (db) {
                case MYSQL -> "TINYINT(1)";
                default -> "BOOLEAN";
            };
            case ENUM -> switch (db) {
                case MYSQL -> "VARCHAR(32) CHARACTER SET utf8mb4";
                default -> "VARCHAR(32)";
            };
            case STRING -> switch (db) {
                case MYSQL -> {
                    if (isPrimaryKey) {
                        yield "VARCHAR(64) CHARACTER SET utf8mb4";
                    } else {
                        yield "TEXT CHARACTER SET utf8mb4";
                    }
                }
                default -> {
                    if (isPrimaryKey) {
                        yield "VARCHAR(64)";
                    } else {
                        yield "TEXT";
                    }
                }
            };
            case FIXED, BYTES -> {
                if (AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    yield "DECIMAL(38, 9)";
                }
                yield switch (db) {
                    case MYSQL -> "MEDIUMBLOB";
                    case POSTGRESQL -> "BYTEA";
                    default -> "BLOB";
                };
            }
            case INT -> {
                if (LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    yield "DATE";
                } else if (LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    yield "TIME";
                } else {
                    yield "INTEGER";
                }
            }
            case LONG -> switch (db) {
                case MYSQL -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                case POSTGRESQL -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                case SQLSERVER -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                default -> "BIGINT";

            };
            case FLOAT -> "REAL";
            case DOUBLE -> switch (db) {
                case MYSQL -> "DOUBLE";
                case POSTGRESQL -> "DOUBLE PRECISION";
                default -> "DOUBLE";
            };
            default -> throw new IllegalArgumentException(String.format("DataType: %s is not supported!", avroSchema.getType().name()));
        };
    }

    private static String replaceReservedKeyword(final String term) {
        if(RESERVED_KEYWORDS.contains(term.trim().toUpperCase())) {
            return String.format("`%s`", term);
        }
        return term;
    }

    public static List<IndexRange> splitIndexRange(
            final List<IndexOffset> parents,
            final List<IndexOffset> from,
            final List<IndexOffset> to,
            final int splitNum) {

        final IndexOffset firstFromOffset = from.get(0);
        final IndexOffset firstToOffset = to.get(0);

        final List<IndexOffset> splitOffsets = switch (firstFromOffset.getFieldType()) {
            case BOOLEAN -> splitBoolean(firstFromOffset.getFieldName(), firstToOffset.getAscending());
            case INT -> splitInteger(firstFromOffset.getFieldName(),
                        firstFromOffset.getIntValue(), firstToOffset.getIntValue(),
                        firstFromOffset.getAscending(), splitNum);
            case LONG -> splitLong(firstFromOffset.getFieldName(),
                        firstFromOffset.getLongValue(), firstToOffset.getLongValue(),
                        firstFromOffset.getAscending(), splitNum);
            case FLOAT -> splitFloat(firstFromOffset.getFieldName(),
                        firstFromOffset.getFloatValue(), firstToOffset.getFloatValue(),
                        firstFromOffset.getAscending(), splitNum);
            case DOUBLE -> splitDouble(firstFromOffset.getFieldName(),
                        firstFromOffset.getDoubleValue(), firstToOffset.getDoubleValue(),
                        firstFromOffset.getAscending(), splitNum);
            case ENUM, STRING -> splitString(firstFromOffset.getFieldName(),
                        firstFromOffset.getStringValue(), firstToOffset.getStringValue(),
                        firstFromOffset.getAscending(), splitNum,
                        firstFromOffset.getIsCaseSensitive());
            case FIXED, BYTES -> splitBytes(firstFromOffset.getFieldName(),
                        firstFromOffset.getBytesValue(), firstToOffset.getBytesValue(),
                        firstFromOffset.getAscending(), splitNum);
            default -> throw new IllegalArgumentException("Not supported range type: " + firstFromOffset.getFieldType());
        };

        List<IndexOffset> parentOffsets = new ArrayList<>();
        if(parents != null && !parents.isEmpty()) {
            parentOffsets.addAll(parents);
        }

        if(splitOffsets.isEmpty()) {
            if(from.size() > 1 && to.size() > 1 && false) {
                // TODO recursive splitting
                return splitIndexRange(parentOffsets, from.subList(1, from.size()), to.subList(1, to.size()), splitNum);
            } else {
                return Arrays.asList(IndexRange.of(
                        IndexPosition.of(from, true),
                        IndexPosition.of(to, false)));
            }
        } else {
            final List<IndexRange> results = new ArrayList<>();
            List<IndexOffset> nextFrom = new ArrayList<>(parentOffsets);
            nextFrom.addAll(from);
            for(final IndexOffset offset : splitOffsets) {
                final List<IndexOffset> nextTo = new ArrayList<>(parentOffsets);
                nextTo.add(offset);
                final IndexRange range = IndexRange.of(
                        IndexPosition.of(nextFrom, true),
                        IndexPosition.of(nextTo, false));
                results.add(range);
                List<IndexOffset> offsets = new ArrayList<>(parentOffsets);
                offsets.add(offset);
                nextFrom = offsets;
            }

            return results;
        }
    }

    private static List<IndexOffset> splitBoolean(final String name, final boolean ascending) {
        final List<IndexOffset> results = new ArrayList<>();
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.FALSE));
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.TRUE));
        return results;
    }

    private static List<IndexOffset> splitInteger(final String name, Integer min, Integer max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            min = Integer.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            max = Integer.MAX_VALUE;
        }
        boundSize = Math.abs((max.doubleValue() / splitNum) - (min.doubleValue() / splitNum));

        int prev = min;
        for(int i=1; i<splitNum; i++) {
            int next = (int)Math.round(min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.INT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitLong(final String name, Long min, Long max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            min = Long.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            max = Long.MAX_VALUE;
        }
        boundSize = Math.abs((max.doubleValue() / splitNum) - (min.doubleValue() / splitNum));

        long prev = min;
        for(int i=1; i<splitNum; i++) {
            long next = Math.round(min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitFloat(final String name, Float min, Float max, final boolean ascending, final int splitNum) {
        final float boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            min = Float.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            max = Float.MAX_VALUE;
        }
        boundSize = Math.abs((max / splitNum) - (min / splitNum));

        float prev = min;
        for(int i=1; i<splitNum; i++) {
            float next = min + boundSize * i;
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitDouble(final String name, Double min, Double max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            min = Double.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            max = Double.MAX_VALUE;
        }
        boundSize = Math.abs((max / splitNum) - (min / splitNum));

        double prev = min;
        for(int i=1; i<splitNum; i++) {
            double next = (min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitNumeric(final String name, BigDecimal min, BigDecimal max, final boolean ascending, final int splitNum) {
        final BigDecimal boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            min = BigDecimal.valueOf(Double.MIN_VALUE);
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            max = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        boundSize = max.subtract(min).divide(BigDecimal.valueOf(splitNum)).abs();

        BigDecimal prev = min;
        for(int i=1; i<splitNum; i++) {
            BigDecimal next = min.add((boundSize.multiply(BigDecimal.valueOf(i))));
            if(prev == next || next.subtract(max).doubleValue() >= 0D) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, max));
        return results;
    }

    public static List<IndexOffset> splitString(final String name, String min, String max, final boolean ascending, final int splitNum, final boolean isCaseSensitive) {
        if(min == null || min.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            sb.append(String.valueOf((char) 33).repeat(32));
            min = sb.toString();
        }
        if(max == null || max.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            if(isCaseSensitive) {
                sb.append(String.valueOf((char) 126).repeat(32));
            } else {
                sb.append(String.valueOf((char) 126 - 26).repeat(32));
            }
            max = sb.toString();
        }

        final char[] mins = min.toCharArray();
        final char[] maxs = max.toCharArray();
        final List<String> strs = splitChar(mins, maxs, 0, splitNum, isCaseSensitive);
        return strs.stream()
                .map(s -> IndexOffset.of(name, Schema.Type.STRING, ascending, s))
                .collect(Collectors.toList());
    }

    public static List<IndexOffset> splitBytes(final String name, ByteBuffer min, ByteBuffer max, final boolean ascending, final int splitNum) {
        if(min == null && max == null) {
            return Arrays.asList(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
        } else if(min == null) {
            byte[] bytes = new byte[32];
            for(int i=0; i<32; i++) {
                bytes[i] = -128;
            }
            min = ByteBuffer.wrap(bytes);
        } else if(max == null) {
            byte[] bytes = new byte[32];
            for(int i=0; i<32; i++) {
                bytes[i] = 127;
            }
            max = ByteBuffer.wrap(bytes);
        }

        final byte[] mins = min.array();
        final byte[] maxs = max.array();

        final List<byte[]> bytes = splitByte(mins, maxs, 0, splitNum);
        return bytes.stream()
                .map(ByteBuffer::wrap)
                .map(s -> IndexOffset.of(name, Schema.Type.BYTES, ascending, s))
                .collect(Collectors.toList());
    }

    static List<String> splitChar(char[] min, char[] max, int index, int splitNum, final boolean isCaseSensitive) {
        if(index >= min.length || index >= max.length) {
            return new ArrayList<>();
        }
        final char cmin;
        final char cmax;
        if(isCaseSensitive) {
            cmin = min[index];
            cmax = max[index];
        } else {
            if(min[index] >= 97 && min[index] <= 122) {
                cmin = (char)(min[index] - 32);
            } else if(min[index] >= 123) {
                cmin = (char)(min[index] - 26);
            } else {
                cmin = min[index];
            }
            if(max[index] >= 97 && max[index] <= 122) {
                cmax = (char)(max[index] - 32);
            } else if(max[index] >= 123) {
                cmax = (char)(max[index] - 26);
            } else {
                cmax = max[index];
            }
        }

        final int diff = cmax - cmin;
        if(diff < 0) {
            throw new IllegalStateException("Illegal string min: " + min + ", max: " + max);
        } else if(diff == 0) {
            return splitChar(
                    min,
                    max,
                    index + 1,
                    splitNum,
                    isCaseSensitive);
        } else {
            final List<Character> results = new ArrayList<>();
            final double boundSize = (double)diff / (double)splitNum;
            char prev = cmin;
            for(int i=1; i<splitNum; i++) {
                char next = (char)Math.round(cmin + boundSize * i);
                if(prev == next || next >= cmax) {
                    continue;
                }
                if(!isCaseSensitive) {
                    if(next >= 97 && next <= 122) {
                        next = (char)(next + 26);
                    }
                }
                results.add(next);
                prev = next;
            }
            results.add(cmax);

            char[] prefix = new char[index + 1];
            for(int i=0; i<index; i++) {
                prefix[i] = min[i];
            }

            final List<String> strs = new ArrayList<>();
            for(int i=0; i<results.size() - 1; i++) {
                prefix[index] = results.get(i);
                strs.add(String.valueOf(prefix));
            }
            strs.add(String.valueOf(max));
            return strs;
        }
    }

    static List<byte[]> splitByte(byte[] min, byte[] max, int index, int splitNum) {
        if(index >= min.length || index >= max.length) {
            return new ArrayList<>();
        }
        final byte cmin = min[index];
        final byte cmax = max[index];
        final int diff = cmax - cmin;
        if(diff < 0) {
            throw new IllegalStateException("Illegal byte at index: " + index + ", min: " + cmin + ", max: " + cmax);
        } else if(diff == 0) {
            return splitByte(
                    min,
                    max,
                    index + 1,
                    splitNum);
        } else {
            final List<Byte> results = new ArrayList<>();
            final double boundSize = (double)diff / (double)splitNum;
            byte prev = cmin;
            for(int i=1; i<splitNum; i++) {
                byte next = (byte)Math.round(cmin + boundSize * i);
                if(prev == next || next >= cmax) {
                    continue;
                }
                results.add(next);
                prev = next;
            }
            results.add(cmax);

            byte[] prefix = new byte[index + 1];
            for(int i=0; i<index; i++) {
                prefix[i] = min[i];
            }

            final List<byte[]> strs = new ArrayList<>();
            for(int i=0; i<results.size() - 1; i++) {
                prefix[index] = results.get(i);
                strs.add(Arrays.copyOf(prefix, prefix.length));
            }
            strs.add(Arrays.copyOf(max, max.length));
            return strs;
        }
    }

    public static String createSeekPreparedQuery(
            final IndexPosition startPosition,
            final IndexPosition stopPosition,
            final String fields,
            final String table,
            final List<String> parameterFields,
            final Integer limit) {

        String preparedQuery = String.format("SELECT %s FROM %s", fields, table);

        final List<String> startConditions = createSeekConditions(startPosition.getOffsets(), true, startPosition.getIsOpen());
        final List<String> stopConditions = createSeekConditions(stopPosition.getOffsets(), false, stopPosition.getIsOpen());

        final String startCondition = "(" + String.join(" OR ", startConditions) + ")";
        final String stopCondition = "(" + String.join(" OR ", stopConditions) + ")";

        final String condition = startCondition + " AND " + stopCondition;
        preparedQuery = preparedQuery + " WHERE " + condition;

        final String parameterFieldsString = parameterFields.stream()
                .map(f -> f.replaceFirst(":", " "))
                .collect(Collectors.joining(", "));
        preparedQuery = preparedQuery + String.format(" ORDER BY %s LIMIT %d", parameterFieldsString, limit);
        return preparedQuery;
    }

    public static List<String> createSeekConditions(
            final List<IndexOffset> offsets,
            final boolean isStart,
            final boolean isOpen) {

        if(offsets.size() == 1) {
            final IndexOffset offset = offsets.get(0);
            if(offset.getValue() == null) {
                return Arrays.asList(offset.getFieldName() + (isOpen ? " IS NOT NULL" : " IS NULL"));
            } else {
                final String operation = (isStart ? Condition.GREATER : Condition.LESSER).getName(offset.getAscending()) + (isOpen ? "" : "=");
                final String condition = offset.getFieldName() + " " + operation + " ?";
                return Arrays.asList(condition);
            }
        }
        final List<String> andConditions = new ArrayList<>();
        for(int i=0; i<offsets.size()-1; i++) {
            final IndexOffset offset = offsets.get(i);
            if(offset.getValue() == null) {
                andConditions.add(offset.getFieldName() + " IS NULL");
            } else {
                andConditions.add(offset.getFieldName() + " = ?");
            }
        }
        final IndexOffset offset = offsets.get(offsets.size() - 1);
        if(offset.getValue() == null) {
            andConditions.add(offset.getFieldName() + (isOpen ? " IS NOT NULL" : " IS NULL"));
        } else {
            final String operation = (isStart ? Condition.GREATER : Condition.LESSER).getName(offset.getAscending()) + (isOpen ? "" : "=");
            andConditions.add(offset.getFieldName() + " " + operation + " ?");
        }

        final String condition = "(" + String.join(" AND ", andConditions) + ")";
        final List<String> conditions = new ArrayList<>();
        conditions.add(condition);

        final List<String> childrenConditions = createSeekConditions(offsets.subList(0, offsets.size() - 1), isStart, isOpen);
        conditions.addAll(childrenConditions);
        return conditions;
    }

    public static int setStatementParameters(
            final PreparedStatement statement,
            final List<JdbcUtil.IndexOffset> offsets,
            final Map<String, Schema.Field> fields,
            final int paramIndexOffset) throws SQLException {

        int paramIndex = paramIndexOffset;
        for(IndexOffset offset : offsets) {
            final Object value = offset.getValue();
            Schema.Field field = fields.get(offset.getFieldName());
            if(field == null) {
                // For PostgreSQL
                field = fields.get(offset.getFieldName().toLowerCase());
            }
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            JdbcUtil.setStatement(statement, paramIndex, fieldSchema, value);
            if(value != null) {
                paramIndex = paramIndex + 1;
            }
        }
        if(offsets.size() > 0) {
            paramIndex = setStatementParameters(
                    statement,
                    offsets.subList(0, offsets.size() - 1),
                    fields,
                    paramIndex);
        }
        return paramIndex;
    }

    public enum Condition implements Serializable {
        GREATER(">"),
        LESSER("<"),
        EQUAL("=");

        private final String name;

        public String getName() {
            return this.name;
        }

        public String getName(final boolean ascending) {
            if(ascending) {
                return this.getName();
            } else {
                return this.reverse().getName();
            }
        }

        Condition(String name) {
            this.name = name;
        }

        public Condition reverse() {
            if(GREATER.equals(this)) {
                return LESSER;
            } else if(LESSER.equals(this)) {
                return GREATER;
            } else {
                return this;
            }
        }
    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexOffset {

        @Nullable
        private String fieldName;
        @Nullable
        private Schema.Type fieldType;
        @Nullable
        private Boolean ascending;

        @Nullable
        private Boolean booleanValue;
        @Nullable
        private String stringValue;
        @Nullable
        private ByteBuffer bytesValue;
        @Nullable
        private Integer intValue;
        @Nullable
        private Long longValue;
        @Nullable
        private Float floatValue;
        @Nullable
        private Double doubleValue;

        @Nullable
        private String logicalType;

        @Nullable
        private Boolean isCaseSensitive;


        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public Schema.Type getFieldType() {
            return fieldType;
        }

        public void setFieldType(Schema.Type fieldType) {
            this.fieldType = fieldType;
        }

        public Boolean getAscending() {
            return ascending;
        }

        public void setAscending(Boolean ascending) {
            this.ascending = ascending;
        }

        public Boolean getBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(Boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(String stringValue) {
            this.stringValue = stringValue;
        }

        public ByteBuffer getBytesValue() {
            return bytesValue;
        }

        public void setBytesValue(ByteBuffer bytesValue) {
            this.bytesValue = bytesValue;
        }

        public Integer getIntValue() {
            return intValue;
        }

        public void setIntValue(Integer intValue) {
            this.intValue = intValue;
        }

        public Long getLongValue() {
            return longValue;
        }

        public void setLongValue(Long longValue) {
            this.longValue = longValue;
        }

        public Float getFloatValue() {
            return floatValue;
        }

        public void setFloatValue(Float floatValue) {
            this.floatValue = floatValue;
        }

        public Double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(Double doubleValue) {
            this.doubleValue = doubleValue;
        }

        public String getLogicalType() {
            return logicalType;
        }

        public void setLogicalType(String logicalType) {
            this.logicalType = logicalType;
        }

        public Boolean getIsCaseSensitive() {
            return isCaseSensitive;
        }

        public void setIsCaseSensitive(Boolean caseSensitive) {
            isCaseSensitive = caseSensitive;
        }

        public Object getValue() {
            return switch (this.fieldType) {
                case BOOLEAN -> this.booleanValue;
                case ENUM, STRING -> this.stringValue;
                case FIXED, BYTES -> this.bytesValue;
                case INT -> this.intValue;
                case LONG -> this.longValue;
                case FLOAT -> this.floatValue;
                case DOUBLE -> this.doubleValue;
                case NULL -> null;
                default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
            };
        }

        public boolean isGreaterThan(final IndexOffset another) {
            return compareTo(another) > 0;
        }

        public boolean isLesserThan(final IndexOffset another) {
            return compareTo(another) < 0;
        }

        public int compareTo(final IndexOffset another) {
            if(this.getValue() == null && another.getValue() == null) {
                return 0;
            } else if(this.getValue() == null) {
                return -1;
            } else if(another.getValue() == null) {
                return 1;
            }
            return switch (this.fieldType) {
                case BOOLEAN -> this.booleanValue.compareTo(another.getBooleanValue());
                case ENUM, STRING -> {
                    if(isCaseSensitive) {
                        yield this.stringValue.compareTo(another.getStringValue());
                    } else {
                        yield this.stringValue.compareToIgnoreCase(another.getStringValue());
                    }
                }
                case FIXED, BYTES -> {
                    if("decimal".equals(logicalType)) {
                        yield BigDecimal.valueOf(new BigInteger(this.bytesValue.array()).longValue(), 9)
                                .compareTo(BigDecimal.valueOf(new BigInteger(another.bytesValue.array()).longValue(), 9));
                    }
                    yield new String(Hex.encodeHex(this.bytesValue.array()))
                            .compareTo(new String(Hex.encodeHex(another.bytesValue.array())));
                }
                case INT -> this.intValue.compareTo(another.getIntValue());
                case LONG -> this.longValue.compareTo(another.getLongValue());
                case FLOAT -> this.floatValue.compareTo(another.getFloatValue());
                case DOUBLE -> this.doubleValue.compareTo(another.getDoubleValue());
                case NULL -> 0;
                default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
            };
        }

        @Override
        public String toString() {
            return String.format("IndexOffset: %s = %s",
                    this.getFieldName() + (this.getAscending() ? "" : "(DESC)"),
                    this.valueToString());
        }

        private String valueToString() {
            if(getValue() == null) {
                return "null";
            }
            return switch (this.fieldType) {
                case BOOLEAN -> this.booleanValue.toString();
                case ENUM, STRING -> this.stringValue;
                case FIXED, BYTES -> {
                    if ("decimal".equals(logicalType)) {
                        yield BigDecimal.valueOf(new BigInteger(this.bytesValue.array()).longValue(), 9).toString();
                    }
                    yield new String(Hex.encodeHex(this.bytesValue.array()));
                }
                case INT -> {
                    if ("date".equals(logicalType)) {
                        yield LocalDate.ofEpochDay(this.intValue).toString();
                    } else if ("time-millis".equals(logicalType)) {
                        yield LocalTime.ofNanoOfDay(1000000L * this.intValue).toString();
                    }
                    yield this.intValue.toString();
                }
                case LONG -> {
                    if ("timestamp-micros".equals(logicalType)) {
                        yield DateTimeUtil.toLocalDateTime(this.longValue).toString();
                    } else if ("time-micros".equals(logicalType)) {
                        yield LocalTime.ofNanoOfDay(1000L * this.intValue).toString();
                    }
                    yield this.longValue.toString();
                }
                case FLOAT -> this.floatValue.toString();
                case DOUBLE -> this.doubleValue.toString();
                case NULL -> null;
                default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
            };
        }

        public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value) {
            return of(fieldName, fieldType, ascending, value, null, true);
        }

        public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value, final String logicalType) {
            return of(fieldName, fieldType, ascending, value, logicalType, true);
        }

        public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value, final boolean isCaseSensitive) {
            return of(fieldName, fieldType, ascending, value, null, isCaseSensitive);
        }

        public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value,
                                     final String logicalType, final boolean isCaseSensitive) {
            final IndexOffset indexOffset = new IndexOffset();
            indexOffset.setFieldName(fieldName);
            indexOffset.setFieldType(fieldType);
            indexOffset.setAscending(ascending);
            indexOffset.setLogicalType(logicalType);
            indexOffset.setIsCaseSensitive(isCaseSensitive);
            switch (fieldType) {
                case BOOLEAN -> {
                    indexOffset.booleanValue = (Boolean) value;
                }
                case ENUM, STRING -> {
                    if(value == null) {
                        indexOffset.stringValue = null;
                    } else {
                        indexOffset.stringValue = value.toString();
                    }
                }
                case FIXED, BYTES -> {
                    indexOffset.bytesValue = (ByteBuffer) value;
                }
                case INT -> {
                    if(value == null) {
                        indexOffset.intValue = null;
                    } else if(value instanceof Long) {
                        indexOffset.intValue = ((Long) value).intValue();
                    } else {
                        indexOffset.intValue = (Integer) value;
                    }
                }
                case LONG -> {
                    indexOffset.longValue = (Long) value;
                }
                case FLOAT -> {
                    indexOffset.floatValue = (Float) value;
                }
                case DOUBLE -> {
                    indexOffset.doubleValue = (Double) value;
                }
                case NULL, UNION -> {}
                default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
            }
            return indexOffset;
        }
    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexPosition {

        @Nullable
        private Boolean completed;
        @Nullable
        private Long count;

        @Nullable
        private Boolean isOpen;
        @Nullable
        private List<IndexOffset> offsets;

        public Boolean getCompleted() {
            return completed;
        }

        public void setCompleted(Boolean completed) {
            this.completed = completed;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Boolean getIsOpen() {
            return isOpen;
        }

        public void setIsOpen(Boolean isOpen) {
            this.isOpen = isOpen;
        }

        public List<IndexOffset> getOffsets() {
            return offsets;
        }

        public void setOffsets(List<IndexOffset> offsets) {
            this.offsets = offsets;
        }

        public boolean isOverTo(final IndexPosition another) {
            final int size = Math.min(this.getOffsets().size(), another.getOffsets().size());
            for(int i=0; i<size; i++) {
                final JdbcUtil.IndexOffset bound = another.getOffsets().get(i);
                if(bound.getValue() == null) {
                    return false;
                }
                final JdbcUtil.IndexOffset point = this.getOffsets().get(i);
                if(point.isLesserThan(bound)) {
                    return false;
                } else if(point.isGreaterThan(bound)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            final String o = this.offsets.stream()
                    .map(offset -> String.format("%s = %s",
                            offset.getFieldName() + (offset.getAscending() ? "" : "(DESC)"),
                            offset.valueToString()))
                    .collect(Collectors.joining(", "));

            return o + " [open=" + isOpen + "]";
        }

        public static IndexPosition of(final List<IndexOffset> offsets, final boolean isOpen) {
            if(offsets == null || offsets.size() == 0) {
                throw new IllegalArgumentException("offsets must not be null or zero size for IndexPosition");
            }
            final IndexPosition indexPosition = new IndexPosition();
            indexPosition.setCount(0L);
            indexPosition.setCompleted(false);
            indexPosition.setIsOpen(isOpen);
            indexPosition.setOffsets(offsets);
            return indexPosition;
        }

    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexRange {

        @Nullable
        private Long totalSize;
        @Nullable
        private Double ratio;

        @Nullable
        private IndexPosition from;
        @Nullable
        private IndexPosition to;

        public IndexRange() {

        }

        public Double getRatio() {
            return ratio;
        }

        public void setRatio(Double ratio) {
            this.ratio = ratio;
        }

        public IndexPosition getFrom() {
            return from;
        }

        public void setFrom(IndexPosition from) {
            this.from = from;
        }

        public IndexPosition getTo() {
            return to;
        }

        public void setTo(IndexPosition to) {
            this.to = to;
        }

        @Override
        public String toString() {
            return String.format("IndexRange(%f) From: %s -> To: %s%s",
                    this.ratio,
                    from.toString(),
                    to.toString(),
                    from.getCompleted() ? "  completed count: " + from.getCount() : "");
        }

        public static IndexRange of(IndexPosition from, IndexPosition to) {
            if(from == null || to == null) {
                throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
            }
            if(from.getOffsets() == null || to.getOffsets() == null) {
                throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
            }
            final IndexRange indexRange = new IndexRange();
            indexRange.setRatio(1.0D);
            indexRange.setFrom(from);
            indexRange.setTo(to);
            return indexRange;
        }

    }

    public static class CloseableDataSource implements DataSource, AutoCloseable {
        private final DataSource dataSource;

        public CloseableDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.dataSource.getParentLogger();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return this.dataSource.isWrapperFor(iface);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return this.dataSource.unwrap(iface);
        }

        @Override
        public void close() throws IOException {
            if(this.dataSource instanceof Closeable) {
                ((Closeable)this.dataSource).close();
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            return this.dataSource.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return this.dataSource.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.dataSource.getLogWriter();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return this.dataSource.getLoginTimeout();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            this.dataSource.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            this.dataSource.setLoginTimeout(seconds);
        }
    }
}
