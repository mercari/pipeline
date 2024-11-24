package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class RowToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RowToMutationConverter.class);

    private static final DateTimeFormatter FORMATTER_HH_MM_SS   = DateTimeFormat.forPattern("HH:mm:ss");

    public static Type convertSchema(final Schema schema) {
        return Type.struct(schema.getFields().stream()
                .map(f -> Type.StructField.of(f.getName(), convertFieldType(f.getType())))
                .collect(Collectors.toList()));
    }

    public static Mutation convert(final Row row, final String table, final String mutationOp) {
        return convert(null, row, table, mutationOp, null, null, null, null);
    }

    // For DataTypeTransform.SpannerMutationDoFn interface
    public static Mutation convert(final Schema schema,
                                   final Row row,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> commitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {

        if(mutationOp != null && "DELETE".equalsIgnoreCase(mutationOp.trim())) {
            if(keyFields == null) {
                throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
            }
            final Key key = createKey(row, keyFields);
            return Mutation.delete(table, key);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);
        for(final Schema.Field field : row.getSchema().getFields()) {
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }

            final boolean hide = hideFields != null && hideFields.contains(field.getName());
            final String fieldName = field.getName();
            final boolean isNullField = row.getValue(fieldName) == null;
            final boolean nullableField = field.getType().getNullable();
            final boolean isCommitTimestampField = commitTimestampFields != null && commitTimestampFields.contains(fieldName);
            switch(field.getType().getTypeName()) {
                case BOOLEAN:
                    final Boolean booleanValue = hide ? (nullableField ? null : false) : (isNullField ? null : row.getBoolean(fieldName));
                    builder.set(fieldName).to(booleanValue);
                    break;
                case STRING:
                    final String stringValue;
                    if(hide) {
                        if(field.getOptions().hasOption("sqlType")
                                && field.getOptions().getValue("sqlType").toString().startsWith("TIMESTAMP")) {
                            stringValue = nullableField ? null : "0001-01-01T00:00:00.00Z";
                        } else {
                            stringValue = nullableField ? null : "";
                        }
                    } else {
                        stringValue = isNullField ? null : row.getString(fieldName);
                    }
                    builder.set(fieldName).to(stringValue);
                    break;
                case BYTES:
                    final ByteArray bytesValue = hide ? (nullableField ? null : ByteArray.copyFrom("")) : (isNullField ? null : ByteArray.copyFrom(row.getBytes(fieldName)));
                    builder.set(fieldName).to(bytesValue);
                    break;
                case INT16:
                    final Short shortValue = hide ? (nullableField ? null : (short)0) : (isNullField ? null : row.getInt16(fieldName));
                    builder.set(fieldName).to(shortValue);
                    break;
                case INT32:
                    final Integer intValue = hide ? (nullableField ? null : 0) : (isNullField ? null : row.getInt32(fieldName));
                    builder.set(fieldName).to(intValue);
                    break;
                case INT64:
                    final Long longValue = hide ? (nullableField ? null : 0L) : (isNullField ? null : row.getInt64(fieldName));
                    builder.set(fieldName).to(longValue);
                    break;
                case FLOAT:
                    final Float floatValue = hide ? (nullableField ? null : 0F) : (isNullField ? null : row.getFloat(fieldName));
                    builder.set(fieldName).to(floatValue);
                    break;
                case DOUBLE:
                    final Double doubleValue = hide ? (nullableField ? null : 0D) : (isNullField ? null : row.getDouble(fieldName));
                    builder.set(fieldName).to(doubleValue);
                    break;
                case DECIMAL:
                    final BigDecimal decimalValue = hide ? (nullableField ? null : BigDecimal.ZERO) : (isNullField ? null : row.getDecimal(fieldName));
                    builder.set(fieldName).to(decimalValue);
                    break;
                case DATETIME: {
                    if(isCommitTimestampField) {
                        builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                    } else {
                        final ReadableDateTime datetimeValue = hide ?
                                (nullableField ? null : DateTime.now()) :
                                (isNullField ? null : row.getDateTime(fieldName));
                        final String allow_commit_timestamp = field.getOptions().hasOption("allow_commit_timestamp") ?
                                field.getOptions().getValue("allow_commit_timestamp") : null;
                        final Timestamp defaultTimestamp = "true".equalsIgnoreCase(allow_commit_timestamp) ? Value.COMMIT_TIMESTAMP : null;
                        builder.set(fieldName).to(datetimeValue == null ? defaultTimestamp : Timestamp
                                .parseTimestamp(datetimeValue
                                        .toDateTime()
                                        .toString(ISODateTimeFormat.dateTime())));
                    }
                    break;
                }
                case LOGICAL_TYPE:
                    if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                        if(hide) {
                            if(nullableField) {
                                builder.set(fieldName).to((Date)null);
                            } else {
                                builder.set(fieldName).to(Date.fromYearMonthDay(1970, 1, 1));
                            }
                        } else {
                            if(isNullField) {
                                builder.set(fieldName).to((Date)null);
                            } else {
                                final LocalDate localDate = row.getLogicalTypeValue(fieldName, LocalDate.class);
                                builder.set(fieldName).to(Date.fromYearMonthDay(
                                        localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
                            }
                        }
                    } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                        final String timeValue = hide ?
                                (nullableField ? null : "00:00:00") :
                                (isNullField ? null : row.getLogicalTypeValue(fieldName, Instant.class).toString(FORMATTER_HH_MM_SS));
                        builder.set(fieldName).to(timeValue);
                    } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                        if(isCommitTimestampField) {
                            builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                        } else {
                            final String datetimeStrValue = hide ?
                                    (nullableField ? null : "1970-01-01T00:00:00.00Z") :
                                    (isNullField ? null : row.getDateTime(fieldName)
                                            .toDateTime()
                                            .toString(ISODateTimeFormat.dateTime()));
                            builder.set(fieldName).to(datetimeStrValue);
                        }
                    } else if(RowSchemaUtil.isLogicalTypeDateTime(field.getType())) {
                        if(isCommitTimestampField) {
                            builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                        } else {
                            final Timestamp datetime;
                            if(isNullField) {
                                if(nullableField) {
                                    datetime = null;
                                } else {
                                    datetime = Timestamp.ofTimeMicroseconds(0L);
                                }
                            } else {
                                final LocalDateTime localDateTime = row.getLogicalTypeValue(fieldName, LocalDateTime.class);
                                datetime = Timestamp.ofTimeSecondsAndNanos(localDateTime.getSecond(), localDateTime.getNano());
                            }
                            builder.set(fieldName).to(datetime);
                        }
                    } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                        final String timeValue = hide ?
                                (nullableField ? null : field.getType().getLogicalType(EnumerationType.class).getValues().get(0)) :
                                (isNullField ? null : RowSchemaUtil.toString(field.getType(),row.getLogicalTypeValue(fieldName, EnumerationType.Value.class)));
                        builder.set(fieldName).to(timeValue);
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                    }
                    break;
                case ROW:
                    // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                    // https://cloud.google.com/spanner/docs/data-types
                    break;
                case ITERABLE:
                case ARRAY:
                    switch (field.getType().getCollectionElementType().getTypeName()) {
                        case BOOLEAN:
                            if(hide) {
                                builder.set(fieldName).toBoolArray(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toBoolArray(isNullField ? null : row.getArray(fieldName));
                            }
                            break;
                        case STRING:
                            if(hide) {
                                builder.set(fieldName).toStringArray(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toStringArray(isNullField ? null : row.getArray(fieldName));
                            }
                            break;
                        case BYTES:
                            if(hide) {
                                builder.set(fieldName).toBytesArray(new ArrayList<>());
                            } else {
                                builder.set(fieldName)
                                        .toBytesArray(isNullField ? null : row.<byte[]>getArray(fieldName).stream()
                                                .map(ByteArray::copyFrom)
                                                .collect(Collectors.toList()));
                            }
                            break;
                        case INT16:
                        case INT32:
                        case INT64:
                            if(hide) {
                                builder.set(fieldName).toInt64Array(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toInt64Array(isNullField ? null : row.getArray(fieldName));
                            }
                            break;
                        case FLOAT:
                        case DOUBLE:
                            if(hide) {
                                builder.set(fieldName).toFloat64Array(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toFloat64Array(isNullField ? null : row.getArray(fieldName));
                            }
                            break;
                        case DECIMAL: {
                            if(hide) {
                                builder.set(fieldName).toNumericArray(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toNumericArray(isNullField ? null : row.getArray(fieldName));
                            }
                            break;
                        }
                        case DATETIME:
                            if(hide) {
                                builder.set(fieldName).toTimestampArray(new ArrayList<>());
                            } else {
                                builder.set(fieldName).toTimestampArray(isNullField ? null : row.<Instant>getArray(fieldName).stream()
                                        .filter(Objects::nonNull)
                                        .map(i -> i.toDateTime().toString(ISODateTimeFormat.dateTime()))
                                        .map(Timestamp::parseTimestamp)
                                        .collect(Collectors.toList()));
                            }
                            break;
                        case LOGICAL_TYPE:
                            if(RowSchemaUtil.isLogicalTypeDate(field.getType().getCollectionElementType())) {
                                if(hide) {
                                    builder.set(fieldName).toDateArray(new ArrayList<>());
                                } else {
                                    builder.set(fieldName)
                                            .toDateArray(isNullField ? null : row.<LocalDate>getArray(fieldName).stream()
                                                    .filter(Objects::nonNull)
                                                    .map(ld -> Date.fromYearMonthDay(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()))
                                                    .collect(Collectors.toList()));
                                }
                            } else if(RowSchemaUtil.isLogicalTypeTime(field.getType().getCollectionElementType())) {
                                if(hide) {
                                    builder.set(fieldName).toStringArray(new ArrayList<>());
                                } else {
                                    builder.set(fieldName)
                                            .toStringArray(isNullField ? null : row.<Instant>getArray(fieldName).stream()
                                                    .filter(Objects::nonNull)
                                                    .map(i -> i.toString(FORMATTER_HH_MM_SS))
                                                    .collect(Collectors.toList()));
                                }
                            } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType().getCollectionElementType())) {
                                if(hide) {
                                    builder.set(fieldName).toTimestampArray(new ArrayList<>());
                                } else {
                                    builder.set(fieldName).toTimestampArray(isNullField ? null : row.<Instant>getArray(fieldName).stream()
                                            .filter(Objects::nonNull)
                                            .map(i -> i.toDateTime().toString(ISODateTimeFormat.dateTime()))
                                            .map(Timestamp::parseTimestamp)
                                            .collect(Collectors.toList()));
                                }
                            } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType().getCollectionElementType())) {
                                if(hide) {
                                    builder.set(fieldName).toStringArray(new ArrayList<>());
                                } else {
                                    builder.set(fieldName)
                                            .toStringArray(isNullField ? null : row.<EnumerationType.Value>getArray(fieldName).stream()
                                                    .filter(Objects::nonNull)
                                                    .map(v -> RowSchemaUtil.toString(field.getType().getCollectionElementType(), v))
                                                    .collect(Collectors.toList()));
                                }
                            } else {
                                throw new IllegalArgumentException(
                                        "Unsupported Beam logical type: "
                                                + fieldName + "/"
                                                + field.getType().getCollectionElementType().getLogicalType() + "/"
                                                + field.getType().getCollectionElementType());

                            }
                            break;
                        case ROW:
                            // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            break;
                        case ITERABLE:
                        case ARRAY:
                            // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            break;
                        case BYTE:
                        case MAP:
                        default:
                            break;
                    }
                    break;
                case BYTE:
                case MAP:
                default:
                    break;

            }
        }

        if(commitTimestampFields != null) {
            for(final String commitTimestampField : commitTimestampFields) {
                if(!schema.hasField(commitTimestampField)) {
                    builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);
                }
            }
        }

        return builder.build();
    }

    public static MutationGroup convertGroup(final Schema schema, final Row row, final String mutationOp, final String primaryField) {
        Mutation primary = null;
        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : row.getSchema().getFields()) {
            final String fieldName = field.getName();
            if(row.getValue(fieldName) == null) {
                continue;
            }
            switch (field.getType().getTypeName()) {
                case BOOLEAN:
                case STRING:
                case DECIMAL:
                case BYTES:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT:
                case DOUBLE:
                case DATETIME:
                case LOGICAL_TYPE:
                case MAP:
                case BYTE:
                    break;
                case ROW:
                    final Mutation mutation = convert(schema, row, fieldName, mutationOp,null, null, null, null);
                    if(fieldName.equals(primaryField)) {
                        primary = mutation;
                    } else {
                        mutations.add(mutation);
                    }
                    break;
                case ITERABLE:
                case ARRAY: {
                    if (!Schema.TypeName.ROW.equals(field.getType().getCollectionElementType().getTypeName())) {
                        break;
                    }
                    final List<Mutation> mutationArray = row.<Row>getArray(fieldName).stream()
                            .map(r -> convert(r.getSchema(), r, fieldName, mutationOp,null, null, null, null))
                            .collect(Collectors.toList());
                    if(mutationArray.size() == 0) {
                        break;
                    }
                    if(fieldName.equals(primaryField)) {
                        primary = mutationArray.get(0);
                        mutations.addAll(mutationArray.subList(1, mutationArray.size()));
                    } else {
                        mutations.addAll(mutationArray);
                    }
                    break;
                }
                default:
                    break;
            }

        }
        if(primary == null) {
            return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
        }
        return MutationGroup.create(primary, mutations);
    }

    public static Type convertFieldType(final Schema.FieldType fieldType) {
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Type.bool();
            case BYTES:
                return Type.bytes();
            case DECIMAL:
                return Type.numeric();
            case STRING:
                return Type.string();
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
                return Type.int64();
            case FLOAT:
            case DOUBLE:
                return Type.float64();
            case DATETIME:
                return Type.timestamp();
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return Type.date();
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return Type.string();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Type.timestamp();
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    return Type.timestamp();
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return Type.string();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case ROW:
                return Type.struct(fieldType.getRowSchema().getFields().stream()
                        .map(f -> Type.StructField.of(f.getName(), convertFieldType(f.getType())))
                        .collect(Collectors.toList()));
            case ITERABLE:
            case ARRAY:
                return Type.array(convertFieldType(fieldType.getCollectionElementType()));
            case MAP:
            default:
                throw new IllegalArgumentException("Not supported fieldType: " + fieldType.getTypeName());
        }
    }

    public static Key createKey(final Row row, final Iterable<String> keyFields) {
        Key.Builder keyBuilder = Key.newBuilder();
        for(final String keyField : keyFields) {
            final Schema.Field field = row.getSchema().getField(keyField);
            switch (field.getType().getTypeName()) {
                case BOOLEAN:
                    keyBuilder = keyBuilder.append(row.getBoolean(keyField));
                    break;
                case STRING:
                    keyBuilder = keyBuilder.append(row.getString(keyField));
                    break;
                case BYTE: {
                    final Long longValue = Optional.ofNullable(row.getByte(keyField)).map(Byte::longValue).orElse(null);
                    keyBuilder = keyBuilder.append(longValue);
                    break;
                }
                case INT16: {
                    final Long longValue = Optional.ofNullable(row.getInt16(keyField)).map(Short::longValue).orElse(null);
                    keyBuilder = keyBuilder.append(longValue);
                    break;
                }
                case INT32: {
                    final Long longValue = Optional.ofNullable(row.getInt32(keyField)).map(Integer::longValue).orElse(null);
                    keyBuilder = keyBuilder.append(longValue);
                    break;
                }
                case INT64:
                    keyBuilder = keyBuilder.append(row.getInt64(keyField));
                    break;
                case FLOAT: {
                    final Double doubleValue = Optional.ofNullable(row.getFloat(keyField)).map(Float::doubleValue).orElse(null);
                    keyBuilder = keyBuilder.append(doubleValue);
                    break;
                }
                case DOUBLE:
                    keyBuilder = keyBuilder.append(row.getDouble(keyField));
                    break;
                case BYTES: {
                    final ByteArray bytes = Optional.ofNullable(row.getBytes(keyField)).map(ByteArray::copyFrom).orElse(null);
                    keyBuilder = keyBuilder.append(bytes);
                    break;
                }
                case DECIMAL:
                    keyBuilder = keyBuilder.append(row.getDecimal(keyField));
                    break;
                case DATETIME: {
                    final Timestamp timestamp = Optional.ofNullable(row.getDateTime(keyField))
                            .map(i -> Timestamp.ofTimeMicroseconds(i.getMillis() * 1000L))
                            .orElse(null);
                    keyBuilder = keyBuilder.append(timestamp);
                    break;

                }
                case LOGICAL_TYPE: {
                    if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                        final Date date = Optional.ofNullable(row.getLogicalTypeValue(keyField, LocalDate.class))
                                .map(i -> Date.fromYearMonthDay(i.getYear(), i.getMonthValue(), i.getDayOfMonth()))
                                .orElse(null);
                        keyBuilder = keyBuilder.append(date);
                        break;
                    } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                        final String time = Optional.ofNullable(row.getLogicalTypeValue(keyField, LocalTime.class))
                                .map(LocalTime::toString)
                                .orElse(null);
                        keyBuilder = keyBuilder.append(time);
                        break;
                    } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                        final Timestamp timestamp = Optional.ofNullable(row.getLogicalTypeValue(keyField, Instant.class))
                                .map(i -> Timestamp.ofTimeMicroseconds(i.getMillis() * 1000L))
                                .orElse(null);
                        keyBuilder = keyBuilder.append(timestamp);
                        break;
                    } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                        final String enumValue = Optional.ofNullable(row.getLogicalTypeValue(keyField, EnumerationType.Value.class))
                                .map(v  -> RowSchemaUtil.toString(field.getType(), v))
                                .orElse(null);
                        keyBuilder = keyBuilder.append(enumValue);
                        break;
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported Beam logical type: "
                                        + keyField + "/"
                                        + field.getType().getCollectionElementType().getLogicalType() + "/"
                                        + field.getType().getCollectionElementType());
                    }
                }
                case ROW:
                case ARRAY:
                case ITERABLE:
                case MAP:
                default: {
                    throw new IllegalStateException();
                }
            }
        }
        return keyBuilder.build();
    }

}
