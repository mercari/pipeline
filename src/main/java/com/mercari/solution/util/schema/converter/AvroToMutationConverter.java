package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class AvroToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToMutationConverter.class);

    public static Type convertSchema(final Schema schema) {
        return convertFieldType(schema);
    }

    public static Mutation convert(final Schema schema,
                                   final GenericRecord record,
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
            final Key key = createKey(record, keyFields);
            return Mutation.delete(table, key);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);
        for(final Schema.Field field : schema.getFields()) {
            if(excludeFields != null && excludeFields.contains(field.name())) {
                continue;
            }
            final boolean hide = hideFields != null && hideFields.contains(field.name());
            final String fieldName = field.name();
            final Object value = record.hasField(fieldName) ? record.get(fieldName) : null;
            final boolean isCommitTimestampField = commitTimestampFields != null && commitTimestampFields.contains(fieldName);
            setValue(builder, fieldName, field.schema(), value, hide, false, isCommitTimestampField);
        }

        if(commitTimestampFields != null) {
            for(final String commitTimestampField : commitTimestampFields) {
                if(schema.getField(commitTimestampField) == null) {
                    builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);
                }
            }
        }
        return builder.build();
    }

    public static MutationGroup convertGroup(final Schema schema, final GenericRecord record, final String mutationOp, final String primaryField) {
        Mutation primary = null;
        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : record.getSchema().getFields()) {
            final String fieldName = field.name();
            if(!record.hasField(fieldName)) {
                continue;
            }
            if(record.get(fieldName) == null) {
                continue;
            }
            switch (field.schema().getType()) {
                case RECORD -> {
                    final Mutation mutation = convert(schema, record, fieldName, mutationOp, null, null, null, null);
                    if (field.name().equals(primaryField)) {
                        primary = mutation;
                    } else {
                        mutations.add(mutation);
                    }
                }
                case ARRAY -> {
                    if (!Schema.Type.RECORD.equals(field.schema().getElementType().getType())) {
                        break;
                    }
                    final List<Mutation> mutationArray = ((List<GenericRecord>)record.get(fieldName)).stream()
                            .map(r -> convert(r.getSchema(), r, fieldName, mutationOp, null, null, null, null))
                            .collect(Collectors.toList());
                    if(mutationArray.isEmpty()) {
                        break;
                    }
                    if(field.name().equals(primaryField)) {
                        primary = mutationArray.get(0);
                        //mutations.addAll(mutationArray.subList(1, mutationArray.size()));
                        mutations.addAll(mutationArray);
                    } else {
                        mutations.addAll(mutationArray);
                    }
                }
            }

        }
        if(primary == null) {
            return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
        }
        LOG.error("{} : {}", primary, mutations.size());
        return MutationGroup.create(primary, mutations);
    }

    public static Key createKey(final GenericRecord record, final Iterable<String> keyFields) {
        Key.Builder keyBuilder = Key.newBuilder();
        for(final String keyField : keyFields) {
            final Schema.Field field = record.getSchema().getField(keyField);
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            final Object fieldValue = record.hasField(keyField) ? record.get(keyField) : null;
            switch (fieldSchema.getType()) {
                case BOOLEAN -> keyBuilder = keyBuilder.append((Boolean)fieldValue);
                case ENUM, STRING -> keyBuilder = keyBuilder.append(fieldValue == null ? null : fieldValue.toString());
                case FLOAT -> keyBuilder = keyBuilder.append((Float)fieldValue);
                case DOUBLE -> keyBuilder = keyBuilder.append((Double)fieldValue);
                case FIXED, BYTES -> {
                    if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                        if(fieldValue == null) {
                            keyBuilder = keyBuilder.append((BigDecimal) null);
                        } else {
                            final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(fieldSchema, (ByteBuffer) fieldValue);
                            keyBuilder = keyBuilder.append(decimal);
                        }
                    } else {
                        if(fieldValue == null) {
                            keyBuilder = keyBuilder.append((ByteArray) null);

                        } else {
                            final ByteArray bytes = ByteArray.copyFrom(((ByteBuffer) fieldValue).array());
                            keyBuilder = keyBuilder.append(bytes);
                        }
                    }
                }
                case INT -> {
                    final Integer intValue = (Integer)fieldValue;
                    if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                        final LocalDate ld = LocalDate.ofEpochDay(intValue);
                        final Date date = Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
                        keyBuilder = keyBuilder.append(date);
                    } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                        final String timeText = LocalTime
                                .ofNanoOfDay((long) intValue * 1000 * 1000)
                                .format(DateTimeFormatter.ISO_LOCAL_TIME);
                        keyBuilder = keyBuilder.append(timeText);
                    } else {
                        keyBuilder = keyBuilder.append(intValue);
                    }
                }
                case LONG -> {
                    final Long longValue = (Long)fieldValue;
                    if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                        final Timestamp timestampValue = Timestamp.ofTimeMicroseconds(longValue * 1000);
                        keyBuilder = keyBuilder.append(timestampValue);
                    } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                        final Timestamp timestampValue = Timestamp.ofTimeMicroseconds(longValue);
                        keyBuilder = keyBuilder.append(timestampValue);
                    } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                        final String timeText = LocalTime
                                .ofNanoOfDay(longValue * 1000)
                                .format(DateTimeFormatter.ISO_LOCAL_TIME);
                        keyBuilder = keyBuilder.append(timeText);
                    } else {
                        keyBuilder = keyBuilder.append(longValue);
                    }
                }
                default -> throw new IllegalStateException();
            }
        }
        return keyBuilder.build();
    }

    public static Map<String, Value> convertValues(final Schema schema, final GenericRecord record) {
        final Map<String, Value> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            if(record == null || !record.hasField(field.name())) {
                values.put(field.name(), convertValue(field.schema(), null));
            } else {
                values.put(field.name(), convertValue(field.schema(), record.get(field.name())));
            }
        }
        return values;
    }

    private static void setValue(final Mutation.WriteBuilder builder,
                                 final String fieldName,
                                 final Schema schema,
                                 final Object value,
                                 final boolean hide,
                                 final boolean nullableField,
                                 final boolean isCommitTimestampField) {

        final boolean isNullField = value == null;
        switch(schema.getType()) {
            case BOOLEAN -> {
                final Boolean booleanValue = hide ? (nullableField ? null : false) : (Boolean) value;
                builder.set(fieldName).to(booleanValue);
            }
            case ENUM, STRING -> {
                final String stringValue = hide ?
                        (nullableField ? null : "") :
                        (isNullField ? null : value.toString());
                final String sqlType = schema.getProp("sqlType");
                if("DATETIME".equalsIgnoreCase(sqlType)) {
                    builder.set(fieldName).to(stringValue);
                } else if("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
                    builder.set(fieldName).to(stringValue);
                } else if("JSON".equalsIgnoreCase(sqlType)) {
                    builder.set(fieldName).to(stringValue);
                } else {
                    builder.set(fieldName).to(stringValue);
                }
            }
            case FIXED, BYTES -> {
                final ByteArray bytesValue = hide ?
                        (nullableField ? null : ByteArray.copyFrom("")) :
                        (isNullField ? null : ByteArray.copyFrom(((ByteBuffer) value).array()));
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final BigDecimal decimal;
                    if(bytesValue == null) {
                        decimal = null;
                    } else {
                        decimal = AvroSchemaUtil.getAsBigDecimal(schema, bytesValue.toByteArray());
                    }
                    builder.set(fieldName).to(decimal);
                } else {
                    builder.set(fieldName).to(bytesValue);
                }
            }
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    final Date dateValue = hide ?
                            (nullableField ? null : Date.fromYearMonthDay(1970, 1, 1)) :
                            convertEpochDaysToDate((Integer) value);
                    builder.set(fieldName).to(dateValue);
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final String timeValue = hide ?
                            (nullableField ? null : "00:00:00") :
                            (isNullField ? null : LocalTime.ofNanoOfDay(Long.valueOf((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                    builder.set(fieldName).to(timeValue);
                } else {
                    final Long intValue = hide ?
                            (nullableField ? null : 0L) :
                            (isNullField ? null : Long.valueOf((Integer) value));
                    builder.set(fieldName).to(intValue);
                }
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    if (isCommitTimestampField) {
                        builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                    } else {
                        final Timestamp timestampValue = hide ? (nullableField ? null : Timestamp.MIN_VALUE)
                                : isNullField ? null : convertMicrosecToTimestamp(longValue * 1000);
                        builder.set(fieldName).to(timestampValue);
                    }
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    if (isCommitTimestampField) {
                        builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                    } else {
                        final Timestamp timestampValue = hide ? (nullableField ? null : Timestamp.MIN_VALUE)
                                : isNullField ? null : convertMicrosecToTimestamp(longValue);
                        builder.set(fieldName).to(timestampValue);
                    }
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    final String timeValue = hide ? (nullableField ? null : "00:00:00")
                            : isNullField ? null : convertNanosecToTimeString(longValue * 1000);
                    builder.set(fieldName).to(timeValue);
                } else {
                    builder.set(fieldName).to(hide ? (nullableField ? null : 0L) : longValue);
                }
            }
            case FLOAT -> {
                final Float floatValue = hide ?
                        (nullableField ? null : 0F) :
                        (isNullField ? null : (Float) value);
                builder.set(fieldName).to(floatValue);
            }
            case DOUBLE -> {
                final Double doubleValue = hide ? (nullableField ? null : 0D) : (Double) value;
                builder.set(fieldName).to(doubleValue);
            }
            case RECORD -> {
                // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                // https://cloud.google.com/spanner/docs/data-types
            }
            case UNION -> {
                final boolean nullable = schema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(Schema.Type.NULL));
                final Schema unnested = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                setValue(builder, fieldName, unnested, value, hide, nullable, isCommitTimestampField);
            }
            case ARRAY -> {
                final List list = new ArrayList();
                final Schema elementSchema = AvroSchemaUtil.unnestUnion(schema.getElementType());
                switch (elementSchema.getType()) {
                    case BOOLEAN -> {
                        final List<Boolean> booleanList = hide || isNullField ? list :
                                ((List<Boolean>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toBoolArray(booleanList);
                    }
                    case ENUM, STRING -> {
                        final List<String> stringList = hide || isNullField ? list :
                                ((List<Object>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(Object::toString)
                                        .collect(Collectors.toList());
                        if(AvroSchemaUtil.isSqlTypeJson(elementSchema)) {
                            builder.set(fieldName).toJsonArray(stringList);
                        } else {
                            builder.set(fieldName).toStringArray(stringList);
                        }
                    }
                    case FIXED, BYTES -> {
                        if(AvroSchemaUtil.isLogicalTypeDecimal(elementSchema)) {
                            final List<BigDecimal> decimalList = hide || isNullField ? list :
                                    ((List<ByteBuffer>) value).stream()
                                            .filter(Objects::nonNull)
                                            .map(bytes -> AvroSchemaUtil.getAsBigDecimal(elementSchema, bytes))
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toNumericArray(decimalList);
                        } else {
                            final List<ByteArray> bytesList = hide || isNullField ? list :
                                    ((List<ByteBuffer>) value).stream()
                                            .filter(Objects::nonNull)
                                            .map(ByteBuffer::array)
                                            .map(ByteArray::copyFrom)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toBytesArray(bytesList);
                        }
                    }
                    case INT -> {
                        final List<Integer> intList = ((List<Integer>) value);
                        if (LogicalTypes.date().equals(elementSchema.getLogicalType())) {
                            final List<Date> dateList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(AvroToMutationConverter::convertEpochDaysToDate)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toDateArray(dateList);
                        } else if (LogicalTypes.timeMillis().equals(elementSchema.getLogicalType())) {
                            final List<String> timeList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(Long::valueOf)
                                            .map(l -> l * 1000 * 1000)
                                            .map(LocalTime::ofNanoOfDay)
                                            .map(l -> l.format(DateTimeFormatter.ISO_LOCAL_TIME))
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toStringArray(timeList);
                        } else {
                            final List<Long> integerList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(Long::valueOf)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toInt64Array(integerList);
                        }
                    }
                    case LONG -> {
                        final List<Long> longList = (List<Long>) value;
                        if (LogicalTypes.timestampMillis().equals(elementSchema.getLogicalType())) {
                            final List<Timestamp> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(l -> l * 1000)
                                            .map(AvroToMutationConverter::convertMicrosecToTimestamp)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toTimestampArray(timestampList);
                        } else if (LogicalTypes.timestampMicros().equals(elementSchema.getLogicalType())) {
                            final List<Timestamp> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(AvroToMutationConverter::convertMicrosecToTimestamp)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toTimestampArray(timestampList);
                        } else if (LogicalTypes.timeMicros().equals(elementSchema.getLogicalType())) {
                            final List<String> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(l -> l * 1000)
                                            .map(AvroToMutationConverter::convertNanosecToTimeString)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toStringArray(timestampList);
                        } else {
                            final List<Long> longLists = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toInt64Array(longLists);
                        }
                    }
                    case FLOAT -> {
                        final List<Float> floatList = hide || isNullField ? list :
                                ((List<Float>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toFloat32Array(floatList);
                    }
                    case DOUBLE -> {
                        final List<Double> doubleList = hide || isNullField ? list :
                                ((List<Double>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toFloat64Array(doubleList);
                    }
                    case RECORD -> {
                        // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                    }
                    case ARRAY -> {
                        // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                    }
                }
            }
        }
    }

    private static Type convertFieldType(final Schema schema) {
        return switch (schema.getType()) {
            case BOOLEAN -> Type.bool();
            case ENUM -> Type.string();
            case STRING -> {
                final String sqlType = schema.getProp("sqlType");
                if ("DATETIME".equalsIgnoreCase(sqlType)) {
                    yield Type.string();
                } else if("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
                    yield Type.string();
                } else if("JSON".equalsIgnoreCase(sqlType)) {
                    yield Type.json();
                }
                yield Type.string();
            }
            case FIXED, BYTES -> {
                if (AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    yield Type.numeric();
                }
                yield Type.bytes();
            }
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    yield Type.date();
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    yield Type.string();
                }
                yield Type.int64();
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    yield Type.timestamp();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    yield Type.timestamp();
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    yield Type.string();
                }
                yield Type.int64();
            }
            case FLOAT -> Type.float32();
            case DOUBLE -> Type.float64();
            case RECORD -> Type.struct(schema.getFields().stream()
                        .map(f -> Type.StructField.of(f.name(), convertFieldType(f.schema())))
                        .collect(Collectors.toList()));
            case ARRAY -> Type.array(convertFieldType(schema.getElementType()));
            case UNION -> {
                final boolean nullable = schema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
                final org.apache.avro.Schema unnested = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                yield convertFieldType(unnested);
            }
            default -> Type.string();
        };

    }

    private static Value convertValue(final Schema fieldSchema, final Object object) {
        final boolean isNull = object == null;
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> Value.bool((Boolean)object);
            case ENUM, STRING -> {
                final String stringValue = isNull ? null : object.toString();
                final String sqlType = fieldSchema.getProp("sqlType");
                if("DATETIME".equalsIgnoreCase(sqlType)) {
                    yield Value.timestamp(isNull ? null : Timestamp.parseTimestamp(stringValue));
                } else if("JSON".equalsIgnoreCase(sqlType)) {
                    yield Value.json(stringValue);
                } else if("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
                    yield Value.string(stringValue);
                } else {
                    yield Value.string(stringValue);
                }
            }
            case FIXED, BYTES -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                    if(isNull) {
                        yield Value.numeric(null);
                    } else {
                        final ByteArray bytesValue = ByteArray.copyFrom(((ByteBuffer) object).array());
                        final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(fieldSchema, bytesValue.toByteArray());
                        yield Value.numeric(decimal);
                    }
                } else {
                    if(isNull) {
                        yield Value.bytes(null);
                    } else {
                        final ByteArray bytesValue = ByteArray.copyFrom(((ByteBuffer) object).array());
                        yield Value.bytes(bytesValue);
                    }
                }
            }
            case FLOAT -> Value.float32((Float) object);
            case DOUBLE -> Value.float64((Double) object);
            case INT -> {
                final Integer intValue = (Integer) object;
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    yield Value.date(isNull ? null : convertEpochDaysToDate(intValue));
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    yield Value.string(isNull ? null : LocalTime.ofNanoOfDay(((intValue) * 1000 * 1000)).format(DateTimeFormatter.ISO_LOCAL_TIME));
                } else {
                    yield Value.int64(isNull ? null : ((Integer) object).longValue());
                }
            }
            case LONG -> {
                final Long longValue = (Long)object;
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    yield Value.timestamp(isNull ? null : convertMicrosecToTimestamp(longValue * 1000));
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    yield Value.timestamp(isNull ? null : convertMicrosecToTimestamp(longValue));
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    yield Value.string(isNull ? null : convertNanosecToTimeString(longValue * 1000));
                } else {
                    yield Value.int64(longValue);
                }
            }
            case ARRAY -> {
                final Schema elementSchema = AvroSchemaUtil.unnestUnion(fieldSchema.getElementType());
                yield switch (elementSchema.getType()) {
                    case BOOLEAN -> Value.boolArray(isNull ? new ArrayList<>() : (List<Boolean>)object);
                    case ENUM, STRING -> {
                        final String sqlType = elementSchema.getProp("sqlType");
                        if("DATETIME".equalsIgnoreCase(sqlType)) {
                            yield Value.timestampArray(isNull ? new ArrayList<>() : ((List<Object>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> Timestamp.parseTimestamp(o.toString()))
                                    .collect(Collectors.toList()));
                        } else if("JSON".equalsIgnoreCase(sqlType)) {
                            yield Value.jsonArray(isNull ? new ArrayList<>() : ((List<Object>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .collect(Collectors.toList()));
                        } else if("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
                            yield Value.stringArray(isNull ? new ArrayList<>() : ((List<Object>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .collect(Collectors.toList()));
                        } else {
                            yield Value.stringArray(isNull ? new ArrayList<>() : ((List<Object>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .collect(Collectors.toList()));
                        }
                    }
                    case FIXED, BYTES -> {
                        if(AvroSchemaUtil.isLogicalTypeDecimal(elementSchema)) {
                            yield Value.numericArray(isNull ? new ArrayList<>() : ((List<ByteBuffer>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(ByteArray::copyFrom)
                                    .map(b -> AvroSchemaUtil.getAsBigDecimal(elementSchema, b.toByteArray()))
                                    .collect(Collectors.toList()));
                        } else {
                            yield Value.bytesArray(isNull ? new ArrayList<>() : ((List<ByteBuffer>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(ByteArray::copyFrom)
                                    .collect(Collectors.toList()));
                        }
                    }
                    case FLOAT -> Value.float32Array(isNull ? new ArrayList<>() : ((List<Float>)object)
                                .stream()
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));
                    case DOUBLE -> Value.float64Array(isNull ? new ArrayList<>() : ((List<Double>)object)
                                .stream()
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));
                    case INT -> {
                        if(LogicalTypes.date().equals(elementSchema.getLogicalType())) {
                            yield Value.dateArray(isNull ? new ArrayList<>() : ((List<Integer>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(AvroToMutationConverter::convertEpochDaysToDate)
                                    .collect(Collectors.toList()));
                        } else if(LogicalTypes.timeMillis().equals(elementSchema.getLogicalType())) {
                            yield Value.stringArray(isNull ? new ArrayList<>() : ((List<Integer>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(i -> LocalTime.ofNanoOfDay(((i) * 1000 * 1000)).format(DateTimeFormatter.ISO_LOCAL_TIME))
                                    .collect(Collectors.toList()));
                        } else {
                            yield Value.int64Array(isNull ? new ArrayList<>() : ((List<Integer>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(Integer::longValue)
                                    .collect(Collectors.toList()));
                        }
                    }
                    case LONG -> {
                        if(LogicalTypes.timestampMillis().equals(elementSchema.getLogicalType())) {
                            yield Value.timestampArray(isNull ? new ArrayList<>() : ((List<Long>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(l -> convertMicrosecToTimestamp(l * 1000))
                                    .collect(Collectors.toList()));
                        } else if(LogicalTypes.timestampMicros().equals(elementSchema.getLogicalType())) {
                            yield Value.timestampArray(isNull ? new ArrayList<>() : ((List<Long>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(AvroToMutationConverter::convertMicrosecToTimestamp)
                                    .collect(Collectors.toList()));
                        } else if(LogicalTypes.timeMicros().equals(elementSchema.getLogicalType())) {
                            yield Value.stringArray(isNull ? new ArrayList<>() : ((List<Long>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .map(l -> convertNanosecToTimeString(l * 1000))
                                    .collect(Collectors.toList()));
                        } else {
                            yield Value.int64Array(isNull ? new ArrayList<>() : ((List<Long>)object)
                                    .stream()
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()));
                        }
                    }
                    default -> throw new IllegalStateException("Not supported array field schema: " + elementSchema);

                };
            }
            case UNION -> convertValue(AvroSchemaUtil.unnestUnion(fieldSchema), object);
            case NULL -> throw new IllegalArgumentException("Not supported field null");
            default -> throw new IllegalArgumentException("Not supported field schema: " + fieldSchema);
        };
    }

    private static Date convertEpochDaysToDate(final Integer epochDays) {
        if(epochDays == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(epochDays);
        return Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
    }

    private static String convertNanosecToTimeString(final Long nanos) {
        if(nanos == null) {
            return null;
        }
        final LocalTime localTime = LocalTime.ofNanoOfDay(nanos);
        return localTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    private static Timestamp convertMicrosecToTimestamp(final Long micros) {
        if(micros == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(micros);
    }

    private static String convertNumericBytesToString(final byte[] bytes, final int scale) {
        if(bytes == null) {
            return null;
        }
        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        final StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}
