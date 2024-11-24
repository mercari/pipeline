package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.StructSchemaUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DocumentToMutationConverter {

    public static Mutation convert(final Type type,
                                   final Document entity,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> commitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {
        if(entity == null) {
            throw new RuntimeException("entity must not be null! ");
        }
        if(type == null) {
            throw new RuntimeException("schema must not be null! " + entity);
        }

        if(mutationOp != null && "DELETE".equalsIgnoreCase(mutationOp.trim())) {
            if(keyFields == null) {
                throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
            }
            final com.google.cloud.spanner.Key key = createKey(entity, keyFields);
            return Mutation.delete(table, key);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);

        for(final Type.StructField field : type.getStructFields()) {
            if(field.getName().equals("__key__") || (excludeFields != null && excludeFields.contains(field.getName()))) {
                continue;
            }
            final Value value = entity.getFieldsOrDefault(field.getName(), null);
            final boolean isCommitTimestampField = commitTimestampFields != null && commitTimestampFields.contains(field.getName());
            setDocumentValue(builder, field.getName(), value, field.getType(), isCommitTimestampField);
        }

        if(commitTimestampFields != null) {
            for(final String commitTimestampField : commitTimestampFields) {
                if(!StructSchemaUtil.hasField(type, commitTimestampField)) {
                    builder.set(commitTimestampField).to(com.google.cloud.spanner.Value.COMMIT_TIMESTAMP);
                }
            }
        }

        return builder.build();
    }

    private static void setDocumentValue(final Mutation.WriteBuilder builder, final String fieldName, final Value value, final Type type, final boolean isCommitTimestampField) {
        if (value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {

            if(Type.Code.ARRAY.equals(type.getCode())) {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL -> builder.set(fieldName).toBoolArray(new ArrayList<>());
                    case STRING -> builder.set(fieldName).toStringArray(new ArrayList<>());
                    case BYTES -> builder.set(fieldName).toBytesArray(new ArrayList<>());
                    case INT64 -> builder.set(fieldName).toInt64Array(new ArrayList<>());
                    case FLOAT64 -> builder.set(fieldName).toFloat64Array(new ArrayList<>());
                    case NUMERIC -> builder.set(fieldName).toNumericArray(new ArrayList<>());
                    case PG_NUMERIC -> builder.set(fieldName).toPgNumericArray(new ArrayList<>());
                    case JSON -> builder.set(fieldName).toJsonArray(new ArrayList<>());
                    case PG_JSONB -> builder.set(fieldName).toPgJsonbArray(new ArrayList<>());
                    case DATE -> builder.set(fieldName).toDateArray(new ArrayList<>());
                    case TIMESTAMP -> builder.set(fieldName).toTimestampArray(new ArrayList<>());
                    case STRUCT -> builder.set(fieldName).toStructArray(type.getArrayElementType(), new ArrayList<>());
                    case ARRAY, UNRECOGNIZED -> throw new IllegalStateException();
                }
            }
            return;
        }

        switch (type.getCode()) {
            case JSON,
                    PG_JSONB,
                    PG_NUMERIC,
                    STRING -> builder.set(fieldName).to(value.getStringValue());
            case BYTES -> builder.set(fieldName).to(ByteArray.copyFrom(value.getBytesValue().toByteArray()));
            case BOOL -> builder.set(fieldName).to(value.getBooleanValue());
            case INT64 -> builder.set(fieldName).to(value.getIntegerValue());
            case FLOAT64 -> builder.set(fieldName).to(value.getDoubleValue());
            case NUMERIC -> {
                final BigDecimal decimal = BigDecimal.valueOf(value.getDoubleValue());
                builder.set(fieldName).to(decimal);
            }
            case DATE -> builder.set(fieldName).to(Date.parseDate(value.getStringValue()));
            case TIMESTAMP -> {
                if (isCommitTimestampField) {
                    builder.set(fieldName).to(com.google.cloud.spanner.Value.COMMIT_TIMESTAMP);
                } else {
                    builder.set(fieldName).to(Timestamp.fromProto(value.getTimestampValue()));
                }
            }
            case ARRAY -> {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL -> {
                        builder.set(fieldName).toBoolArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getBooleanValue)
                                .collect(Collectors.toList()));
                    }
                    case BYTES -> {
                        builder.set(fieldName).toBytesArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getBytesValue)
                                .map(ByteString::toByteArray)
                                .map(ByteArray::copyFrom)
                                .collect(Collectors.toList()));
                    }
                    case STRING -> {
                        builder.set(fieldName).toStringArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                    }
                    case JSON -> {
                        builder.set(fieldName).toJsonArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                    }
                    case PG_JSONB -> {
                        builder.set(fieldName).toPgJsonbArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                    }
                    case PG_NUMERIC -> {
                        builder.set(fieldName).toPgNumericArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                    }
                    case INT64 -> {
                        builder.set(fieldName).toInt64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getIntegerValue)
                                .collect(Collectors.toList()));
                    }
                    case FLOAT64 -> {
                        builder.set(fieldName).toFloat64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getDoubleValue)
                                .collect(Collectors.toList()));
                    }
                    case NUMERIC -> {
                        builder.set(fieldName).toNumericArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getDoubleValue)
                                .map(BigDecimal::valueOf)
                                .collect(Collectors.toList()));
                    }
                    case DATE -> {
                        builder.set(fieldName).toDateArray(value.getArrayValue().getValuesList().stream()
                                .map(e -> Date.parseDate(e.getStringValue()))
                                .toList());
                    }
                    case TIMESTAMP -> {
                        builder.set(fieldName).toTimestampArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getTimestampValue)
                                .map(Timestamp::fromProto)
                                .collect(Collectors.toList()));
                    }
                    default -> {
                    }
                }
            }
            default -> {
            }
        }
    }

    public static com.google.cloud.spanner.Key createKey(final Document entity, final Iterable<String> keyFields) {
        com.google.cloud.spanner.Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
        for(final String keyField : keyFields) {
            final Value value = entity.getFieldsOrDefault(keyField, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            switch (value.getValueTypeCase()) {
                case BOOLEAN_VALUE ->
                        keyBuilder = keyBuilder.append(value.getBooleanValue());
                case STRING_VALUE ->
                        keyBuilder = keyBuilder.append(value.getStringValue());
                case INTEGER_VALUE ->
                        keyBuilder = keyBuilder.append(value.getIntegerValue());
                case DOUBLE_VALUE ->
                        keyBuilder = keyBuilder.append(value.getDoubleValue());
                case BYTES_VALUE ->
                        keyBuilder = keyBuilder.append(ByteArray.copyFrom(value.getBytesValue().toByteArray()));
                case TIMESTAMP_VALUE -> {
                    Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(value.getTimestampValue().getSeconds(), value.getTimestampValue().getNanos());
                    keyBuilder = keyBuilder.append(timestamp);
                }
                case MAP_VALUE,
                        ARRAY_VALUE,
                        GEO_POINT_VALUE,
                        NULL_VALUE,
                        VALUETYPE_NOT_SET ->
                        keyBuilder = keyBuilder.appendObject(null);
                default -> throw new IllegalStateException();
            }
        }
        return keyBuilder.build();
    }

}
