package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EntityToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(EntityToMutationConverter.class);

    private EntityToMutationConverter() {}

    public static Mutation convert(final Type type,
                                   final Entity entity,
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
            throw new RuntimeException("schema must not be null! " + entity.getKey().toString());
        }

        if(mutationOp != null && "DELETE".equalsIgnoreCase(mutationOp.trim())) {
            if(keyFields == null) {
                throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
            }
            final com.google.cloud.spanner.Key key = createKey(entity, keyFields);
            return Mutation.delete(table, key);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);

        final Key.PathElement pe = entity.getKey().getPath(entity.getKey().getPathCount()-1);
        if(pe.getName() == null) {
            builder.set("__key__").to(pe.getName());
        } else {
            builder.set("__key__").to(pe.getId());
        }

        for(final Type.StructField field : type.getStructFields()) {
            if(field.getName().equals("__key__") || (excludeFields != null && excludeFields.contains(field.getName()))) {
                continue;
            }
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
            final boolean isCommitTimestampField = commitTimestampFields != null && commitTimestampFields.contains(field.getName());
            setEntityValue(builder, field.getName(), value, field.getType(), isCommitTimestampField);
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

    private static void setEntityValue(final Mutation.WriteBuilder builder, final String fieldName, final Value value, final Type type, final boolean isCommitTimestampField) {
        if (value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {

            if(Type.Code.ARRAY.equals(type.getCode())) {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL:
                        builder.set(fieldName).toBoolArray(new ArrayList<>());
                        break;
                    case STRING:
                        builder.set(fieldName).toStringArray(new ArrayList<>());
                        break;
                    case BYTES:
                        builder.set(fieldName).toBytesArray(new ArrayList<>());
                        break;
                    case INT64:
                        builder.set(fieldName).toInt64Array(new ArrayList<>());
                        break;
                    case FLOAT64:
                        builder.set(fieldName).toFloat64Array(new ArrayList<>());
                        break;
                    case NUMERIC:
                        builder.set(fieldName).toNumericArray(new ArrayList<>());
                        break;
                    case PG_NUMERIC:
                        builder.set(fieldName).toPgNumericArray(new ArrayList<>());
                        break;
                    case JSON:
                        builder.set(fieldName).toJsonArray(new ArrayList<>());
                        break;
                    case PG_JSONB:
                        builder.set(fieldName).toPgJsonbArray(new ArrayList<>());
                        break;
                    case DATE:
                        builder.set(fieldName).toDateArray(new ArrayList<>());
                        break;
                    case TIMESTAMP:
                        builder.set(fieldName).toTimestampArray(new ArrayList<>());
                        break;
                    case STRUCT:
                        builder.set(fieldName).toStructArray(type.getArrayElementType(), new ArrayList<>());
                        break;
                    case ARRAY:
                    case UNRECOGNIZED:
                        throw new IllegalStateException();
                }
            }
            return;
        }

        switch (type.getCode()) {
            case JSON:
            case PG_JSONB:
            case PG_NUMERIC:
            case STRING:
                builder.set(fieldName).to(value.getStringValue());
                return;
            case BYTES:
                builder.set(fieldName).to(ByteArray.copyFrom(value.getBlobValue().toByteArray()));
                return;
            case BOOL:
                builder.set(fieldName).to(value.getBooleanValue());
                return;
            case INT64:
                builder.set(fieldName).to(value.getIntegerValue());
                return;
            case FLOAT64:
                builder.set(fieldName).to(value.getDoubleValue());
                return;
            case NUMERIC: {
                final BigDecimal decimal = BigDecimal.valueOf(value.getDoubleValue());
                builder.set(fieldName).to(decimal);
                return;
            }
            case DATE:
                builder.set(fieldName).to(EntitySchemaUtil.convertDate(value));
                return;
            case TIMESTAMP:
                if(isCommitTimestampField) {
                    builder.set(fieldName).to(com.google.cloud.spanner.Value.COMMIT_TIMESTAMP);
                } else {
                    builder.set(fieldName).to(com.google.cloud.Timestamp.fromProto(value.getTimestampValue()));
                }
                return;
            case ARRAY: {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL:
                        builder.set(fieldName).toBoolArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getBooleanValue)
                                .collect(Collectors.toList()));
                        return;
                    case BYTES:
                        builder.set(fieldName).toBytesArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getBlobValue)
                                .map(ByteString::toByteArray)
                                .map(ByteArray::copyFrom)
                                .collect(Collectors.toList()));
                        return;
                    case STRING:
                        builder.set(fieldName).toStringArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                        return;
                    case JSON:
                        builder.set(fieldName).toJsonArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                        return;
                    case PG_JSONB:
                        builder.set(fieldName).toPgJsonbArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                        return;
                    case PG_NUMERIC:
                        builder.set(fieldName).toPgNumericArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                        return;
                    case INT64:
                        builder.set(fieldName).toInt64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getIntegerValue)
                                .collect(Collectors.toList()));
                        return;
                    case FLOAT64:
                        builder.set(fieldName).toFloat64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getDoubleValue)
                                .collect(Collectors.toList()));
                        return;
                    case NUMERIC:
                        builder.set(fieldName).toNumericArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getDoubleValue)
                                .map(BigDecimal::valueOf)
                                .collect(Collectors.toList()));
                        return;
                    case DATE:
                        builder.set(fieldName).toDateArray(value.getArrayValue().getValuesList().stream()
                                .map(EntitySchemaUtil::convertDate)
                                .collect(Collectors.toList()));
                        return;
                    case TIMESTAMP:
                        builder.set(fieldName).toTimestampArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getTimestampValue)
                                .map(Timestamp::fromProto)
                                .collect(Collectors.toList()));
                        return;
                    case STRUCT:
                    case ARRAY:
                    default:
                        return;
                }
            }
            case STRUCT:
            default:
        }
    }

    public static com.google.cloud.spanner.Key createKey(final Entity entity, final Iterable<String> keyFields) {
        com.google.cloud.spanner.Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
        for(final String keyField : keyFields) {
            final Value value = entity.getPropertiesOrDefault(keyField, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            switch (value.getValueTypeCase()) {
                case BOOLEAN_VALUE:
                    keyBuilder = keyBuilder.append(value.getBooleanValue());
                    break;
                case STRING_VALUE:
                    keyBuilder = keyBuilder.append(value.getStringValue());
                    break;
                case INTEGER_VALUE:
                    keyBuilder = keyBuilder.append(value.getIntegerValue());
                    break;
                case DOUBLE_VALUE:
                    keyBuilder = keyBuilder.append(value.getDoubleValue());
                    break;
                case BLOB_VALUE:
                    keyBuilder = keyBuilder.append(ByteArray.copyFrom(value.getBlobValue().toByteArray()));
                    break;
                case TIMESTAMP_VALUE: {
                    Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(value.getTimestampValue().getSeconds(), value.getTimestampValue().getNanos());
                    keyBuilder = keyBuilder.append(timestamp);
                    break;
                }
                case KEY_VALUE: {
                    for(final Key.PathElement pathElement : value.getKeyValue().getPathList()) {
                        if(pathElement.hasId()) {
                            keyBuilder = keyBuilder.append(pathElement.getId());
                        } else if(pathElement.hasName()) {
                            keyBuilder = keyBuilder.append(pathElement.getName());
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    break;
                }
                case ENTITY_VALUE:
                case ARRAY_VALUE:
                case GEO_POINT_VALUE:
                case NULL_VALUE:
                case VALUETYPE_NOT_SET:
                    keyBuilder = keyBuilder.appendObject(null);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        return keyBuilder.build();
    }

}
