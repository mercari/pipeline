/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.*;
import com.google.gson.JsonArray;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Converter converts Cloud Spanner Struct to Cloud Spanner Mutation
 */
public class StructToMutationConverter {

    private static final String DEFAULT_PREFIX = "autoprefix";

    private StructToMutationConverter() {}

    public static Mutation convert(final Struct struct,
                                   final String table, final String mutationOp, final Iterable<String> keyFields) {

        return convert(struct, table, mutationOp, keyFields, null, null, null, DEFAULT_PREFIX);
    }

    public static Mutation convert(final Struct struct,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {

        return convert(struct, table, mutationOp, keyFields, null, excludeFields, hideFields, DEFAULT_PREFIX);
    }

    // For DataTypeTransform.SpannerMutationDoFn interface
    public static Mutation convert(final Schema schema,
                                   final Struct struct,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> allowCommitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {

        return convert(struct, table, mutationOp, keyFields, allowCommitTimestampFields, excludeFields, hideFields, DEFAULT_PREFIX);
    }

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object.
     *
     * @param struct Spanner Struct to be converted to Mutation object.
     * @param table Spanner table name to store.
     * @param mutationOp Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE.
     * @return Spanner Mutation object.
     */
    public static Mutation convert(final Struct struct,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> commitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields,
                                   final String fieldPrefix) {

        if(mutationOp != null && "DELETE".equalsIgnoreCase(mutationOp.trim())) {
            return delete(struct, table, keyFields);
        }

        Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if (excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }

            final boolean isNullField = struct.isNull(field.getName());
            final boolean isCommitTimestampField = commitTimestampFields != null && commitTimestampFields.contains(field.getName());
            final String fieldName;
            if (field.getName().startsWith("_")) {
                fieldName = fieldPrefix + field.getName();
            } else {
                fieldName = field.getName();
            }
            builder = switch (field.getType().getCode()) {
                case STRING -> builder.set(fieldName).to(isNullField ? null : struct.getString(field.getName()));
                case JSON -> builder.set(fieldName).to(isNullField ? null : struct.getJson(field.getName()));
                case PG_NUMERIC -> builder.set(fieldName).to(isNullField ? null : struct.getString(field.getName()));
                case PG_JSONB -> builder.set(fieldName).to(isNullField ? null : struct.getPgJsonb(field.getName()));
                case BYTES -> builder.set(fieldName).to(isNullField ? null : struct.getBytes(field.getName()));
                case BOOL -> builder.set(fieldName).to(isNullField ? null : struct.getBoolean(field.getName()));
                case INT64 -> builder.set(fieldName).to(isNullField ? null : struct.getLong(field.getName()));
                case FLOAT32 -> builder.set(fieldName).to(isNullField ? null : struct.getFloat(field.getName()));
                case FLOAT64 -> builder.set(fieldName).to(isNullField ? null : struct.getDouble(field.getName()));
                case NUMERIC -> builder.set(fieldName).to(isNullField ? null : struct.getBigDecimal(field.getName()));
                case DATE -> builder.set(fieldName).to(isNullField ? null : struct.getDate(field.getName()));
                case TIMESTAMP -> {
                    if (isCommitTimestampField) {
                        yield builder.set(fieldName).to(Value.COMMIT_TIMESTAMP);
                    } else {
                        yield builder.set(fieldName).to(isNullField ? null : struct.getTimestamp(field.getName()));
                    }
                }
                case STRUCT -> {
                    // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                    // https://cloud.google.com/spanner/docs/data-types
                    if (isNullField) {
                        yield builder.set(fieldName).to((String) null);
                    } else {
                        Struct child = struct.getStruct(field.getName());
                        final String json = StructToJsonConverter.convert(child);
                        yield builder.set(fieldName).to(json);
                    }
                }
                case ARRAY -> switch (field.getType().getArrayElementType().getCode()) {
                    case STRING ->
                            builder.set(fieldName).toStringArray(isNullField ? null : struct.getStringList(field.getName()));
                    case JSON ->
                            builder.set(fieldName).toJsonArray(isNullField ? null : struct.getJsonList(field.getName()));
                    case PG_JSONB ->
                            builder.set(fieldName).toPgJsonbArray(isNullField ? null : struct.getPgJsonbList(field.getName()));
                    case PG_NUMERIC ->
                            builder.set(fieldName).toPgNumericArray(isNullField ? null : struct.getStringList(field.getName()));
                    case BYTES ->
                            builder.set(fieldName).toBytesArray(isNullField ? null : struct.getBytesList(field.getName()));
                    case BOOL ->
                            builder.set(fieldName).toBoolArray(isNullField ? null : struct.getBooleanArray(field.getName()));
                    case INT64 ->
                            builder.set(fieldName).toInt64Array(isNullField ? null : struct.getLongArray(field.getName()));
                    case FLOAT32 ->
                            builder.set(fieldName).toFloat32Array(isNullField ? null : struct.getFloatList(field.getName()));
                    case FLOAT64 ->
                            builder.set(fieldName).toFloat64Array(isNullField ? null : struct.getDoubleList(field.getName()));
                    case NUMERIC ->
                            builder.set(fieldName).toNumericArray(isNullField ? null : struct.getBigDecimalList(field.getName()));
                    case DATE ->
                            builder.set(fieldName).toDateArray(isNullField ? null : struct.getDateList(field.getName()));
                    case TIMESTAMP ->
                            builder.set(fieldName).toTimestampArray(isNullField ? null : struct.getTimestampList(field.getName()));
                    case STRUCT -> {
                        // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                        if (isNullField) {
                            yield builder.set(fieldName).to((String) null);
                        } else {
                            List<Struct> children = struct.getStructList(field.getName());
                            final JsonArray array = new JsonArray();
                            children.stream()
                                    .map(StructToJsonConverter::convertObject)
                                    .forEach(array::add);
                            yield builder.set(fieldName).to(array.toString());
                        }
                    }
                    case ARRAY -> {
                        // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                        throw new IllegalArgumentException();
                    }
                    default -> throw new IllegalArgumentException();

                };
                default -> throw new IllegalArgumentException();
            };
        }

        if(commitTimestampFields != null) {
            for(final String commitTimestampField : commitTimestampFields) {
                if(!StructSchemaUtil.hasField(struct, commitTimestampField)) {
                    builder = builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);
                }
            }
        }

        return builder.build();
    }

    public static MutationGroup convertGroup(final Type type, final Struct struct, final String mutationOp, final String primaryField) {
        return convertGroup(struct, mutationOp, primaryField);
    }

    private static MutationGroup convertGroup(final Struct struct, final String mutationOp, final String primaryField) {
        Mutation primary = null;
        final List<Mutation> mutations = new ArrayList<>();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            if(struct.isNull(fieldName)) {
                continue;
            }
            switch (field.getType().getCode()) {
                case STRUCT -> {
                    final Mutation mutation = convert(struct, fieldName, mutationOp, null, null, null);
                    if (fieldName.equals(primaryField)) {
                        primary = mutation;
                    } else {
                        mutations.add(mutation);
                    }
                }
                case ARRAY -> {
                    if (!Type.Code.STRUCT.equals(field.getType().getArrayElementType().getCode())) {
                        break;
                    }
                    final List<Mutation> mutationArray = struct.getStructList(fieldName).stream()
                            .map(s -> convert(s, fieldName, mutationOp, null, null, null))
                            .toList();
                    if(mutationArray.isEmpty()) {
                        break;
                    }
                    if(fieldName.equals(primaryField)) {
                        primary = mutationArray.get(0);
                        mutations.addAll(mutationArray.subList(1, mutationArray.size()));
                    } else {
                        mutations.addAll(mutationArray);
                    }
                }
            }
        }
        if(primary == null) {
            return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
        }
        return MutationGroup.create(primary, mutations);
    }

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object to delete.
     *
     * @param struct Spanner Struct to be deleted.
     * @param table Spanner table name to delete specified struct object.
     * @param keyFields Key fields in the struct. If composite key case, set comma-separated fields in key sequence.
     * @return Spanner delete Mutation object.
     */
    private static Mutation delete(final Struct struct, final String table, final Iterable<String> keyFields) {
        Key.Builder builder = Key.newBuilder();
        for(final String keyField : keyFields) {
            if(struct.isNull(keyField)) {
                builder = builder.appendObject(null);
                continue;
            }
            builder = switch(struct.getColumnType(keyField).getCode()) {
                case STRING -> builder.append(struct.getString(keyField));
                case JSON -> builder.append(struct.getJson(keyField));
                case BYTES -> builder.append(struct.getBytes(keyField));
                case BOOL -> builder.append(struct.getBoolean(keyField));
                case INT64 -> builder.append(struct.getLong(keyField));
                case FLOAT32 -> builder.append(struct.getFloat(keyField));
                case FLOAT64 -> builder.append(struct.getDouble(keyField));
                case NUMERIC -> builder.append(struct.getBigDecimal(keyField));
                case DATE -> builder.append(struct.getDate(keyField));
                case TIMESTAMP -> builder.append(struct.getTimestamp(keyField));
                default -> throw new IllegalArgumentException(String.format(
                            "field: %s, fieldType: %s at table %s, is impossible as Key.",
                            keyField, struct.getColumnType(keyField).toString(), table));
            };
        }
        return Mutation.delete(table, builder.build());
    }

}