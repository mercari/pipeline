package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutationToTableRowConverter {

    public static TableRow convert(final Mutation mutation) {
        if(Mutation.Op.DELETE.equals(mutation.getOperation())) {
            throw new IllegalArgumentException("Not supported");
        }
        return convert(mutation.asMap());
    }

    public static TableRow convert(final Mutation mutation, final List<String> primaryKeyFields) {
        if(Mutation.Op.DELETE.equals(mutation.getOperation())) {
            final List<Object> values = new ArrayList<>();
            for(final Key key : mutation.getKeySet().getKeys()) {
                for(final Object object : key.getParts()) {
                    values.add(object);
                }
            }
            final TableRow tableRow = new TableRow();
            for(int index=0; index<primaryKeyFields.size(); index++) {
                final String primaryKeyField = primaryKeyFields.get(index);
                final Object value = values.get(index);
                tableRow.set(primaryKeyField, convertTableRowValue(value));
            }
            return tableRow;
        } else {
            return convert(mutation.asMap());
        }
    }

    public static TableRow convertMutationRecord(final Mutation mutation) {
        return new TableRow()
                .set("table", mutation.getTable())
                .set("op", mutation.getOperation().name())
                .set("timestamp", Instant.now().toString())
                .set("keys", StructSchemaUtil.getKeys(mutation))
                .set("mutation", MutationToJsonConverter.convertJsonString(mutation));
    }

    private static TableRow convert(final Map<String, Value> values) {
        final TableRow tableRow = new TableRow();
        for(final Map.Entry<String, Value> entry : values.entrySet()) {
            final Value value = entry.getValue();
            tableRow.set(entry.getKey(), convertTableRowValue(value));
        }
        return tableRow;
    }

    private static Object convertTableRowValue(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRING -> value.getString();
            case BOOL -> value.getBool();
            case INT64 -> value.getInt64();
            case FLOAT64 -> value.getFloat64();
            case DATE -> value.getDate().toString();
            case TIMESTAMP -> value.getTimestamp().toString();
            case JSON -> value.getJson();
            case PG_JSONB -> value.getPgJsonb();
            case BYTES -> value.getBytes();
            case NUMERIC -> value.getNumeric().toString();
            case STRUCT -> {
                final Struct struct = value.getStruct();
                final Map<String, Value> values = new HashMap<>();
                for(Type.StructField field : struct.getType().getStructFields()) {
                    final Value fieldValue = StructSchemaUtil.getStructValue(struct, field.getName());
                    values.put(field.getName(), fieldValue);
                }
                yield convert(values);
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static Object convertTableRowValue(final Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof String
                || value instanceof Long
                || value instanceof Double
                || value instanceof Boolean) {
            return value;
        } else if(value instanceof Date date) {
            return date.toString();
        } else if(value instanceof Timestamp timestamp) {
            return timestamp.toString();
        } else {
            throw new IllegalArgumentException();
        }
    }

}
