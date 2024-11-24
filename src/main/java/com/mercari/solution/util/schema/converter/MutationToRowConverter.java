package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.HashMap;

public class MutationToRowConverter {

    public static Row convert(final Schema schema, final Mutation mutation) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final Object value = convertRowValue(mutation.asMap().get(field.getName()));
            builder.withFieldValue(field.getName(), value);
        }
        return builder.build();
    }

    public static Row convertMutationRecord(final Schema schema, final Mutation mutation) {
        return Row.withSchema(schema)
                .withFieldValue("table", mutation.getTable())
                .withFieldValue("op", RowSchemaUtil.toEnumerationTypeValue(schema.getField("op").getType(), mutation.getOperation().name()))
                .withFieldValue("timestamp", Instant.now())
                .withFieldValue("keys", StructSchemaUtil.getKeys(mutation))
                .withFieldValue("mutation", MutationToJsonConverter.convertJsonString(mutation))
                .build();
    }

    private static Object convertRowValue(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRING -> value.getString();
            case BOOL -> value.getBool();
            case INT64 -> value.getInt64();
            case FLOAT64 -> value.getFloat64();
            case DATE -> LocalDate.of(value.getDate().getYear(), value.getDate().getMonth(), value.getDate().getDayOfMonth());
            case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
            case JSON -> value.getJson();
            case BYTES -> ByteBuffer.wrap(value.getBytes().toByteArray());
            case NUMERIC -> value.getNumeric().toString();
            case STRUCT -> {
                final Struct struct = value.getStruct();
                yield StructToRowConverter.convert(StructToRowConverter.convertSchema(struct.getType()), struct);
            }
            default -> throw new IllegalArgumentException();
        };
    }
}
