package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.util.List;
import java.util.Map;

public class ElementToSpannerMutationConverter {

    public static Mutation convert(
            final Schema schema,
            final MElement element,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final List<String> allowCommitTimestampFields) {

        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String, Object>) element.getValue(),
                    table, mutationOp,
                    keyFields, allowCommitTimestampFields);
            case AVRO -> AvroToMutationConverter
                    .convert(schema.getAvroSchema(), (GenericRecord) element.getValue(),
                            table, mutationOp,
                            keyFields, allowCommitTimestampFields, null, null);
            case ROW -> RowToMutationConverter
                    .convert(schema.getRowSchema(), (Row) element.getValue(),
                            table, mutationOp, keyFields,
                            allowCommitTimestampFields, null, null);
            case STRUCT -> StructToMutationConverter
                    .convert(schema.getRowSchema(), (Struct) element.getValue(),
                            table, mutationOp,
                            keyFields, allowCommitTimestampFields, null, null);
            default -> throw new IllegalArgumentException();
        };
    }

    private static Mutation convert(
            final Schema schema,
            final Map<String, Object> primitiveValues,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final List<String> allowCommitTimestampFields) {

        throw new IllegalStateException("Not supported element type: " + schema);
    }

}
