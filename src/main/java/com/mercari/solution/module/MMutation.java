package com.mercari.solution.module;

import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.converter.*;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MMutation {

    private static final List<String> opSymbols = Op.symbols();
    private static final List<String> dataTypeSymbols = DataType.symbols();

    private final String table;
    private final Long commitTimestampMicros;
    private final Integer sequence;

    private final Op op;

    private final DataType type;

    private final Object value;

    public String getTable() {
        return table;
    }

    public Long getCommitTimestampMicros() {
        return commitTimestampMicros;
    }

    public Integer getSequence() {
        return sequence;
    }

    public Op getOp() {
        return op;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public MMutation(
            final DataType type,
            final Op op,
            final String table,
            final Long commitTimestampMicros,
            final Integer sequence,
            final Object value) {

        this.type = type;
        this.op = op;
        this.table = Optional.ofNullable(table).orElse("");
        this.commitTimestampMicros = commitTimestampMicros;
        this.sequence = Optional.ofNullable(sequence).orElse(-1);
        this.value = value;
    }

    public static MMutation of(Mutation mutation, String table, long commitTimestampMicros, final Integer sequence) {
        final Op op = switch (mutation.getOperation()) {
            case INSERT -> Op.INSERT;
            case UPDATE -> Op.UPDATE;
            case REPLACE -> Op.REPLACE;
            case INSERT_OR_UPDATE -> Op.UPSERT;
            case DELETE -> Op.DELETE;
        };
        return new MMutation(DataType.MUTATION, op, table, commitTimestampMicros, sequence, mutation);
    }

    public static Schema schema() {
        return Schema.builder()
                .withField("type", Schema.FieldType.enumeration(dataTypeSymbols))
                .withField("table", Schema.FieldType.STRING)
                .withField("op", Schema.FieldType.enumeration(opSymbols))
                .withField("sequence", Schema.FieldType.INT64)
                .withField("commitTimestamp", Schema.FieldType.TIMESTAMP)
                .withField("value", Schema.FieldType.JSON)
                .build();
    }

    public static MMutation copy(final MMutation mutation, long commitTimestampMicros) {
        return new MMutation(mutation.getType(), mutation.getOp(), mutation.getTable(), commitTimestampMicros, mutation.getSequence(), mutation.getValue());
    }


    public Mutation getSpannerMutation() {
        if(!DataType.MUTATION.equals(this.type)) {
            throw new IllegalArgumentException("Not spanner mutation for type: " + this.type);
        }
        return (Mutation) value;
    }

    public static String toJson(MMutation mutation) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("type", mutation.getType().name());
        jsonObject.addProperty("table", mutation.getTable());
        jsonObject.addProperty("op", mutation.getOp().name());
        jsonObject.addProperty("sequence", mutation.getSequence());
        jsonObject.addProperty("commitTimestamp", Instant.ofEpochMilli(mutation.getCommitTimestampMicros() / 1000).toString());
        jsonObject.add("value", convertMutationValueToJson(mutation));
        return jsonObject.toString();
    }


    private static JsonObject convertMutationValueToJson(final MMutation mutation) {
        return switch (mutation.getType()) {
            case MUTATION -> MutationToJsonConverter.convert((Mutation) mutation.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public enum Op implements Serializable {

        INSERT(1),
        UPDATE(2),
        REPLACE(3),
        UPSERT(4),
        DELETE(5);

        private final int id;


        Op(final int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static Op of(final int id) {
            for(final Op op : values()) {
                if(op.id == id) {
                    return op;
                }
            }
            throw new IllegalArgumentException("No such enum object for MutationOp id: " + id);
        }

        public static List<String> symbols() {
            final List<String> symbols = new ArrayList<>();
            for(final Op op : values()) {
                symbols.add(op.name());
            }
            return symbols;
        }

    }

}
