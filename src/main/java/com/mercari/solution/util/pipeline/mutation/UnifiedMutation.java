package com.mercari.solution.util.pipeline.mutation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutation;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class UnifiedMutation implements Serializable {

    private static final List<String> mutationOpSymbols = MutationOp.symbols();
    private static final List<String> dataTypeSymbols = DataType.symbols();

    private final String table;
    private final Long commitTimestampMicros;
    private final Integer sequence;

    private final MutationOp op;

    private final DataType type;

    private final Object value;


    public UnifiedMutation(
            final DataType type,
            final MutationOp op,
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

    public static UnifiedMutation of(Mutation mutation, String table, long commitTimestampMicros, final Integer sequence) {
        final MutationOp op = switch (mutation.getOperation()) {
            case INSERT -> MutationOp.INSERT;
            case UPDATE -> MutationOp.UPDATE;
            case REPLACE -> MutationOp.REPLACE;
            case INSERT_OR_UPDATE -> MutationOp.UPSERT;
            case DELETE -> MutationOp.DELETE;
        };
        return new UnifiedMutation(DataType.MUTATION, op, table, commitTimestampMicros, sequence, mutation);
    }

    public static UnifiedMutation copy(final UnifiedMutation unifiedMutation, long commitTimestampMicros) {
        return new UnifiedMutation(unifiedMutation.getType(), unifiedMutation.getOp(), unifiedMutation.getTable(), commitTimestampMicros, unifiedMutation.getSequence(), unifiedMutation.getValue());
    }

    public String getTable() {
        return table;
    }

    public Long getCommitTimestampMicros() {
        return commitTimestampMicros;
    }

    public Integer getSequence() {
        return sequence;
    }

    public MutationOp getOp() {
        return op;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public Mutation getSpannerMutation() {
        if(!DataType.MUTATION.equals(this.type)) {
            throw new IllegalArgumentException("Not spanner mutation for type: " + this.type);
        }
        return (Mutation) value;
    }

    public RowMutation toRowMutation(final List<String> primaryKeyFields) {
        final TableRow tableRow = toTableRow(primaryKeyFields);
        final RowMutationInformation rowMutationInformation = toRowMutationInformation();
        return RowMutation.of(tableRow, rowMutationInformation);
    }

    public RowMutationInformation toRowMutationInformation() {
        final RowMutationInformation.MutationType mutationType = switch (this.op) {
            case INSERT, UPDATE, UPSERT -> RowMutationInformation.MutationType.UPSERT;
            case DELETE -> RowMutationInformation.MutationType.DELETE;
            default -> throw new IllegalArgumentException();
        };
        return RowMutationInformation.of(mutationType, this.commitTimestampMicros);
    }

    public TableRow toTableRow(final List<String> primaryKeyFields) {
        return switch (this.type) {
            case ROW -> RowToTableRowConverter.convert((Row) this.value);
            case AVRO -> AvroToTableRowConverter.convert((GenericRecord) this.value);
            case STRUCT -> StructToTableRowConverter.convert((Struct) this.value);
            case MUTATION -> MutationToTableRowConverter.convert((Mutation) this.value, primaryKeyFields);
            default -> throw new IllegalArgumentException();
        };
    }

    public static String getAsString(final UnifiedMutation unifiedMutation, final String field) {
        return "";
    }

    public GenericRecord toGenericRecord(final Schema schema) {
        return toGenericRecord(schema, this);
    }

    public static String toJson(UnifiedMutation unifiedMutation) {
        return toJson(unifiedMutation, null);
    }

    public static String toJson(UnifiedMutation unifiedMutation, List<String> fields) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("type", unifiedMutation.getType().name());
        jsonObject.addProperty("table", unifiedMutation.getTable());
        jsonObject.addProperty("op", unifiedMutation.getOp().name());
        jsonObject.addProperty("sequence", unifiedMutation.getSequence());
        jsonObject.addProperty("commitTimestamp", Instant.ofEpochMilli(unifiedMutation.getCommitTimestampMicros() / 1000).toString());
        jsonObject.add("value", convertMutationValueToJson(unifiedMutation));
        return jsonObject.toString();
    }

    public static GenericRecord toGenericRecord(final Schema schema, final UnifiedMutation unifiedMutation) {
        final JsonObject jsonObject = convertMutationValueToJson(unifiedMutation);
        return new GenericRecordBuilder(schema)
                .set("type", AvroSchemaUtil.createEnumSymbol("type", dataTypeSymbols, unifiedMutation.getType().name()))
                .set("table", unifiedMutation.getTable())
                .set("op", AvroSchemaUtil.createEnumSymbol("op", mutationOpSymbols, unifiedMutation.getOp().name()))
                .set("sequence", unifiedMutation.getSequence())
                .set("commitTimestamp", unifiedMutation.getCommitTimestampMicros())
                .set("value", jsonObject.toString())
                .build();
    }

    public static Row toRow(final org.apache.beam.sdk.schemas.Schema schema, final UnifiedMutation unifiedMutation) {
        final JsonObject jsonObject = convertMutationValueToJson(unifiedMutation);
        return Row.withSchema(schema)
                .withFieldValue("type", RowSchemaUtil.toEnumerationTypeValue(schema.getField("type").getType(), unifiedMutation.getType().name()))
                .withFieldValue("table", unifiedMutation.getTable())
                .withFieldValue("op", RowSchemaUtil.toEnumerationTypeValue(schema.getField("op").getType(), unifiedMutation.getOp().name()))
                .withFieldValue("sequence", unifiedMutation.getSequence())
                .withFieldValue("commitTimestamp", Instant.ofEpochMilli(unifiedMutation.getCommitTimestampMicros() / 1000L))
                .withFieldValue("value", jsonObject.toString())
                .build();
    }

    private static JsonObject convertMutationValueToJson(final UnifiedMutation unifiedMutation) {
        return switch (unifiedMutation.getType()) {
            case MUTATION -> MutationToJsonConverter.convert((Mutation) unifiedMutation.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static Schema createAvroSchema() {
        return SchemaBuilder.record("UnifiedMutation").fields()
                .name("type").type(Schema.createEnum("type", null, null, dataTypeSymbols)).noDefault()
                .name("table").type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))).noDefault()
                .name("op").type(Schema.createEnum("op", null, null, mutationOpSymbols)).noDefault()
                .name("sequence").type(Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))).withDefault(0)
                .name("commitTimestamp").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("value").type(SchemaBuilder.unionOf()
                        .stringBuilder().prop("sqlType", "JSON").endString().and()
                        .nullType()
                        .endUnion()).noDefault()
                .endRecord();
    }

    private static org.apache.beam.sdk.schemas.Schema createSchema() {
        return AvroToRowConverter.convertSchema(createAvroSchema());
    }

}
