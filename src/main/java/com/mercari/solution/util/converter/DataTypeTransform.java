package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.List;
import java.util.Set;


public class DataTypeTransform {

    public static <OutputT> TypeTransform<OutputT> transform(
            final FCollection<?> input,
            final DataType outputType) {

        return new TypeTransform<>(input, outputType);
    }

    public static PTransform<PCollection<?>, PCollection<Mutation>> spannerMutation(
            final FCollection<?> input,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final Set<String> excludeFields,
            final Set<String> maskFields) {

        return new TypeTransform<>(
                input, DataType.MUTATION, table, keyFields,
                mutationOp,
                excludeFields, maskFields);
    }

    public static PTransform<PCollection<?>, PCollection<KV<String, ?>>> withKeys(
            final FCollection<?> inputCollection, final List<String> keyFields) {
        return new WithKeyTransform(inputCollection, keyFields);
    }

    public static <InputT> PTransform<PCollection<InputT>, PCollection<InputT>> withTimestamp(
            final DataType dataType, final String timestampAttribute, final String timestampDefault) {

        if(timestampDefault == null) {
            return new WithTimestampTransform<>(dataType, timestampAttribute, Instant.ofEpochSecond(0L));
        }

        try {
            return new WithTimestampTransform<>(dataType, timestampAttribute, Instant.parse(timestampDefault));
        } catch (Exception e) {
            return new WithTimestampTransform<>(dataType, timestampAttribute, Instant.ofEpochSecond(0L));
        }
    }

    public static class TypeTransform<OutputT> extends PTransform<PCollection<?>, PCollection<OutputT>> {

        private final FCollection<?> inputCollection;
        private final DataType outputType;

        private final String destination;
        private final List<String> keyFields;

        private final String spannerMutationOp;

        private final Set<String> excludeFields;
        private final Set<String> maskFields;

        private FCollection<OutputT> outputCollection;

        public FCollection<OutputT> getOutputCollection() {
            return outputCollection;
        }

        private TypeTransform(final FCollection<?> collection, final DataType outputType) {
            this(collection, outputType, null, null,
                    null,
                    null, null);
        }

        private TypeTransform(final FCollection<?> collection,
                              final DataType outputType,
                              final String destination,
                              final List<String> keyFields,
                              final String spannerMutationOp,
                              final Set<String> excludeFields,
                              final Set<String> maskFields) {

            this.inputCollection = collection;
            this.outputType = outputType;

            this.destination = destination;
            this.keyFields = keyFields;

            this.spannerMutationOp = spannerMutationOp;

            this.excludeFields = excludeFields;
            this.maskFields = maskFields;
        }

        @Override
        public PCollection<OutputT> expand(final PCollection<?> input) {
            final DataType inputType = inputCollection.getDataType();
            if(inputType.equals(outputType)) {
                this.outputCollection = (FCollection<OutputT>) inputCollection;
                return (PCollection<OutputT>)input;
            }

            final PCollection<OutputT> output;
            switch (inputType) {
                case AVRO: {
                    final PCollection<GenericRecord> inputAvro = (PCollection<GenericRecord>) input;
                    final String inputAvroSchema = inputCollection.getAvroSchema().toString();
                    switch (outputType) {
                        case ROW: {
                            final Schema schema = AvroToRowConverter.convertSchema(inputAvroSchema);
                            output = (PCollection<OutputT>) inputAvro
                                    .apply("RecordToRow", ParDo
                                            .of(new RowDoFn<>(inputAvroSchema,
                                                    inputCollection.getSchema(),
                                                    AvroSchemaUtil::convertSchema,
                                                    AvroToRowConverter::convert)))
                                    .setCoder(RowCoder.of(schema))
                                    .setRowSchema(schema);
                            this.outputCollection = FCollection.of(name, output, outputType, schema);
                            return output;
                        }
                        case MUTATION: {
                            output = (PCollection<OutputT>) inputAvro
                                    .apply("RecordToMutation", ParDo.of(new SpannerMutationDoFn<>(
                                            destination, spannerMutationOp, keyFields, null, excludeFields, maskFields,
                                            inputAvroSchema,
                                            AvroSchemaUtil::convertSchema,
                                            AvroToMutationConverter::convert)))
                                    .setCoder(SerializableCoder.of(Mutation.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getAvroSchema());
                            return output;
                        }
                        case ENTITY: {
                            output = (PCollection<OutputT>) inputAvro
                                    .apply("RecordToEntity", ParDo.of(new DatastoreEntityDoFn<>(
                                            destination, keyFields, "#",
                                            inputAvroSchema,
                                            AvroSchemaUtil::convertSchema,
                                            AvroToEntityConverter::convert)))
                                    .setCoder(SerializableCoder.of(Entity.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getAvroSchema());
                            return output;
                        }
                    }
                }
                case ROW: {
                    final PCollection<Row> inputRow = (PCollection<Row>) input;
                    switch (outputType) {
                        case AVRO: {
                            output = (PCollection<OutputT>) inputRow
                                    .apply("RowToRecord", ParDo
                                            .of(new TransformDoFn<>(inputCollection.getSchema(),
                                                    RowToRecordConverter::convertSchema,
                                                    RowToRecordConverter::convert)))
                                    .setCoder(AvroCoder.of(inputCollection.getAvroSchema()));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getAvroSchema());
                            return output;
                        }
                        case MUTATION: {
                            output = (PCollection<OutputT>) inputRow
                                    .apply("RowToMutation", ParDo.of(new SpannerMutationDoFn<Schema, Schema, Row>(
                                            destination, spannerMutationOp, keyFields, null, excludeFields, maskFields,
                                            RowToMutationConverter::convert)))
                                    .setCoder(SerializableCoder.of(Mutation.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSchema());
                            return output;
                        }
                        case ENTITY: {
                            output = (PCollection<OutputT>) inputRow
                                    .apply("RecordToEntity", ParDo.of(new DatastoreEntityDoFn<>(
                                            destination, keyFields, "#",
                                            RowToEntityConverter::convert)))
                                    .setCoder(SerializableCoder.of(Entity.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSchema());
                            return output;
                        }
                    }
                }
                case STRUCT: {
                    final PCollection<Struct> inputStruct = (PCollection<Struct>) input;
                    switch (outputType) {
                        case AVRO: {
                            output = (PCollection<OutputT>) inputStruct
                                    .apply("StructToRecord", ParDo
                                            .of(new TransformDoFn<>(inputCollection.getSpannerType(),
                                                    StructToAvroConverter::convertSchema,
                                                    StructToAvroConverter::convert)))
                                    .setCoder(AvroCoder.of(inputCollection.getAvroSchema()));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getAvroSchema());
                            return output;
                        }
                        case ROW: {
                            output = (PCollection<OutputT>) inputStruct
                                    .apply("StructToRow", ParDo
                                            .of(new TransformDoFn<>(inputCollection.getSpannerType(),
                                                    StructToRowConverter::convertSchema,
                                                    StructToRowConverter::convert)))
                                    .setCoder(RowCoder.of(inputCollection.getSchema()))
                                    .setRowSchema(inputCollection.getSchema());
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSchema());
                            return output;
                        }
                        case MUTATION: {
                            output = (PCollection<OutputT>) inputStruct
                                    .apply("StructToMutation", ParDo.of(new SpannerMutationDoFn<Schema, Schema, Struct>(
                                            destination, spannerMutationOp, keyFields, null, excludeFields, maskFields,
                                            StructToMutationConverter::convert)))
                                    .setCoder(SerializableCoder.of(Mutation.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSpannerType());
                            return output;
                        }
                        case ENTITY: {
                            output = (PCollection<OutputT>) inputStruct
                                    .apply("StructToEntity", ParDo.of(new DatastoreEntityDoFn<>(
                                            destination, keyFields, "#",
                                            StructToEntityConverter::convert)))
                                    .setCoder(SerializableCoder.of(Entity.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSchema());
                            return output;
                        }
                    }
                }
                case ENTITY: {
                    final PCollection<Entity> inputEntity = (PCollection<Entity>) input;
                    switch (outputType) {
                        case AVRO: {
                            final org.apache.avro.Schema withKeySchema = inputCollection.getAvroSchema();
                            output = (PCollection<OutputT>) inputEntity
                                    .apply("EntityToRecord", ParDo
                                            .of(new TransformDoFn<>(withKeySchema.toString(),
                                                    AvroSchemaUtil::convertSchema,
                                                    EntityToAvroConverter::convert)))
                                    .setCoder(AvroCoder.of(withKeySchema));
                            this.outputCollection = FCollection.of(name, output, outputType, withKeySchema);
                            return output;
                        }
                        case ROW: {
                            final Schema withKeySchema = inputCollection.getSchema();
                            output = (PCollection<OutputT>) inputEntity
                                    .apply("EntityToRow", ParDo
                                            .of(new TransformDoFn<>(withKeySchema,
                                                    s -> s,
                                                    EntityToRowConverter::convert)))
                                    .setCoder(RowCoder.of(withKeySchema))
                                    .setRowSchema(withKeySchema);
                            this.outputCollection = FCollection.of(name, output, outputType, withKeySchema);
                            return output;
                        }
                        case MUTATION: {
                            output = (PCollection<OutputT>) inputEntity
                                    .apply("EntityToMutation", ParDo.of(new SpannerMutationDoFn<>(
                                            destination, spannerMutationOp, keyFields, null, excludeFields, maskFields,
                                            inputCollection.getSpannerType(),
                                            s -> s,
                                            EntityToMutationConverter::convert)))
                                    .setCoder(SerializableCoder.of(Mutation.class));
                            this.outputCollection = FCollection.of(name, output, outputType, inputCollection.getSpannerType());
                            return output;
                        }
                    }
                }
                default:
                    throw new IllegalArgumentException("Not supported target data type: " + outputType.name() + ", from: " + inputType.name());
            }
        }

    }

    private static class TransformDoFn<InputSchemaT, OutputSchemaT, InputT, OutputT> extends DoFn<InputT, OutputT> {

        private final InputSchemaT schema;
        private final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter;
        private final DataConverter<OutputSchemaT, InputT, OutputT> dataConverter;

        private transient OutputSchemaT runtimeSchema;

        private TransformDoFn(final InputSchemaT schema,
                              final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter,
                              final DataConverter<OutputSchemaT, InputT, OutputT> dataConverter) {

            this.schema = schema;
            this.schemaConverter = schemaConverter;
            this.dataConverter = dataConverter;
        }

        @Setup
        public void setup() {
            this.runtimeSchema = this.schemaConverter.convert(this.schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            OutputT output = this.dataConverter.convert(runtimeSchema, c.element());
            c.output(output);
        }
    }

    private interface SchemaConverter<InputSchemaT, OutputSchemaT> extends Serializable {
        OutputSchemaT convert(InputSchemaT schema);
    }

    private interface DataConverter<SchemaT, InputT, OutputT> extends Serializable {
        OutputT convert(SchemaT schema, InputT element);
    }

    private static class RowDoFn<InputSchemaT, RuntimeSchemaT, InputT> extends DoFn<InputT, Row> {

        private final InputSchemaT schema;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final RowConverter<RuntimeSchemaT, InputT> rowConverter;

        private final Schema outputSchema;
        private transient RuntimeSchemaT runtimeSchema;

        private RowDoFn(final InputSchemaT schema,
                        final Schema outputSchema,
                        final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                        final RowConverter<RuntimeSchemaT, InputT> rowConverter) {

            this.schema = schema;
            this.outputSchema = outputSchema;
            this.schemaConverter = schemaConverter;
            this.rowConverter = rowConverter;
        }

        @Setup
        public void setup() {
            this.runtimeSchema = this.schemaConverter.convert(this.schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(this.rowConverter.convert(runtimeSchema, outputSchema, c.element()));
        }
    }

    private interface RowConverter<SchemaT, InputT> extends Serializable {
        Row convert(SchemaT inputSchema, Schema outputSchema, InputT element);
    }


    private static class SpannerMutationDoFn<InputSchemaT, OutputSchemaT, InputT> extends DoFn<InputT, Mutation> {

        private final String table;
        private final String mutationOp;
        private final List<String> keyFields;
        private final List<String> allowCommitTimestampFields;
        private final Set<String> excludeFields;
        private final Set<String> maskFields;

        private final InputSchemaT schema;
        private final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter;
        private final SpannerMutationConverter<OutputSchemaT, InputT> recordConverter;

        private transient OutputSchemaT runtimeSchema;

        private SpannerMutationDoFn(final String table, final String mutationOp, final List<String> keyFields,
                                    final List<String> allowCommitTimestampFields,
                                    final Set<String> excludeFields, final Set<String> maskFields,
                                    final SpannerMutationConverter<OutputSchemaT, InputT> recordConverter) {

            this(table, mutationOp, keyFields, allowCommitTimestampFields, excludeFields, maskFields, null, null, recordConverter);
        }

        private SpannerMutationDoFn(final String table, final String mutationOp, final List<String> keyFields,
                                    final List<String> allowCommitTimestampFields,
                                    final Set<String> excludeFields, final Set<String> maskFields,
                                    final InputSchemaT schema,
                                    final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter,
                                    final SpannerMutationConverter<OutputSchemaT, InputT> recordConverter) {
            this.table = table;
            this.mutationOp = mutationOp;
            this.keyFields = keyFields;// == null ? null : Arrays.asList(keyFields.split(","));
            this.allowCommitTimestampFields = allowCommitTimestampFields;
            this.excludeFields = excludeFields;
            this.maskFields = maskFields;
            this.schema = schema;
            this.schemaConverter = schemaConverter;
            this.recordConverter = recordConverter;
        }

        @Setup
        public void setup() {
            if(this.schema == null || this.schemaConverter == null) {
                this.runtimeSchema = null;
            } else {
                this.runtimeSchema = this.schemaConverter.convert(schema);
            }
        }

        @ProcessElement
        public void processElement(final @Element InputT input, final OutputReceiver<Mutation> receiver) {
            final Mutation mutation = recordConverter.convert(runtimeSchema, input, table, mutationOp, keyFields, allowCommitTimestampFields, excludeFields, maskFields);
            receiver.output(mutation);
        }

    }

    private static class DatastoreEntityDoFn<InputSchemaT, OutputSchemaT, InputT> extends DoFn<InputT, Entity> {

        private final String kind;
        private final List<String> keyFields;
        private final String splitter;

        private final InputSchemaT schema;
        private final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter;
        private final DatastoreEntityConverter<OutputSchemaT, InputT> recordConverter;

        private transient OutputSchemaT runtimeSchema;

        private DatastoreEntityDoFn(final String kind, final List<String> keyFields, final String splitter,
                                    final DatastoreEntityConverter<OutputSchemaT, InputT> recordConverter) {

            this(kind, keyFields, splitter, null, null, recordConverter);
        }

        private DatastoreEntityDoFn(final String kind, final List<String> keyFields, final String splitter,
                                    final InputSchemaT schema,
                                    final SchemaConverter<InputSchemaT, OutputSchemaT> schemaConverter,
                                    final DatastoreEntityConverter<OutputSchemaT, InputT> recordConverter) {
            this.kind = kind;
            this.keyFields = keyFields;
            this.splitter = splitter;
            this.schema = schema;
            this.schemaConverter = schemaConverter;
            this.recordConverter = recordConverter;
        }

        @Setup
        public void setup() {
            if(this.schema == null || this.schemaConverter == null) {
                this.runtimeSchema = null;
            } else {
                this.runtimeSchema = this.schemaConverter.convert(schema);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(recordConverter.convert(runtimeSchema, c.element(), kind, keyFields, splitter));
        }

    }

    public static class WithKeyTransform<InputT extends Object> extends PTransform<PCollection<InputT>, PCollection<KV<String, InputT>>> {

        private Iterable<String> keyFields;
        private FCollection<InputT> inputCollection;

        public WithKeyTransform(final FCollection<InputT> inputCollection,
                                final List<String> keyFields) {
            this.keyFields = keyFields;
            this.inputCollection = inputCollection;
        }

        public WithKeyTransform(final FCollection<InputT> inputCollection,
                                final Iterable<String> keyFields) {
            this.keyFields = keyFields;
            this.inputCollection = inputCollection;
        }

        @Override
        public PCollection<KV<String, InputT>> expand(final PCollection<InputT> input) {
            final PCollection output;
            switch (inputCollection.getDataType()) {
                case AVRO: {
                    final PCollection<GenericRecord> records = (PCollection<GenericRecord>)input;
                    output = records.apply(ParDo.of(new WithKeyDoFn<>(keyFields, AvroSchemaUtil::getAsString)));
                    return output;
                }
                case ROW: {
                    final PCollection<Row> rows = (PCollection<Row>)input;
                    output = rows.apply(ParDo.of(new WithKeyDoFn<>(keyFields, RowSchemaUtil::getAsString)));
                    return output;
                }
                case STRUCT: {
                    final PCollection<Struct> structs = (PCollection<Struct>)input;
                    output = structs.apply(ParDo.of(new WithKeyDoFn<>(keyFields, StructSchemaUtil::getAsString)));
                    return output;
                }
                case ENTITY: {
                    final PCollection<Entity> structs = (PCollection<Entity>)input;
                    output = structs.apply(ParDo.of(new WithKeyDoFn<>(keyFields, EntitySchemaUtil::getFieldValueAsString)));
                    return output;
                }
                default:
                    throw new IllegalArgumentException("Not supported data type: " + inputCollection.getDataType().name());
            }
        }
    }

    public static class WithTimestampTransform<InputT extends Object> extends PTransform<PCollection<InputT>, PCollection<InputT>> {

        private final String timestampAttribute;
        private final DataType dataType;
        private final Instant timestampDefault;

        public WithTimestampTransform(final DataType dataType,
                                      final String timestampAttribute,
                                      final Instant timestampDefault) {
            this.timestampAttribute = timestampAttribute;
            this.dataType = dataType;
            this.timestampDefault = timestampDefault;
        }

        @Override
        public PCollection<InputT> expand(final PCollection<InputT> input) {
            final PCollection output;
            switch (dataType) {
                case AVRO: {
                    final PCollection<GenericRecord> records = (PCollection<GenericRecord>)input;
                    output = records.apply("WithTimestamp", ParDo.of(new WithTimestampDoFn<>(timestampAttribute, AvroSchemaUtil::getTimestamp, timestampDefault)));
                    return output;
                }
                case ROW: {
                    final PCollection<Row> rows = (PCollection<Row>)input;
                    output = rows.apply("WithTimestamp", ParDo.of(new WithTimestampDoFn<>(timestampAttribute, RowSchemaUtil::getTimestamp, timestampDefault)));
                    return output;
                }
                case STRUCT: {
                    final PCollection<Struct> structs = (PCollection<Struct>)input;
                    output = structs.apply("WithTimestamp", ParDo.of(new WithTimestampDoFn<>(timestampAttribute, StructSchemaUtil::getTimestamp, timestampDefault)));
                    return output;
                }
                case ENTITY: {
                    final PCollection<Entity> entities = (PCollection<Entity>)input;
                    output = entities.apply("WithTimestamp", ParDo.of(new WithTimestampDoFn<>(timestampAttribute, EntitySchemaUtil::getTimestamp, timestampDefault)));
                    return output;
                }
                default:
                    throw new IllegalArgumentException("Not supported data type: " + dataType.name());
            }
        }
    }

    public static GenericRecord convertRecord(final DataType inputType, final org.apache.avro.Schema schema, final Object value) {
        switch (inputType) {
            case AVRO:
                return (GenericRecord) value;
            case ROW:
                return RowToRecordConverter.convert(schema, (Row) value);
            case STRUCT:
                return StructToAvroConverter.convert(schema, (Struct) value);
            case ENTITY:
                return EntityToAvroConverter.convert(schema, (Entity) value);
            default:
                throw new IllegalArgumentException("Not supported input type: " + inputType.name());
        }
    }

    private static class WithKeyDoFn<InputT> extends DoFn<InputT, KV<String,InputT>> {

        private final Iterable<String> keyFields;
        private final FieldExtractor<InputT> keyExtractor;

        private WithKeyDoFn(final List<String> keyFields, final FieldExtractor<InputT> keyExtractor) {
            this.keyFields = keyFields;
            this.keyExtractor = keyExtractor;
        }

        private WithKeyDoFn(final Iterable<String> keyFields, final FieldExtractor<InputT> keyExtractor) {
            this.keyFields = keyFields;
            this.keyExtractor = keyExtractor;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final InputT input = c.element();
            StringBuilder sb = new StringBuilder();
            for(final String field : keyFields) {
                sb.append(keyExtractor.getAsString(input, field));
                sb.append("#");
            }
            c.output(KV.of(sb.toString(), input));
        }

    }

    private static class WithTimestampDoFn<InputT> extends DoFn<InputT, InputT> {

        private final String timestampAttribute;
        private final TimestampExtractor<InputT> timestampExtractor;
        private final Instant timestampDefault;

        private WithTimestampDoFn(final String timestampAttribute,
                                  final TimestampExtractor<InputT> timestampExtractor,
                                  final Instant timestampDefault) {
            this.timestampAttribute = timestampAttribute;
            this.timestampExtractor = timestampExtractor;
            this.timestampDefault = timestampDefault;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final InputT input = c.element();
            c.outputWithTimestamp(input, timestampExtractor.getTimestamp(input, timestampAttribute, timestampDefault));
        }

    }

    private interface SpannerMutationConverter<SchemaT, InputT> extends Serializable {
        Mutation convert(final SchemaT schema,
                         final InputT element,
                         final String table,
                         final String mutationOp,
                         final Iterable<String> keyFields,
                         final List<String> allowCommitTimestampFields,
                         final Set<String> excludeFields,
                         final Set<String> maskFields);
    }

    private interface DatastoreEntityConverter<SchemaT, InputT> extends Serializable {
        Entity convert(SchemaT schema, InputT element,
                       String kind, List<String> keyFields, String splitter);
    }

    private interface FieldExtractor<InputT> extends Serializable {
        String getAsString(final InputT input, final String keyField);
    }

    private interface TimestampExtractor<InputT> extends Serializable {
        Instant getTimestamp(final InputT input, final String keyField, final Instant defaultTimestamp);
    }

}
