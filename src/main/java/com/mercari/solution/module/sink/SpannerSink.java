package com.mercari.solution.module.sink;

import com.google.cloud.spanner.*;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToSpannerMutationConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


@Sink.Module(name="spanner")
public class SpannerSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSink.class);

    private static class Parameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String databaseId;
        private String table;
        private String mutationOp;
        private List<String> keyFields;
        private List<String> commitTimestampFields;

        private Boolean flattenFailures;
        private Boolean flattenGroup;

        private Mode mode;
        private Boolean emulator;
        private Long maxNumRows;
        private Long maxNumMutations;
        private Long batchSizeBytes;
        private Integer groupingFactor;
        private Options.RpcPriority priority;

        public void validate(final MPipeline.Runner runner, final DataType dataType, final Boolean isTuple) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.projectId == null) {
                errorMessages.add("Parameter must contain projectId");
            }
            if(this.instanceId == null) {
                errorMessages.add("Parameter must contain instanceId");
            }
            if(this.databaseId == null) {
                errorMessages.add("Parameter must contain databaseId");
            }
            if(this.table == null) {
                switch (dataType) {
                    case MUTATION, MUTATIONGROUP, UNIFIEDMUTATION -> {}
                    default -> errorMessages.add("Parameter must contain table");
                }
            }

            if(this.emulator) {
                if(MPipeline.Runner.direct.equals(runner)) {
                    errorMessages.add("If use spanner emulator, Use DirectRunner");
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }
        public void setDefaults() {
            //
            if(this.mutationOp == null) {
                this.mutationOp = Mutation.Op.INSERT_OR_UPDATE.name();
            }
            if(this.priority == null) {
                this.priority = Options.RpcPriority.MEDIUM;
            }

            if(this.emulator == null) {
                this.emulator = false;
            }
            if(this.commitTimestampFields == null) {
                this.commitTimestampFields = new ArrayList<>();
            }
            if(mode == null) {
                mode = Mode.normal;
            }

            if(this.maxNumRows == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L383
                this.maxNumRows = 500L;
            }
            if(this.maxNumMutations == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L381
                this.maxNumMutations = 5000L;
            }
            if(this.batchSizeBytes == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L379
                this.batchSizeBytes = 1024L * 1024L;
            }
            if(this.groupingFactor == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L385
                this.groupingFactor = 1000;
            }

            if(this.flattenFailures == null) {
                this.flattenFailures = true;
            }
            if(this.flattenGroup == null) {
                this.flattenGroup = false;
            }

        }
    }

    public enum Mode {
        normal,
        changeCapture,
        restore
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.setDefaults();
        //parameters.validate(, sample.getDataType(), sample.getIsTuple());

        switch (parameters.mode) {
            case normal -> {
                final Schema inputSchema = Union.createUnionSchema(inputs);
                final PCollection<MElement> input = inputs
                        .apply("Union", Union.flatten()
                                .withWaits(getWaits())
                                .withStrategy(getStrategy()));
                final PCollection<Void> output = input
                        .apply("Write", new SpannerWriteSingle(parameters, inputSchema));
            }
            case changeCapture -> {
                // TODO
            }
            case restore -> {
                final PCollection<Void> output = inputs
                        .apply("Write", new SpannerWriteMulti(parameters, getWaits()));
            }
        }

        return null;
    }

    public static class SpannerWriteSingle extends PTransform<PCollection<MElement>, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteSingle.class);

        private final Parameters parameters;
        private final Schema inputSchema;

        private SpannerWriteSingle(
                final Parameters parameters,
                final Schema inputSchema) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
        }

        public PCollection<Void> expand(final PCollection<MElement> input) {
            // SpannerWrite
            final SpannerIO.Write write = createWrite(parameters, true);

            final PCollection<Mutation> mutations = input
                    .apply("ToMutation", ParDo.of(new SpannerMutationDoFn(
                            parameters.table, parameters.mutationOp, parameters.keyFields, parameters.commitTimestampFields, inputSchema)))
                    .setCoder(SerializableCoder.of(Mutation.class));

            // Custom SpannerWrite for DirectRunner
            if(OptionUtil.isDirectRunner(input)) {
                return mutations
                        .apply("WriteSpanner", ParDo
                                .of(new WriteMutationDoFn(parameters.projectId, parameters.instanceId, parameters.databaseId, 500, parameters.emulator)));
            }
            final SpannerWriteResult writeResult = mutations
                    .apply("WriteSpanner", write);
            return writeResult.getOutput();
        }

    }

    public static class SpannerWriteMulti extends PTransform<MCollectionTuple, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteMulti.class);

        private final Parameters parameters;
        private final List<PCollection<?>> waits;

        private SpannerWriteMulti(
                final Parameters parameters,
                final List<PCollection<?>> waits) {

            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<Void> expand(final MCollectionTuple inputs) {

            final Map<String, String> tagNames = inputs.getAllSchemaAsMap()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getName()));
            final Map<String, org.apache.avro.Schema> avroSchemas = inputs.getAllSchemaAsMap()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getAvroSchema()));
            final Map<String, String> tags = tagNames
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getValue,
                            Map.Entry::getKey));
            final Map<String, List<String>> pedigree = calcPedigree(tags, avroSchemas);

            final List<PCollection<?>> waitList = new ArrayList<>();
            final PCollection<String> ddls = executeDDLs(inputs.getPipeline(), waits, parameters, pedigree, avroSchemas);
            waitList.add(ddls);

            final List<PCollection<Void>> outputs = new ArrayList<>();
            final List<String> parents = new ArrayList<>();
            parents.add(null);
            int level = 1;
            while(!parents.isEmpty()) {
                PCollectionList<Mutation> mutationsList = PCollectionList.empty(inputs.getPipeline());
                final List<String> childrenTags = new ArrayList<>();
                for(final String parent : parents) {
                    final List<String> children = pedigree.getOrDefault(parent, Collections.emptyList());
                    for(final String child : children) {
                        final String name = tagNames.get(child);
                        final org.apache.avro.Schema avroSchema = avroSchemas.get(child);
                        final PCollection<MElement> collection = inputs.get(child);
                        final PCollection<Mutation> mutations = collection
                                .apply("ToMutation", ParDo
                                        .of(new SpannerMutationDoFn(parameters.table, parameters.mutationOp, parameters.keyFields, parameters.commitTimestampFields, Schema.of(avroSchema))));
                        mutationsList = mutationsList.and(mutations);
                        childrenTags.add(child);
                    }
                }

                if(childrenTags.isEmpty()) {
                    break;
                }

                final SpannerIO.Write write = createWrite(parameters, true);
                final PCollection<Mutation> mutations = mutationsList
                        .apply("Flatten." + level, Flatten.pCollections());
                final SpannerWriteResult writeResult;
                final PCollection<Void> v;
                if(!waitList.isEmpty()) {
                    writeResult = mutations
                            .apply("Wait." + level, Wait.on(waitList))
                            .apply("Write." + level, write);
                } else {
                    writeResult = mutations
                            .apply("Write." + level, write);
                }

                parents.clear();
                waitList.clear();
                parents.addAll(childrenTags);
                waitList.add(writeResult.getOutput());

                outputs.add(writeResult.getOutput());
                level += 1;
            }

            PCollectionList<Void> outputsList = PCollectionList.empty(inputs.getPipeline());
            for(final PCollection<Void> output : outputs) {
                outputsList = outputsList.and(output);
            }

            return outputsList
                    .apply("FlattenOutput", Flatten.pCollections())
                    .setCoder(VoidCoder.of());
        }

        private Map<String, List<String>> calcPedigree(
                final Map<String, String> tagNames,
                final Map<String, org.apache.avro.Schema> avroSchemas) {

            final Map<String, List<String>> pedigree = new HashMap<>();
            for(final Map.Entry<String, org.apache.avro.Schema> entry : avroSchemas.entrySet()) {
                final org.apache.avro.Schema schema = entry.getValue();
                final String childName = schema.getName();
                final String parentName = schema.getProp("spannerParent");
                final String childTag = tagNames.get(childName);
                final String parentTag = tagNames.get(parentName);
                if(!pedigree.containsKey(parentTag)) {
                    pedigree.put(parentTag, new ArrayList<>());
                }
                if(childTag != null) {
                    pedigree.get(parentTag).add(childTag);
                }
            }
            return pedigree;
        }

        private PCollection<String> executeDDLs(
                final Pipeline pipeline,
                final List<PCollection<?>> waits,
                final Parameters parameters,
                final Map<String, List<String>> pedigree,
                final Map<String, org.apache.avro.Schema> avroSchemas) {

            final List<PCollection<String>> outputs = new ArrayList<>();

            final List<PCollection<?>> waitList = new ArrayList<>(waits);
            final List<String> parents = new ArrayList<>();
            parents.add(null);
            int level = 1;
            while(!parents.isEmpty()) {
                final List<String> ddls = new ArrayList<>();
                final List<String> childrenTags = new ArrayList<>();
                for(final String parent : parents) {
                    final List<String> children = pedigree.getOrDefault(parent, Collections.emptyList());
                    for(final String child : children) {
                        final org.apache.avro.Schema avroSchema = avroSchemas.get(child);
                        ddls.add(avroSchema.toString());
                        childrenTags.add(child);
                    }
                }

                if(ddls.isEmpty()) {
                    break;
                }

                final PCollection<String> ddlResult;
                if(!waitList.isEmpty()) {
                    ddlResult = pipeline
                            .apply("SupplyDDL." + level, Create.of(KV.of("", ddls)).withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))))
                            .apply("WaitDDL." + level, Wait.on(waitList))
                            .apply("ExecuteDDL." + level, ParDo.of(new DDLDoFn(
                                    parameters.projectId,
                                    parameters.instanceId,
                                    parameters.databaseId,
                                    parameters.emulator)));
                } else {
                    ddlResult = pipeline
                            .apply("SupplyDDL." + level, Create.of(KV.of("", ddls)).withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))))
                            .apply("ExecuteDDL." + level, ParDo.of(new DDLDoFn(
                                    parameters.projectId,
                                    parameters.instanceId,
                                    parameters.databaseId,
                                    parameters.emulator)));

                }

                parents.clear();
                waitList.clear();
                parents.addAll(childrenTags);
                waitList.add(ddlResult);

                outputs.add(ddlResult);
                level += 1;
            }

            return PCollectionList
                    .of(outputs)
                    .apply("Flatten", Flatten.pCollections());
        }

        private static class DDLDoFn extends DoFn<KV<String,List<String>>, String> {

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final Boolean emulator;

            private transient Spanner spanner;

            DDLDoFn(final String projectId, final String instanceId, final String databaseId, final Boolean emulator) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.emulator = emulator;
            }


            @Setup
            public void setup() {
                this.spanner = SpannerUtil.connectSpanner(
                        projectId, 1, 1, 1, false, emulator);
            }
            @ProcessElement
            public void processElement(final ProcessContext c) {
                final List<String> schemaStrings = c.element().getValue();
                final List<String> ddls = new ArrayList<>();
                for(final String schemaString : schemaStrings) {
                    LOG.info("Table avro schema: " + schemaString);
                    final org.apache.avro.Schema schema = AvroSchemaUtil.convertSchema(schemaString);
                    ddls.addAll(createDDLs(schema));
                }
                LOG.info("Execute DDLs: " + ddls);
                SpannerUtil.executeDDLs(spanner, instanceId, databaseId, ddls, 3600, 5);

                c.output("ok");
            }

            @Teardown
            public void teardown() {
                if(this.spanner != null) {
                    this.spanner.close();
                }
            }

            private List<String> createDDLs(org.apache.avro.Schema schema) {
                if(schema == null) {
                    throw new IllegalArgumentException("avro schema must not be null for creating spanner tables");
                }
                final String table = schema.getName();
                final String parent = schema.getProp("spannerParent");
                final String onDeleteAction = schema.getProp("spannerOnDeleteAction");
                final String primaryKey = schema.getProp("spannerPrimaryKey");

                final Map<Integer, String> primaryKeys = new TreeMap<>();
                final Map<Integer, String> indexes = new TreeMap<>();
                final Map<Integer, String> foreignKeys = new TreeMap<>();
                final Map<Integer, String> checkConstraints = new TreeMap<>();
                for(final Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
                    if(entry.getKey().startsWith("spannerPrimaryKey_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerPrimaryKey_", ""));
                        primaryKeys.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerIndex_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerIndex_", ""));
                        indexes.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerForeignKey_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerForeignKey_", ""));
                        foreignKeys.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerCheckConstraint_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerCheckConstraint_", ""));
                        checkConstraints.put(n, entry.getValue().toString());
                    }
                }

                final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE %s ( %n", table));
                for(final org.apache.avro.Schema.Field field : schema.getFields()) {
                    final String sqlType = field.getProp("sqlType");
                    final String generationExpression = field.getProp("generationExpression");

                    final String defaultExpression = field.getProp("defaultExpression");
                    final String stored;
                    if(generationExpression != null) {
                        stored = field.getProp("stored");
                    } else {
                        stored = null;
                    }

                    final Map<Integer, String> fieldOptions = new TreeMap<>();
                    for(final Map.Entry<String, Object> entry : field.getObjectProps().entrySet()) {
                        if(entry.getKey().startsWith("spannerOption_")) {
                            final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerOption_", ""));
                            fieldOptions.put(n, entry.getValue().toString());
                        }
                    }

                    final String columnExpression = String.format("`%s` %s%s%s%s,%n",
                            field.name(),
                            sqlType,
                            AvroSchemaUtil.isNullable(field.schema()) ? "" : " NOT NULL",
                            defaultExpression == null ? (stored == null ? "" : " AS ("+ stored +") STORED") : " DEFAULT (" + defaultExpression +")",
                            fieldOptions.isEmpty() ? "" : " OPTIONS (" + String.join(",", fieldOptions.values()) + ")");

                    sb.append(columnExpression);
                }

                for(String foreignKey : foreignKeys.values()) {
                    final String foreignKeyExpression = String.format("%s,%n", foreignKey);
                    sb.append(foreignKeyExpression);
                }
                for(String checkConstraint : checkConstraints.values()) {
                    final String checkConstraintExpression = String.format("%s,%n", checkConstraint);
                    sb.append(checkConstraintExpression);
                }

                //sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                if(primaryKey != null) {
                    sb.append(String.format(" PRIMARY KEY ( %s )", primaryKey));
                }
                if(parent != null) {
                    sb.append(",");
                    sb.append("INTERLEAVE IN PARENT ");
                    sb.append(parent);
                    sb.append(String.format(" ON DELETE %s", "cascade".equalsIgnoreCase(onDeleteAction) ? "CASCADE" : "NO ACTION"));
                }

                final List<String> ddls = new ArrayList<>();
                ddls.add(sb.toString());
                ddls.addAll(indexes.values());
                return ddls;
            }

        }

    }


    public static class SpannerWriteMutations extends PTransform<PCollection<Mutation>, PCollectionTuple> {

        private final Parameters parameters;
        private final List<PCollection<?>> waitsPCollections;

        private final Map<String, Type> types;
        private final Map<String, Set<String>> parentTables;
        private final Map<String, TupleTag<Mutation>> tags;
        private final TupleTag<Void> outputTag;
        private final TupleTag<MElement> failureTag;
        private final boolean failFast;

        public TupleTag<Void> getOutputTag() {
            return this.outputTag;
        }

        public TupleTag<MElement> getFailuresTag() {
            return this.failureTag;
        }

        SpannerWriteMutations(
                final Parameters parameters,
                final List<PCollection<?>> waits,
                final boolean failFast) {

            this.parameters = parameters;
            this.waitsPCollections = waits;
            this.failFast = failFast;

            this.types = SpannerUtil.getTypesFromDatabase(
                    parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.emulator);
            this.parentTables = SpannerUtil.getParentTables(
                    parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.emulator);

            LOG.info("parent tables: " + this.parentTables);

            final Set<String> tables = parentTables.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            this.tags = new HashMap<>();
            for(String table : tables) {
                this.tags.put(table, new TupleTag<>(){});
            }

            this.outputTag = new TupleTag<>() {};
            this.failureTag = new TupleTag<>() {};
        }
        @Override
        public PCollectionTuple expand(PCollection<Mutation> input) {

            final List<TupleTag<?>> tagList = new ArrayList<>(tags.values());

            final TupleTag<Mutation> ftag = new TupleTag<>() {};
            final PCollectionTuple tuple = input.apply("WithTableTag", ParDo
                    .of(new WithTableTagDoFn())
                    .withOutputTags(ftag, TupleTagList.of(tagList)));
            List<PCollection<?>> waits;
            if(waitsPCollections == null || waitsPCollections.isEmpty()) {
                waits = List.of(input.getPipeline().apply("Create", Create.empty(VoidCoder.of())));
            } else {
                waits = new ArrayList<>(waitsPCollections);
            }

            PCollectionList<Void> outputsList = PCollectionList.empty(input.getPipeline());
            PCollectionList<MutationGroup> failuresList = PCollectionList.empty(input.getPipeline());

            int level = 0;
            final Set<String> parents = new HashSet<>();
            parents.add("");
            while(!parents.isEmpty()) {
                PCollectionList<Mutation> mutationsList = PCollectionList.empty(input.getPipeline());
                Set<String> nextParents = new HashSet<>();
                for(final String parent : parents) {
                    final Set<String> children = parentTables.getOrDefault(parent, new HashSet<>());
                    for(final String child : children) {
                        final TupleTag<Mutation> tag = tags.get(child);
                        final PCollection<Mutation> mutations = tuple.get(tag);
                        mutationsList = mutationsList.and(mutations);
                    }
                    nextParents.addAll(children);
                }

                final SpannerIO.Write write = createWrite(parameters, failFast);

                final SpannerWriteResult result = mutationsList
                        .apply("Flatten" + level, Flatten.pCollections())
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Wait" + level, Wait.on(waits))
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Write" + level, write);

                waits = List.of(result.getOutput());
                outputsList = outputsList.and(result.getOutput());
                if(!failFast) {
                    failuresList = failuresList.and(result.getFailedMutations());
                }

                parents.clear();
                parents.addAll(nextParents);
                level += 1;
            }

            final PCollection<Void> outputs = outputsList.apply("FlattenOutputs", Flatten.pCollections());

            if(failFast) {
                return PCollectionTuple
                        .of(outputTag, outputs);
            } else {
                final PCollection<MElement> failures = failuresList
                        .apply("FlattenFailures", Flatten.pCollections())
                        .apply("ConvertFailures", ParDo.of(new FailedMutationConvertDoFn(parameters)))
                        .setCoder(ElementCoder.of(FailedMutationConvertDoFn.createFailureSchema(parameters.flattenFailures)));
                return PCollectionTuple
                        .of(outputTag, outputs)
                        .and(failureTag, failures);
            }
        }

        private class WithTableTagDoFn extends DoFn<Mutation, Mutation> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Mutation mutation = c.element();
                if(mutation == null) {
                    return;
                }

                final String table = mutation.getTable();
                final TupleTag<Mutation> tag = tags.get(table);
                if(tag == null) {
                    LOG.warn("Destination table tag is missing for table: {}, mutation: {}", table, mutation);
                    return;
                }

                final Type type = types.get(table);
                if(type == null) {
                    LOG.warn("Destination table type is missing for table: {}, mutation: {}", table, mutation);
                    return;
                }

                if(StructSchemaUtil.validate(type, mutation)) {
                    c.output(tag, mutation);
                } else {
                    final Mutation adjusted = StructSchemaUtil.adjust(type, mutation);
                    c.output(tag, adjusted);
                }
            }

        }
    }

    private static SpannerIO.Write createWrite(
            final Parameters parameters,
            final boolean failFast) {

        SpannerIO.Write write = SpannerIO.write()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withDatabaseId(parameters.databaseId)
                .withMaxNumRows(parameters.maxNumRows)
                .withMaxNumMutations(parameters.maxNumMutations)
                .withBatchSizeBytes(parameters.batchSizeBytes)
                .withGroupingFactor(parameters.groupingFactor);

        if(failFast) {
            write = write.withFailureMode(SpannerIO.FailureMode.FAIL_FAST);
        } else {
            write = write.withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES);
        }

        write = switch (parameters.priority) {
            case LOW -> write.withLowPriority();
            case HIGH -> write.withHighPriority();
            default -> write;
        };

        return write;
    }

    private static class SpannerMutationDoFn extends DoFn<MElement, Mutation> {

        private final String table;
        private final String mutationOp;
        private final List<String> keyFields;
        private final List<String> allowCommitTimestampFields;

        private final Schema schema;

        private SpannerMutationDoFn(
                final String table,
                final String mutationOp,
                final List<String> keyFields,
                final List<String> allowCommitTimestampFields,
                final Schema schema) {

            this.table = table;
            this.mutationOp = mutationOp;
            this.keyFields = keyFields;
            this.allowCommitTimestampFields = allowCommitTimestampFields;
            this.schema = schema;
        }

        @Setup
        public void setup() {
            this.schema.setup();
        }

        @ProcessElement
        public void processElement(final @Element MElement input, final OutputReceiver<Mutation> receiver) {
            final Mutation mutation = ElementToSpannerMutationConverter.convert(schema, input, table, mutationOp, keyFields, allowCommitTimestampFields);
            receiver.output(mutation);
        }

    }

    private static class FailedMutationConvertDoFn extends DoFn<MutationGroup, MElement> {

        private final Schema schema;
        private final Schema childSchema;

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Boolean flatten;


        FailedMutationConvertDoFn(
                final Parameters parameters) {

            this.schema = createFailureSchema(parameters.flattenFailures);
            this.childSchema = createFailureMutationSchema();
            this.projectId = parameters.projectId;
            this.instanceId = parameters.instanceId;
            this.databaseId = parameters.databaseId;
            this.flatten = parameters.flattenFailures;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MutationGroup input = c.element();
            final Instant timestamp = Instant.now();

            if(flatten) {
                final MElement primaryFailedRow = convertFailedFlatRow(input.primary(), timestamp);
                c.output(primaryFailedRow);
                for(final Mutation attached : input.attached()) {
                    final MElement attachedFailedRow = convertFailedFlatRow(attached, timestamp);
                    c.output(attachedFailedRow);
                }
            } else {
                final List<MElement> contents = new ArrayList<>();
                final Mutation primary = input.primary();
                contents.add(convertFailedRow(primary));
                for(final Mutation attached : input.attached()) {
                    contents.add(convertFailedRow(attached));
                }

                final MElement output = MElement.builder()
                        .withTimestamp("timestamp", timestamp)
                        .withPrimitiveValue("project", projectId)
                        .withPrimitiveValue("instance", instanceId)
                        .withPrimitiveValue("database", databaseId)
                        .withElementList("mutations", contents)
                        .build();

                c.output(output);
            }
        }

        private MElement convertFailedRow(final Mutation mutation) {
            final JsonObject jsonObject = MutationToJsonConverter.convert(mutation);
            return MElement.builder()
                    .withString("table", mutation.getTable())
                    .withString("op", mutation.getOperation().name())
                    .withString("mutation", jsonObject.toString())
                    .build();
        }

        private MElement convertFailedFlatRow(final Mutation mutation, final Instant timestamp) {
            final JsonObject jsonObject = MutationToJsonConverter.convert(mutation);
            return MElement.builder()
                    .withString("id", UUID.randomUUID().toString())
                    .withTimestamp("timestamp", timestamp)
                    .withString("project", projectId)
                    .withString("instance", instanceId)
                    .withString("database", databaseId)
                    .withString("table", mutation.getTable())
                    .withString("op", mutation.getOperation().name())
                    .withPrimitiveValue("mutation", jsonObject.toString())
                    .build();
        }

        public static Schema createFailureSchema(boolean flatten) {
            if(flatten) {
                return Schema.builder()
                        .withField("id", Schema.FieldType.STRING)
                        .withField("timestamp", Schema.FieldType.TIMESTAMP)
                        .withField("project", Schema.FieldType.STRING)
                        .withField("instance", Schema.FieldType.STRING)
                        .withField("database", Schema.FieldType.STRING)
                        .withField("table", Schema.FieldType.STRING.withNullable(true))
                        .withField("op", Schema.FieldType.STRING.withNullable(true))
                        .withField("mutation", Schema.FieldType.JSON)
                        .build();
            } else {
                return Schema.builder()
                        .withField("timestamp", Schema.FieldType.TIMESTAMP)
                        .withField("project", Schema.FieldType.STRING)
                        .withField("instance", Schema.FieldType.STRING)
                        .withField("database", Schema.FieldType.STRING)
                        .withField("mutations", Schema.FieldType.array(Schema.FieldType.element(createFailureMutationSchema())))
                        .build();
            }
        }

        private static Schema createFailureMutationSchema() {
            return Schema.builder()
                    .withField("table", Schema.FieldType.STRING.withNullable(true))
                    .withField("op", Schema.FieldType.STRING.withNullable(true))
                    .withField(Schema.Field.of("mutation", Schema.FieldType.JSON))
                    .build();
        }

    }

    private static class WriteMutationDoFn extends DoFn<Mutation, Void> {

        private static final Logger LOG = LoggerFactory.getLogger(WriteMutationDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Integer bufferSize;
        private final Boolean emulator;

        private transient Spanner spanner;
        private transient DatabaseClient client;
        private transient List<Mutation> buffer;
        private transient Integer count;

        public WriteMutationDoFn(final String projectId, final String instanceId, final String databaseId,
                                 final Integer bufferSize, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.bufferSize = bufferSize;
            this.emulator = emulator;
        }

        @Setup
        public void setup() {
            this.spanner = SpannerUtil.connectSpanner(projectId, 1, 4, 8, !emulator, emulator);
            this.client = this.spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        }

        @StartBundle
        public void startBundle(StartBundleContext c) {
            this.count = 0;
            this.buffer = new ArrayList<>();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            this.buffer.add(c.element());
            if(this.buffer.size() >= bufferSize) {
                this.client.write(this.buffer);
                this.count += this.buffer.size();
                this.buffer.clear();
                LOG.info("write: " + count);
            }
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            if(!this.buffer.isEmpty()) {
                this.client.write(this.buffer);
                this.count += this.buffer.size();
                LOG.info("write: " + count);
            }
            this.buffer.clear();
        }

        @Teardown
        public void teardown() {
            if(this.spanner != null) {
                this.spanner.close();
            }
        }
    }

    private static class WriteMutationGroupDoFn extends DoFn<MutationGroup, Void> {

        private final String name;
        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Boolean emulator;

        private transient Spanner spanner;
        private transient DatabaseClient client;

        WriteMutationGroupDoFn(final String name,
                               final String projectId,
                               final String instanceId,
                               final String databaseId,
                               final Boolean emulator) {

            this.name = name;
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.emulator = emulator;
        }


        @Setup
        public void setup() {
            LOG.info("SpannerSink: " + name + " setup");
            this.spanner = SpannerUtil
                    .connectSpanner(projectId, 1, 1, 1, !emulator, emulator);
            this.client = this.spanner
                    .getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        }

        @ProcessElement
        public void processElement(final ProcessContext c){
            final MutationGroup mutationGroup = c.element();
            final List<Mutation> mutations = new ArrayList<>();
            if(mutationGroup == null) {
                return;
            }
            mutations.add(mutationGroup.primary());
            if(mutationGroup.attached() != null && !mutationGroup.attached().isEmpty()) {
                mutations.addAll(mutationGroup.attached());
            }
            this.client.writeAtLeastOnce(mutations);
        }

        @Teardown
        public void teardown() {
            LOG.info("SpannerSink: " + name + " teardown");
            if(this.spanner != null) {
                this.spanner.close();
            }
        }

    }

}
