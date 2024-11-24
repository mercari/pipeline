package com.mercari.solution.module.source;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Partition;
import com.google.gson.JsonElement;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.MicroBatch;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Source.Module(name="spanner")
public class SpannerSource extends Source {

    private static final Counter ERROR_COUNTER = Metrics.counter("spanner_source", "error");;

    private static class SpannerSourceParameters implements Serializable {

        // common parameters
        private Source.Mode mode;
        private String projectId;
        private String instanceId;
        private String databaseId;

        private String timestampBound;
        private Boolean enableDataBoost;
        private String requestTag;
        private Boolean emulator;

        // for query parameters
        private String query;
        private Options.RpcPriority priority;

        // for table parameters
        private String table;
        private List<String> fields;
        private List<KeyRangeParameter> keyRange;

        // for change stream parameter
        private ChangeStreamParameter changeStream;

        // for microBatch parameter
        private MicroBatch.MicroBatchParameter microBatch;

        // for view parameter
        private ViewParameter view;

        private static class KeyRangeParameter {

            private String startType;
            private String endType;
            private JsonElement startKeys;
            private JsonElement endKeys;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {

            }

        }

        private static class ChangeStreamParameter implements Serializable {

            private String changeStreamName;
            private String metadataInstance;
            private String metadataDatabase;
            private String metadataTable;
            private String inclusiveStartAt;
            private String inclusiveEndAt;

            public List<String> validate(
                    String name,
                    SpannerSourceParameters parentParameters) {

                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults(SpannerSourceParameters parentParameters) {

                if(this.metadataInstance == null) {
                    this.metadataInstance = parentParameters.instanceId;
                }
                if(this.metadataDatabase == null) {
                    this.metadataDatabase = parentParameters.databaseId;
                }
                if(this.inclusiveStartAt == null) {
                    this.inclusiveStartAt = Timestamp.now().toString();
                }
                if(this.inclusiveEndAt == null) {
                    this.inclusiveEndAt = Timestamp.MAX_VALUE.toString();
                }

            }

        }

        private static class ViewParameter implements Serializable {

            private String keyField;
            private Integer intervalMinute;

            public List<String> validate(String name) {
                final List<String> errorMessages = new ArrayList<>();
                if(keyField == null) {
                    errorMessages.add("spanner source module[" + name + "].view requires 'keyField' parameter");
                }
                return errorMessages;
            }

            private void setDefaults() {
                if(intervalMinute == null) {
                    intervalMinute = 60;
                }
            }

        }

        private void validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'projectId' parameter");
            }
            if(instanceId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'instanceId' parameter");
            }
            if(databaseId == null) {
                errorMessages.add("spanner source module[" + name + "] requires 'databaseId' parameter");
            }

            switch (mode) {
                case microBatch -> {
                    if(microBatch == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'microBatch' parameter if mode is 'microBatch'");
                    } else {
                        errorMessages.addAll(microBatch.validate(name));
                    }
                }
                case changeDataCapture -> {
                    if(changeStream == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'changeStream' parameter if mode is 'changeStream'");
                    } else {
                        errorMessages.addAll(changeStream.validate(name, this));
                    }
                }
                case view -> {
                    if(view == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'view' parameter if mode is 'view'");
                    } else {
                        errorMessages.addAll(view.validate(name));
                    }
                }
                case null, default -> {
                    if(query == null && table == null) {
                        errorMessages.add("spanner source module[" + name + "] requires 'query' or 'table' parameter if mode is 'batch' or default");
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if (priority == null) {
                this.priority = Options.RpcPriority.MEDIUM;
            }
            if (emulator == null) {
                this.emulator = false;
            }
            if(this.enableDataBoost == null) {
                this.enableDataBoost = false;
            }

            if(changeStream != null) {
                changeStream.setDefaults(this);
            }
            if(microBatch != null) {
                microBatch.setDefaults();
            }
            if(view != null) {
                view.setDefaults();
            }
        }
    }

    private enum Mode {
        batch,
        microBatch,
        changeStream,
        view
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {

        final SpannerSourceParameters parameters = getParameters(SpannerSourceParameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        return switch (getMode()) {
            case batch -> {
                final Type type;
                final PCollection<Struct> structs;
                if(parameters.query != null) {
                    final QuerySource source = new QuerySource(parameters, getTemplateArgs());
                    structs = begin.apply("Query", source);
                    type = source.type;
                } else {
                    final TableSource source = new TableSource(parameters);
                    structs = begin.apply("Table", source);
                    type = source.type;
                }

                final Schema outputSchema = Schema.of(type);
                final PCollection<MElement> output = structs
                        .apply("Format", ParDo.of(new WithTimestampDoFn(
                                getTimestampAttribute(), DateTimeUtil.toJodaInstant(getTimestampDefault()))));
                yield MCollectionTuple
                        .of(output, outputSchema);
            }
            case microBatch -> {
                yield null;
            }
            case changeDataCapture -> {
                final ChangeStreamSource source = new ChangeStreamSource(parameters);
                final PCollection<MMutation> mutation = begin.apply("ChangeStream", source);

                yield null;
                //yield MCollectionTuple
                //        .of(mutation, outputSchema);
            }
            case view -> {
                final TupleTag<MElement> outputTag = new TupleTag<>(){};
                final TupleTag<MElement> failureTag = new TupleTag<>(){};
                final ViewSource source = new ViewSource(getJobName(), getName(), parameters, outputTag, failureTag);
                final PCollectionTuple outputs = begin
                        .apply("Query", source);
                final PCollection<MElement> output = outputs.get(outputTag);
                // TODO
                final Type type = source.type;
                yield MCollectionTuple
                        .of(output, Schema.builder().build());
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static class QuerySource extends PTransform<PBegin, PCollection<Struct>> {

        private static final String SQL_SPLITTER = "--SPLITTER--";

        private final SpannerSourceParameters parameters;
        private final Map<String, Object> templateArgs;

        private Type type;

        QuerySource(final SpannerSourceParameters parameters, final Map<String, Object> templateArgs) {
            this.parameters = parameters;
            this.templateArgs = templateArgs;
        }

        @Override
        public PCollection<Struct> expand(PBegin begin) {

            final String rawQuery;
            if(parameters.query.startsWith("gs://")) {
                rawQuery = StorageUtil.readString(parameters.query);
            } else {
                rawQuery = parameters.query;
            }

            final String query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            this.type = SpannerUtil.getTypeFromQuery(parameters.projectId, parameters.instanceId, parameters.databaseId, query, parameters.emulator);

            final PCollectionView<Transaction> transactionView = begin
                    .apply(Create.of(1L))
                    .apply("CreateTransaction", ParDo.of(new CreateTransactionFn(parameters)))
                    .apply("AsView", View.asSingleton());

            final TupleTag<KV<String, KV<BatchTransactionId, Partition>>> tagPartition = new TupleTag<>(){};
            final TupleTag<Struct> tagStruct = new TupleTag<>(){};

            final PCollectionTuple results = begin
                    .apply("SupplyQuery", Create.of(query))
                    .apply("SplitQuery", FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))))
                    .apply("ExecuteQuery", ParDo.of(new QueryPartitionDoFn(
                                    parameters, transactionView, tagStruct))
                            .withSideInput("transactionView", transactionView)
                            .withOutputTags(tagPartition, TupleTagList.of(tagStruct)));

            final PCollection<Struct> struct1 = results.get(tagPartition)
                    .apply("GroupByPartition", GroupByKey.create())
                    .apply("ReadStruct", ParDo.of(new ReadStructDoFn(parameters, transactionView))
                            .withSideInput("transactionView", transactionView))
                    .setCoder(SerializableCoder.of(Struct.class));
            final PCollection<Struct> struct2 = results.get(tagStruct);
            return PCollectionList.of(struct1).and(struct2)
                    .apply(Flatten.pCollections());
        }

        public static class CreateTransactionFn extends DoFn<Object, Transaction> {

            private static final Logger LOG = LoggerFactory.getLogger(CreateTransactionFn.class);

            private final SpannerSourceParameters parameters;
            private final TimestampBound timestampBound;

            public CreateTransactionFn(final SpannerSourceParameters parameters) {
                this.parameters = parameters;
                this.timestampBound = toTimestampBound(parameters.timestampBound);
                LOG.info(String.format("TimestampBound: %s", timestampBound.toString()));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                try(final Spanner spanner = SpannerUtil
                        .connectSpanner(parameters.projectId, 1, 1, 1, true, parameters.emulator)) {

                    final BatchReadOnlyTransaction tx = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(timestampBound);
                    c.output(Transaction.create(tx.getBatchTransactionId()));
                } catch (final Throwable e) {
                    ERROR_COUNTER.inc();
                    LOG.error("Failed to create transaction cause: {}", e.getMessage());
                    throw new RuntimeException("Failed to create transaction", e);
                }
            }
        }

        public static class QueryPartitionDoFn extends DoFn<String, KV<String, KV<BatchTransactionId, Partition>>> {

            private static final Logger LOG = LoggerFactory.getLogger(QueryPartitionDoFn.class);

            private final SpannerSourceParameters parameters;
            private final PCollectionView<Transaction> transactionView;
            private final TupleTag<Struct> tagStruct;

            private QueryPartitionDoFn(
                    final SpannerSourceParameters parameters,
                    final PCollectionView<Transaction> transactionView,
                    final TupleTag<Struct> tagStruct) {

                this.parameters = parameters;
                this.transactionView = transactionView;
                this.tagStruct = tagStruct;
            }

            @Setup
            public void setup() {
                LOG.info("QueryPartitionDoFn.setup");
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String query = c.element();
                LOG.info(String.format("Received query [%s], timestamp bound [%s]", query, parameters.timestampBound));
                final Statement statement = Statement.of(query);
                final Transaction tx = c.sideInput(transactionView);

                try(final Spanner spanner = SpannerUtil.connectSpanner(
                        parameters.projectId, 1, 1, 1, true, parameters.emulator)) {
                    final BatchReadOnlyTransaction transaction = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(tx.transactionId());

                    final PartitionOptions options = PartitionOptions.newBuilder()
                            //.setMaxPartitions(10000) // Note: this hint is currently ignored in v1.
                            //.setPartitionSizeBytes(100000000) // Note: this hint is currently ignored in v1.
                            .build();

                    final Options.ReadQueryUpdateTransactionOption tagOption = createSpannerRequestTag(c.getPipelineOptions(), parameters.requestTag);
                    try {
                        final List<com.google.cloud.spanner.Partition> partitions = transaction
                                .partitionQuery(options, statement, tagOption,
                                        Options.priority(parameters.priority),
                                        Options.dataBoostEnabled(parameters.enableDataBoost));
                        LOG.info(String.format("Query [%s] divided to [%d] partitions.", query, partitions.size()));
                        for (int i = 0; i < partitions.size(); ++i) {
                            final KV<BatchTransactionId, Partition> value = KV.of(transaction.getBatchTransactionId(), partitions.get(i));
                            final String key = String.format("%d-%s", i, query);
                            final KV<String, KV<BatchTransactionId, Partition>> kv = KV.of(key, value);
                            c.output(kv);
                        }
                    } catch (SpannerException e) {
                        if (!e.getErrorCode().equals(ErrorCode.INVALID_ARGUMENT)) {
                            throw e;
                        }
                        LOG.warn(String.format("Query [%s] could not be executed. Retrying as single query.", query));
                        try (final ResultSet resultSet = transaction.executeQuery(statement, tagOption,
                                Options.priority(parameters.priority),
                                Options.dataBoostEnabled(parameters.enableDataBoost))) {
                            int count = 0;
                            while (resultSet.next()) {
                                c.output(tagStruct, resultSet.getCurrentRowAsStruct());
                                count++;
                            }
                            LOG.info(String.format("Query read record num [%d]", count));
                        }
                    }
                }

            }

            private Options.ReadQueryUpdateTransactionOption createSpannerRequestTag(
                    final PipelineOptions options,
                    final String vtag) {

                final String project = DataflowOptions.getProject(options);
                final String jobName = options.getJobName();
                final String serviceAccount = DataflowOptions.getServiceAccount(options);

                final String tag = String.format("job=%s,sa=%s,project=%s", jobName, serviceAccount, project);
                if(vtag != null) {
                    return Options.tag(vtag + "," + tag);
                }
                return Options.tag(tag);
            }

        }

        public static class ReadStructDoFn extends DoFn<KV<String, Iterable<KV<BatchTransactionId, Partition>>>, Struct> {

            private static final Logger LOG = LoggerFactory.getLogger(ReadStructDoFn.class);

            private final SpannerSourceParameters parameters;
            private final PCollectionView<Transaction> transactionView;
            private transient Spanner spanner;
            private transient BatchClient batchClient;

            private ReadStructDoFn(
                    final SpannerSourceParameters parameters,
                    final PCollectionView<Transaction> transactionView) {

                this.parameters = parameters;
                this.transactionView = transactionView;
            }

            @Setup
            public void setup() {
                LOG.info("ReadStructDoFn.setup");
                this.spanner = SpannerUtil.connectSpanner(parameters.projectId, 1, 1, 1, true, parameters.emulator);
                this.batchClient = spanner.getBatchClient(DatabaseId.of(
                        parameters.projectId, parameters.instanceId, parameters.databaseId));
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final KV<String, Iterable<KV<BatchTransactionId, com.google.cloud.spanner.Partition>>> kv = c.element();
                final String partitionNumberQuery = kv.getKey();
                final KV<BatchTransactionId, com.google.cloud.spanner.Partition> value = kv.getValue().iterator().next();

                final Transaction tx = c.sideInput(transactionView);
                final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(tx.transactionId()); // DO NOT CLOSE!!!
                final Partition partition = value.getValue();

                try(final ResultSet resultSet = transaction.execute(partition)) {
                    LOG.info(String.format("Started %s th partition[%s] query.", partitionNumberQuery.split("-")[0], partition));
                    int count = 0;
                    while (resultSet.next()) {
                        c.output(resultSet.getCurrentRowAsStruct());
                        count++;
                    }
                    LOG.info(String.format("%s th partition completed to read record: [%d]", partitionNumberQuery.split("-")[0], count));
                }
            }

            @Teardown
            public void teardown() {
                if(this.spanner != null) {
                    this.spanner.close();
                }
                LOG.info("ReadStructDoFn.teardown");
            }

        }

    }

    private static class TableSource extends PTransform<PBegin, PCollection<Struct>> {

        private final SpannerSourceParameters parameters;
        private Type type;

        TableSource(final SpannerSourceParameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public PCollection<Struct> expand(PBegin begin) {

            this.type = SpannerUtil.getTypeFromTable(
                    parameters.projectId, parameters.instanceId, parameters.databaseId,
                    parameters.table, parameters.fields, parameters.emulator);

            // TODO check columns exists in table
            final List<String> columns = type.getStructFields().stream()
                    .map(Type.StructField::getName)
                    .collect(Collectors.toList());
            final List<SpannerSourceParameters.KeyRangeParameter> keyRanges = parameters.keyRange;
            final KeySet keySet = createKeySet(parameters, type);

            SpannerConfig config = SpannerConfig.create()
                    .withProjectId(parameters.projectId)
                    .withInstanceId(parameters.instanceId)
                    .withDatabaseId(parameters.databaseId)
                    .withDataBoostEnabled(ValueProvider.StaticValueProvider.of(parameters.enableDataBoost))
                    .withRpcPriority(parameters.priority);

            if(parameters.emulator) {
                config = config.withEmulatorHost(ValueProvider.StaticValueProvider.of(SpannerUtil.SPANNER_HOST_EMULATOR));
            }

            final SpannerIO.Read read = SpannerIO.read()
                    .withSpannerConfig(config)
                    .withTable(parameters.table)
                    .withKeySet(keySet)
                    .withColumns(columns)
                    .withBatching(true)
                    .withTimestampBound(toTimestampBound(parameters.timestampBound));

            return begin.apply("ReadSpannerTable", read);
        }

        private static KeySet createKeySet(
                final SpannerSourceParameters parameters,
                final Type type) {

            if(parameters.keyRange == null) {
                return KeySet.all();
            } else {
                final List<String> keyFieldNames = SpannerUtil.getPrimaryKeyFieldNames(
                        parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.table, parameters.emulator);
                final List<Type.StructField> keyFields = keyFieldNames.stream()
                        .map(f -> type.getStructFields().stream()
                                .filter(s -> s.getName().equals(f))
                                .findAny()
                                .orElseThrow(() -> new IllegalArgumentException("PrimaryKey: " + f + " not found!")))
                        .collect(Collectors.toList());

                final KeySet.Builder builder = KeySet.newBuilder();
                for(final SpannerSourceParameters.KeyRangeParameter keyRangeParameter : parameters.keyRange) {
                    final KeyRange.Endpoint startType;
                    if(keyRangeParameter.startType == null) {
                        startType = KeyRange.Endpoint.CLOSED;
                    } else {
                        startType = "open".equalsIgnoreCase(keyRangeParameter.startType) ?
                                KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                    }

                    final KeyRange.Endpoint endType;
                    if(keyRangeParameter.endType == null) {
                        endType = KeyRange.Endpoint.CLOSED;
                    } else {
                        endType = "open".equalsIgnoreCase(keyRangeParameter.endType) ?
                                KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                    }
                    final Key start = createRangeKey(keyFields, keyRangeParameter.startKeys);
                    final Key end   = createRangeKey(keyFields, keyRangeParameter.endKeys);

                    builder.addRange(KeyRange.newBuilder()
                            .setStartType(startType)
                            .setEndType(endType)
                            .setStart(start)
                            .setEnd(end)
                            .build());
                }
                return builder.build();
            }
        }

        private static Key createRangeKey(final List<Type.StructField> keyFields, final JsonElement keyValues) {
            final Key.Builder key = Key.newBuilder();
            if(keyValues == null) {
                return key.build();
            }
            if(keyValues.isJsonPrimitive()) {
                final Type.StructField field = keyFields.get(0);
                setRangeKey(key, field, keyValues);
            } else {
                for(int i=0; i< keyValues.getAsJsonArray().size(); i++) {
                    final Type.StructField field = keyFields.get(i);
                    setRangeKey(key, field, keyValues.getAsJsonArray().get(i));
                }
            }
            return key.build();
        }

        private static void setRangeKey(final Key.Builder key, final Type.StructField field, final JsonElement element) {
            switch (field.getType().getCode()) {
                case STRING -> key.append(element.getAsString());
                case INT64 -> key.append(element.getAsLong());
                case FLOAT64 -> key.append(element.getAsDouble());
                case BOOL -> key.append(element.getAsBoolean());
                case DATE -> key.append(Date.parseDate(element.getAsString()));
                case TIMESTAMP -> key.append(Timestamp.parseTimestamp(element.getAsString()));
                default -> {
                }
            }
        }
    }

    private static class ChangeStreamSource extends PTransform<PBegin, PCollection<MMutation>> {

        private final SpannerSourceParameters parameters;

        ChangeStreamSource(final SpannerSourceParameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public PCollection<MMutation> expand(PBegin begin) {
            final SpannerIO.ReadChangeStream readChangeStream = createDataChangeRecordSource(parameters);
            final PCollection<DataChangeRecord> dataChangeRecords = begin
                    .apply("ReadChangeStream", readChangeStream);

            DataChangeRecord a;
            return null;
        }

        private static SpannerIO.ReadChangeStream createDataChangeRecordSource(
                final SpannerSourceParameters parameters) {

            final SpannerConfig spannerConfig = SpannerConfig.create()
                    .withHost(ValueProvider.StaticValueProvider.of(SpannerUtil.SPANNER_HOST_BATCH))
                    .withProjectId(parameters.projectId)
                    .withInstanceId(parameters.instanceId)
                    .withDatabaseId(parameters.databaseId)
                    .withDataBoostEnabled(ValueProvider.StaticValueProvider.of(parameters.enableDataBoost));

            SpannerIO.ReadChangeStream readChangeStream = SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(parameters.changeStream.changeStreamName)
                    .withMetadataInstance(parameters.changeStream.metadataInstance)
                    .withMetadataDatabase(parameters.changeStream.metadataDatabase)
                    .withRpcPriority(parameters.priority);

            if(parameters.changeStream.inclusiveStartAt != null) {
                final Timestamp inclusiveStartAt = Timestamp.parseTimestamp(parameters.changeStream.inclusiveStartAt);
                readChangeStream = readChangeStream.withInclusiveStartAt(inclusiveStartAt);
            }
            if(parameters.changeStream.inclusiveEndAt != null) {
                final Timestamp inclusiveEndAt = Timestamp.parseTimestamp(parameters.changeStream.inclusiveEndAt);
                readChangeStream = readChangeStream.withInclusiveEndAt(inclusiveEndAt);
            }
            if(parameters.changeStream.metadataTable != null) {
                readChangeStream = readChangeStream.withMetadataTable(parameters.changeStream.metadataTable);
            }

            return readChangeStream;
        }

    }

    private static class ViewSource extends PTransform<PBegin, PCollectionTuple> {

        private final String jobName;
        private final String moduleName;
        private final SpannerSourceParameters parameters;
        private final TupleTag<MElement> outputTag;
        private final TupleTag<MElement> failuresTag;

        private Type type;

        ViewSource(
                final String jobName,
                final String moduleName,
                final SpannerSourceParameters parameters,
                final TupleTag<MElement> outputTag,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.parameters = parameters;
            this.outputTag = outputTag;
            this.failuresTag = failuresTag;
        }

        @Override
        public PCollectionTuple expand(PBegin begin) {

            type = SpannerUtil.getTypeFromQuery(
                    parameters.projectId, parameters.instanceId, parameters.databaseId, parameters.query, parameters.emulator);

            final PCollection<Long> sequence;
            if(OptionUtil.isStreaming(begin)) {
                sequence = begin
                        .apply("Generate", GenerateSequence
                                .from(0)
                                .withRate(1, Duration.standardMinutes(parameters.view.intervalMinute)));
            } else {
                sequence = begin
                        .apply("Create", Create.of(1L).withCoder(VarLongCoder.of()));
            }

            return sequence
                    .apply(ParDo.of(new QueryMapDoFn(jobName, moduleName, parameters, failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        private static class QueryMapDoFn extends DoFn<Long, MElement> {

            private final String jobName;
            private final String name;
            private final SpannerSourceParameters parameters;
            private final TupleTag<MElement> failuresTag;

            QueryMapDoFn(
                    final String jobName,
                    final String name,
                    final SpannerSourceParameters parameters,
                    final TupleTag<MElement> failuresTag) {

                this.jobName = jobName;
                this.name = name;
                this.parameters = parameters;
                this.failuresTag = failuresTag;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final List<Struct> structs = new ArrayList<>();
                final Statement statement = Statement.of(parameters.query);
                try(final Spanner spanner = SpannerUtil.connectSpanner(
                        parameters.projectId, 1, 1, 1, true, parameters.emulator);
                    final BatchReadOnlyTransaction tx = spanner
                            .getBatchClient(DatabaseId.of(
                                    parameters.projectId, parameters.instanceId, parameters.databaseId))
                            .batchReadOnlyTransaction(TimestampBound.strong());
                    final ResultSet resultSet = tx.executeQuery(statement,
                            Options.priority(parameters.priority),
                            Options.dataBoostEnabled(parameters.enableDataBoost))) {

                    while (resultSet.next()) {
                        final Struct struct = resultSet.getCurrentRowAsStruct();
                        structs.add(struct);
                    }
                } catch (final Throwable e) {
                    final MElement failure = MFailure
                            .of(jobName, name, parameters.query, e, c.timestamp())
                            .toElement(c.timestamp());
                    c.output(failuresTag, failure);
                }

                final Map<String ,Object> map = new HashMap<>();
                for(final Struct struct : structs) {
                    String key = struct.getString(parameters.view.keyField);
                    Map<String, Object> values = StructSchemaUtil.asPrimitiveMap(struct);
                    map.put(key, values);
                }
                System.out.println("size: " + UnionMapCoder.serializeSize(map));
                final MElement output = MElement.of(map, c.timestamp());
                c.output(output);
            }

        }
    }

    private static class WithTimestampDoFn extends DoFn<Struct, MElement> {

        private final String timestampAttribute;
        private final Instant timestampDefault;

        private WithTimestampDoFn(
                final String timestampAttribute,
                final Instant timestampDefault) {

            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault == null ? Instant.ofEpochSecond(0L) : timestampDefault;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Struct struct = c.element();
            if(struct == null) {
                return;
            }
            if(timestampAttribute == null) {
                final MElement element = MElement.of(struct, c.timestamp());
                c.output(element);
            } else {
                final Instant timestamp = StructSchemaUtil.getTimestamp(struct, timestampAttribute, timestampDefault);
                final MElement element = MElement.of(struct, timestamp);
                c.outputWithTimestamp(element, timestamp);
            }
        }

    }

    private static TimestampBound toTimestampBound(final String timestampBoundString) {
        if(timestampBoundString == null) {
            return TimestampBound.strong();
        } else {
            try {
                final Instant instant = Instant.parse(timestampBoundString);
                final com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
                return TimestampBound.ofReadTimestamp(timestamp);
            } catch (Exception e) {
                return TimestampBound.strong();
            }
        }
    }
}
