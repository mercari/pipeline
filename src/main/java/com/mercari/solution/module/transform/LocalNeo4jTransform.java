package com.mercari.solution.module.transform;

import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.search.Neo4jUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
//import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


//@Transform.Module(name="localNeo4j")
public class LocalNeo4jTransform {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNeo4jTransform.class);

    public static class LocalNeo4jTransformParameters implements Serializable {

        private List<String> groupFields;
        private IndexDefinition index;
        private List<QueryDefinition> queries;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public IndexDefinition getIndex() {
            return index;
        }

        public List<QueryDefinition> getQueries() {
            return queries;
        }


        public void validate(final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.index == null) {
                errorMessages.add("parameters.index must not be null.");
            } else {
                errorMessages.addAll(this.index.validate(inputNames));
            }
            if(this.queries == null || this.queries.isEmpty()) {
                errorMessages.add("parameters.queries must not be empty");
            } else {
                for(int i=0; i<queries.size(); i++) {
                    errorMessages.addAll(queries.get(i).validate(i, inputNames));
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults(final List<String> inputNames, final List<Schema> inputSchemas) {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            this.index.setDefaults();
            for(final QueryDefinition queryDefinition : this.queries) {
                int index = inputNames.indexOf(queryDefinition.getInput());
                queryDefinition.setDefaults(inputSchemas.get(index));
            }
        }

    }

    public static class IndexDefinition implements Serializable {

        private String path;
        private String database;
        private String conf;
        private List<Neo4jUtil.NodeConfig> nodes;
        private List<Neo4jUtil.RelationshipConfig> relationships;
        private List<String> setupCyphers;
        private List<String> teardownCyphers;
        private Boolean useGDS;
        private Boolean mutable;
        private Integer bufferSize;

        public String getPath() {
            return path;
        }

        public String getDatabase() {
            return database;
        }

        public String getConf() {
            return conf;
        }

        public List<Neo4jUtil.NodeConfig> getNodes() {
            return nodes;
        }

        public List<Neo4jUtil.RelationshipConfig> getRelationships() {
            return relationships;
        }

        public List<String> getSetupCyphers() {
            return setupCyphers;
        }

        public List<String> getTeardownCyphers() {
            return teardownCyphers;
        }

        public Boolean getUseGDS() {
            return useGDS;
        }

        public Boolean getMutable() {
            return mutable;
        }

        public Integer getBufferSize() {
            return bufferSize;
        }

        public List<String> validate(final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.path == null) {
                errorMessages.add("parameters.index.path must not be null");
            }
            if((nodes == null || nodes.isEmpty()) && (relationships == null || relationships.isEmpty())) {
                errorMessages.add("parameters.nodes or relationships must not be null");
            } else {
                if(nodes != null) {
                    for(int i=0; i<nodes.size(); i++) {
                        errorMessages.addAll(nodes.get(i).validate(i));
                        if(!inputNames.contains(nodes.get(i).getInput())) {
                            errorMessages.add("parameters.nodes[" + i + "].input does not exist in module inputs: " + inputNames);
                        }
                    }
                }
                if(relationships != null) {
                    for(int i=0; i<relationships.size(); i++) {
                        errorMessages.addAll(relationships.get(i).validate(i));
                        if(!inputNames.contains(relationships.get(i).getInput())) {
                            errorMessages.add("parameters.relationships[" + i + "].input does not exist in module inputs: " + inputNames);
                        }
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(database == null) {
                database = Neo4jUtil.DEFAULT_DATABASE_NAME;
            }
            if(nodes == null) {
                nodes = new ArrayList<>();
            } else {
                nodes.forEach(Neo4jUtil.NodeConfig::setDefaults);
            }
            if(relationships == null) {
                relationships = new ArrayList<>();
            } else {
                relationships.forEach(Neo4jUtil.RelationshipConfig::setDefaults);
            }
            if(setupCyphers == null) {
                setupCyphers = new ArrayList<>();
            }
            if(teardownCyphers == null) {
                teardownCyphers = new ArrayList<>();
            }
            if(useGDS == null) {
                useGDS = false;
            }
            if(mutable == null) {
                mutable = nodes.isEmpty() && relationships.isEmpty();
            }
            if(bufferSize == null) {
                bufferSize = 500;
            }
        }

    }

    public static class QueryDefinition implements Serializable {

        private String name;
        private String input;
        private List<String> fields;
        private String cypher;
        private JsonElement schema;
        private List<String> requiredFields;
        private transient Template cypherTemplate;

        public String getName() {
            return name;
        }

        public String getInput() {
            return input;
        }

        public List<String> getFields() {
            return fields;
        }

        public String getCypher() {
            return cypher;
        }

        public JsonElement getSchema() {
            return schema;
        }

        public List<String> getRequiredFields() {
            return requiredFields;
        }

        public Template getCypherTemplate() {
            return cypherTemplate;
        }

        public List<String> validate(int i, final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.name == null) {
                errorMessages.add("parameters.queries[" + i + "].name must not be null.");
            }
            if(this.input == null) {
                errorMessages.add("parameters.queries[" + i + "].input must not be null.");
            } else if(!inputNames.contains(this.input)) {
                errorMessages.add("parameters.queries[" + i + "].input does not exists in module inputs: " + inputNames);
            }
            if(this.cypher == null) {
                errorMessages.add("parameters.queries[" + i + "].cypher must not be null.");
            }
            if(this.schema == null) {
                errorMessages.add("parameters.queries[" + i + "].schema must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults(final Schema inputSchema) {
            if(fields == null) {
                fields = new ArrayList<>();
            }
            requiredFields = new ArrayList<>(this.fields);
            final List<String> cypherTemplateArgs = TemplateUtil.extractTemplateArgs(cypher, inputSchema);
            for(final String arg : cypherTemplateArgs) {
                if(!requiredFields.contains(arg)) {
                    requiredFields.add(arg);
                }
            }
        }

        public void setup() {
            this.cypherTemplate = TemplateUtil.createStrictTemplate(name + "Cypher", cypher);
        }

    }

            /*
    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final LocalNeo4jTransformParameters parameters = getParameters(LocalNeo4jTransformParameters.class);
        parameters.validate(inputs.getAllInputs());
        parameters.setDefaults(inputs.getAllInputs(), inputs.getAllSchema());

        final Map<String, Schema> inputSchemas = inputs.getAllSchemaAsMap();

        final List<KV<TupleTag<MElement>, KV<String, Schema>>> outputNameAndTagsAndSchemas = createOutputTagsAndSchemas(
                parameters.getQueries(), inputSchemas);
        final Map<TupleTag<MElement>, String> outputNames = outputNameAndTagsAndSchemas.stream()
                .collect(Collectors.toMap(KV::getKey, kv -> kv.getValue().getKey()));
        final Map<TupleTag<MElement>, Schema> outputSchemas = outputNameAndTagsAndSchemas.stream()
                .collect(Collectors.toMap(KV::getKey, kv -> kv.getValue().getValue()));

        final TupleTag<MElement> outputFailureTag = new TupleTag<>() {};

        final Map<String, TupleTag<MElement>> outputTags = outputNames.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        final Map<String, Schema> outputInputSchemas = new HashMap<>();
        for(final Map.Entry<TupleTag<MElement>, String> entry : outputNames.entrySet()) {
            outputInputSchemas.put(entry.getValue(), outputSchemas.get(entry.getKey()));
        }

        final String processName;
        final DoFn<KV<String, MElement>, MElement> dofn;
        if(parameters.index.getMutable()) {
            processName = "Query";
            dofn = new Neo4jQueryDoFn(getName(),
                    inputs.getAllInputs(), outputInputSchemas, parameters.index, parameters.queries,
                    outputTags, outputFailureTag);
        } else {
            processName = "IndexAndQuery";
            final Coder<MElement> unionCoder = ElementCoder.of(inputs.getAllSchema());
            if(OptionUtil.isStreaming(inputs)) {
                dofn = new Neo4jQueryAndIndexStreamingDoFn(getName(),
                        inputs.getAllInputs(), outputInputSchemas, parameters.index, parameters.queries,
                        outputTags, outputFailureTag,
                        unionCoder);
            } else {
                dofn = new Neo4jQueryAndIndexBatchDoFn(getName(),
                        inputs.getAllInputs(), outputInputSchemas, parameters.index, parameters.queries,
                        outputTags, outputFailureTag,
                        unionCoder);
            }
        }

        final TupleTagList tupleTagList = TupleTagList.of(outputTags.values().stream()
                .filter(t -> !t.getId().equals(outputFailureTag.getId()))
                .collect(Collectors.toList()));

        final PCollectionTuple outputs = inputs
                .apply("Union", Union
                        .withKeys(parameters.groupFields)
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply(processName, ParDo.of(dofn)
                        .withOutputTags(outputFailureTag, tupleTagList));

        MCollectionTuple outputTuple = MCollectionTuple.empty(inputs.getPipeline());
        for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : outputs.getAll().entrySet()) {
            final String outputName = outputNames.get(entry.getKey());
            final Schema outputSchema = outputSchemas.get(entry.getKey());
            outputTuple = outputTuple.and(outputName, (PCollection<MElement>) entry.getValue(), outputSchema);
        }
        return outputTuple;
        return null;
    }
         */

    private Schema createOutputSchema(final QueryDefinition query, final Schema inputSchema, final Schema resultSchema) {
        Schema.Builder builder = Schema.builder();
        for(final String field : query.getFields()) {
            builder = builder.withField(field, inputSchema.getField(field).getFieldType().withNullable(true));
        }
        return builder
                .withField("cypher", Schema.FieldType.STRING.withNullable(true))
                .withField("results", Schema.FieldType.array(Schema.FieldType.element(resultSchema)).withNullable(true))
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    private List<KV<TupleTag<MElement>, KV<String, Schema>>> createOutputTagsAndSchemas(
            final List<QueryDefinition> queries,
            final Map<String, Schema> inputSchemas) {

        final List<KV<TupleTag<MElement>, KV<String,Schema>>> outputs = new ArrayList<>();
        for(final QueryDefinition query : queries) {
            final TupleTag<MElement> tag = new TupleTag<>(){};
            final Schema resultSchema = Schema.parse(query.getSchema());
            final Schema outputSchema = createOutputSchema(query, inputSchemas.get(query.getInput()), resultSchema);
            outputs.add(KV.of(tag, KV.of(query.getName(), outputSchema)));
        }
        return outputs;
    }

    /*
    private static class Neo4jDoFn extends DoFn<KV<String, MElement>, MElement> {

        private static final String NEO4J_HOME = "/neo4j/";

        private final String name;
        private final String indexPath;
        protected final List<String> inputNames;
        private final Map<String, Schema> outputSchemas;
        private final Map<String, Schema> outputResultSchemas;
        private final IndexDefinition index;
        private final List<QueryDefinition> queries;
        private final Map<String, TupleTag<MElement>> outputTags;
        private final TupleTag<MElement> failureTag;

        //private static GraphDatabaseService graphDB;

        Neo4jDoFn(final String name,
                  final List<String> inputNames,
                  final Map<String, Schema> outputSchemas,
                  final IndexDefinition index,
                  final List<QueryDefinition> queries,
                  final Map<String, TupleTag<MElement>> outputTags,
                  final TupleTag<MElement> failureTag) {

            this.name = name;
            this.indexPath = NEO4J_HOME + name + "/";
            this.inputNames = inputNames;
            this.outputSchemas = outputSchemas;
            this.outputResultSchemas = this.outputSchemas.entrySet().stream()
                    .filter(e -> e.getValue().hasField("results"))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getField("results").getFieldType().getArrayValueType().getElementSchema()));
            this.index = index;
            this.queries = queries;

            this.outputTags = outputTags;
            this.failureTag = failureTag;
        }

        synchronized protected void setupIndex() throws IOException {
            setupIndex(this.indexPath, this.index);
        }

        protected void setupQuery() {
            for(final QueryDefinition queryDefinition : queries) {
                queryDefinition.setup();
            }
        }

        synchronized protected void teardownIndex() throws IOException {
            if(!index.getTeardownCyphers().isEmpty()) {
                try(final Transaction tx = graphDB.beginTx()) {
                    for(final String teardownCypher : index.getTeardownCyphers()) {
                        final Result result = tx.execute(teardownCypher);
                        LOG.info("teardown cypher query: " + teardownCypher + ". result: " + result.resultAsString());
                    }
                    tx.commit();
                }
            }

            //ZipFileUtil.uploadZipFile(this.indexPath, index.path);
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            Neo4jUtil.dump(this.indexPath, index.getDatabase(), os);
            os.flush();
            os.close();
            StorageUtil.writeBytes(this.index.getPath(), os.toByteArray(), "application/zstd", new HashMap<>(), new HashMap<>());
        }

        protected void query(final ProcessContext c) {
            final MElement element = c.element().getValue();
            final Instant timestamp = c.timestamp();
            final String inputName = inputNames.get(element.getIndex());
            try(final Transaction tx = graphDB.beginTx()){
                for(final QueryDefinition query : queries) {
                    if(!inputName.equals(query.getInput())) {
                        continue;
                    }

                    final Map<String, Object> input = element.asPrimitiveMap(query.getRequiredFields());
                    final String cypher = TemplateUtil.executeStrictTemplate(query.getCypherTemplate(), input);

                    try(final Result result = tx.execute(cypher)) {
                        final TupleTag<MElement> outputTag = outputTags.get(query.name);
                        final MElement output = createOutput(query, input, cypher, result, timestamp.getMillis());
                        c.output(outputTag, output);
                    } catch (final Throwable e) {
                        final String message = "Failed to execute cypher: " + cypher + ". cause: " + e.getMessage();
                        LOG.error(message);
                        final MElement outputFailure = createFailure(inputName, "query", cypher, timestamp.getMillis(), e);
                        c.output(outputFailure);
                    }
                }
            } catch (final Throwable e) {
                LOG.error("Failed to begin transaction cause: " + e.getMessage());
                final MElement outputFailure  = createFailure(inputName, "query", null, timestamp.getMillis(), e);
                c.output(outputFailure);
            }
        }

        protected void index(final List<MElement> buffer) {
            Neo4jUtil.index(graphDB, buffer, index.getNodes(), index.getRelationships(), inputNames);
            buffer.clear();
        }

        protected boolean isQueryInput(MElement input) {
            for(final QueryDefinition query : queries) {
                if(inputNames.get(input.getIndex()).equals(query.getInput())) {
                    return true;
                }
            }
            return false;
        }

        private MElement createOutput(QueryDefinition query, Map<String, Object> input, String cypher, Result result, long epochMillis) {
            // create results
            final List<MElement> outputResults = new ArrayList<>();
            while(result.hasNext()) {
                final Map<String, Object> resultValues = result.next();
                final MElement outputResult = MElement.builder().withPrimitiveValues(resultValues).build();
                outputResults.add(outputResult);
            }

            // create output
            final Map<String, Object> outputValues = new HashMap<>();
            for(final String field : query.getFields()) {
                outputValues.put(field, input.get(field));
            }
            outputValues.put("cypher", cypher);
            outputValues.put("results", outputResults);
            outputValues.put("timestamp", epochMillis * 1000L);
            return MElement.of(outputValues, epochMillis);
        }

        private MElement createFailure(final String input, final String type, final String cypher, final long epochMillis, final Throwable e) {
            final Map<String, Object> values = new HashMap<>();
            values.put("input", input);
            values.put("type", type);
            values.put("cypher", cypher);
            values.put("message", e.getMessage());
            values.put("timestamp", epochMillis * 1000L);

            final Instant instant = Instant.ofEpochMilli(epochMillis);
            return MFailure.of("", "", input, e, instant).toElement(instant);
        }

        static synchronized protected void setupIndex(String indexPath, IndexDefinition index) throws IOException {

            LOG.info("Start setup database PID: " + ProcessHandle.current().pid() + ", ThreadID:" + Thread.currentThread().getId());

            final Path indexDirPath = Paths.get(indexPath);
            final File indexDir = indexDirPath.toFile();
            if(!indexDir.exists()) {
                indexDir.mkdir();
                if(StorageUtil.exists(index.getPath())) {
                    Neo4jUtil.load(indexPath, index.getDatabase(), index.getPath());
                    //ZipFileUtil.downloadZipFiles(index.getPath(), indexPath);
                    LOG.info("Load Neo4j initial database file from: " + index.getPath() + " to " + indexPath);
                } else if(index.getPath() != null) {
                    LOG.warn("Not found Neo4j initial database file: " + index.getPath());
                }
            }

            if(graphDB == null) {
                LOG.info("Start neo4j database");
                final DatabaseManagementService service = new DatabaseManagementServiceBuilder(indexDirPath).build();
                graphDB = service.database(index.getDatabase());
                if(index.getUseGDS()) {
                    Neo4jUtil.setupGds(graphDB);
                }
                if(!index.getSetupCyphers().isEmpty()) {
                    try(final Transaction tx = graphDB.beginTx()) {
                        for(final String setupCypher : index.getSetupCyphers()) {
                            final Result result = tx.execute(setupCypher);
                            LOG.info("setup cypher query: " + setupCypher + ". result: " + result.resultAsString());
                        }
                        tx.commit();
                    }
                }
                Neo4jUtil.registerShutdownHook(service);
            }

            LOG.info("Finish setup database");
        }

        static synchronized protected void teardownIndex(String indexPath, IndexDefinition index) {
            if(graphDB != null) {
                graphDB = null;
            }
        }

    }

    private static class Neo4jQueryDoFn extends Neo4jDoFn {

        Neo4jQueryDoFn(final String name,
                       final List<String> inputNames,
                       final Map<String, Schema> outputSchemas,
                       final IndexDefinition index,
                       final List<QueryDefinition> queries,
                       final Map<String, TupleTag<MElement>> outputTags,
                       final TupleTag<MElement> failureTag) {

            super(name, inputNames, outputSchemas, index, queries, outputTags, failureTag);
        }

        @Setup
        synchronized public void setup() throws IOException {
            super.setupIndex();
            super.setupQuery();
        }

        @Teardown
        public void teardown() {

        }

        @ProcessElement
        public void processElement(final ProcessContext c) {

            final MElement input = c.element().getValue();
            if(isQueryInput(input)) {
                query(c);
            } else {
                LOG.info("not query input: " + input + " for inputNames: " + inputNames);
            }
        }

    }

    private static class Neo4jQueryAndIndexDoFn extends Neo4jDoFn {

        protected static final String STATE_ID_INDEX_BUFFER = "indexBuffer";
        protected static final String STATE_ID_INTERVAL_COUNTER = "intervalCounter";

        Neo4jQueryAndIndexDoFn(final String name,
                               final List<String> inputNames,
                               final Map<String, Schema> outputSchemas,
                               final IndexDefinition index,
                               final List<QueryDefinition> queries,
                               final Map<String, TupleTag<MElement>> outputTags,
                               final TupleTag<MElement> failureTag) {

            super(name, inputNames, outputSchemas, index, queries, outputTags, failureTag);
        }

        protected void setup() throws IOException {
            super.setupIndex();
            super.setupQuery();
        }

        protected void teardown() throws IOException {
            //super.uploadIndex();
        }

        protected void processElement(final ProcessContext c,
                                      final ValueState<List<MElement>> bufferIndexBufferValueState,
                                      final ValueState<Integer> bufferUpdateIntervalValueState) {

            final List<MElement> buffer = Optional
                    .ofNullable(bufferIndexBufferValueState.read())
                    .orElseGet(ArrayList::new);

            final MElement input = c.element().getValue();
            buffer.add(input);

            final boolean isQueryInput = isQueryInput(input);

            // indexing
            if(isQueryInput || buffer.size() > 100) {
                index(buffer);
                bufferIndexBufferValueState.clear();
            } else {
                bufferIndexBufferValueState.write(buffer);
            }

            // query
            if(isQueryInput) {
                query(c);
            }

        }

    }

    protected static class Neo4jQueryAndIndexBatchDoFn extends Neo4jQueryAndIndexDoFn {

        @StateId(STATE_ID_INDEX_BUFFER)
        private final StateSpec<ValueState<List<MElement>>> indexBufferSpec;
        @StateId(STATE_ID_INTERVAL_COUNTER)
        private final StateSpec<ValueState<Integer>> bufferIntervalCounterSpec;

        Neo4jQueryAndIndexBatchDoFn(
                final String name,
                final List<String> inputNames,
                final Map<String, Schema> outputSchemas,
                final IndexDefinition index,
                final List<QueryDefinition> queries,
                final Map<String, TupleTag<MElement>> outputTags,
                final TupleTag<MElement> failureTag,
                final Coder<MElement> unionCoder) {

            super(name, inputNames, outputSchemas, index, queries, outputTags, failureTag);

            this.indexBufferSpec = StateSpecs.value(ListCoder.of(unionCoder));
            this.bufferIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
        }

        @Setup
        public void setup() throws IOException {
            super.setup();
        }

        @Teardown
        public void teardown() throws IOException {
            super.teardown();
        }

        @ProcessElement
        @RequiresTimeSortedInput
        public void processElement(
                final ProcessContext c,
                final @AlwaysFetched @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<MElement>> bufferIndexBufferValueState,
                final @AlwaysFetched @StateId(STATE_ID_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

            super.processElement(c, bufferIndexBufferValueState, bufferUpdateIntervalValueState);
        }

        @OnWindowExpiration
        public void onWindowExpiration(
                @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<MElement>> bufferIndexBufferValueState) {

            LOG.info("onWindowExpiration");

            final List<MElement> buffer = Optional
                    .ofNullable(bufferIndexBufferValueState.read())
                    .orElseGet(ArrayList::new);

            index(buffer);
        }

    }

    protected static class Neo4jQueryAndIndexStreamingDoFn extends Neo4jQueryAndIndexDoFn {

        @StateId(STATE_ID_INDEX_BUFFER)
        private final StateSpec<ValueState<List<MElement>>> indexBufferSpec;
        @StateId(STATE_ID_INTERVAL_COUNTER)
        private final StateSpec<ValueState<Integer>> bufferIntervalCounterSpec;

        Neo4jQueryAndIndexStreamingDoFn(
                final String name,
                final List<String> inputNames,
                final Map<String, Schema> outputSchemas,
                final IndexDefinition index,
                final List<QueryDefinition> queries,
                final Map<String, TupleTag<MElement>> outputTags,
                final TupleTag<MElement> failureTag,
                final Coder<MElement> unionCoder) {

            super(name, inputNames, outputSchemas, index, queries, outputTags, failureTag);

            this.indexBufferSpec = StateSpecs.value(ListCoder.of(unionCoder));
            this.bufferIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
        }

        @Setup
        public void setup() throws IOException {
            super.setup();
        }

        @Teardown
        public void teardown() throws IOException {
            super.teardown();
        }

        @ProcessElement
        public void processElement(
                final ProcessContext c,
                final @AlwaysFetched @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<MElement>> bufferIndexBufferValueState,
                final @AlwaysFetched @StateId(STATE_ID_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

            super.processElement(c, bufferIndexBufferValueState, bufferUpdateIntervalValueState);

        }

    }

         */

}