package com.mercari.solution.module.transform;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import com.google.cloud.bigtable.data.v2.models.sql.Statement;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.BigtableUtil;
import com.mercari.solution.util.pipeline.Partition;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.sql.calcite.MemorySchema;
import freemarker.template.Template;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Planner;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
/*
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

 */

@Transform.Module(name="bigtable")
public class BigtableTransform extends Transform {

    private static class Parameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String tableId;

        private QueryParameter query;
        private PostProcessParameter postProcess;

        private JsonElement filter;
        private KeyRange keyRange;

        private List<BigtableSchemaUtil.ColumnFamilyProperties> columns;
        private BigtableSchemaUtil.Format format;
        private BigtableSchemaUtil.CellType cellType;

        private String appProfileId;
        private Boolean flowControl;

        private String postProcessingSql;
        private Boolean postProcessingFlatten;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(instanceId == null) {
                errorMessages.add("parameters.instanceId must not be null");
            }
            if((filter == null || filter.isJsonNull()) && keyRange == null) {
                errorMessages.add("parameters.rowFilter and keyRange must not be null");
            } else if(keyRange != null) {
                errorMessages.addAll(keyRange.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(format == null) {
                format = BigtableSchemaUtil.Format.bytes;
            }
            if(cellType == null) {
                cellType = BigtableSchemaUtil.CellType.last;
            }
            if(columns == null) {
                columns = new ArrayList<>();
            }
            for(var column : columns) {
                column.setDefaults(format, cellType);
            }
        }

    }

    private static class QueryParameter {

        private KeyRange keyRange;
        private JsonElement filter;
        private String sql;

    }

    private static class PostProcessParameter {

        private JsonArray select;
        private JsonElement filter;
        private String flattenField;

        private String sql;

        private JsonArray partitions;

    }

    private static class KeyRange implements Serializable {

        private String start;
        private String end;
        private String prefix;
        private String exact;

        private Set<String> templateArgs;

        private transient Template startTemplate;
        private transient Template endTemplate;
        private transient Template prefixTemplate;
        private transient Template exactTemplate;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public void setup() {
            if(start != null) {
                this.startTemplate = TemplateUtil.createStrictTemplate("bigtableKeyRangeStartTemplate", start);
            }
            if(end != null) {
                this.endTemplate = TemplateUtil.createStrictTemplate("bigtableKeyRangeEndTemplate", end);
            }
            if(prefix != null) {
                this.prefixTemplate = TemplateUtil.createStrictTemplate("bigtableKeyRangePrefixTemplate", prefix);
            }
            if(exact != null) {
                this.exactTemplate = TemplateUtil.createStrictTemplate("bigtableKeyRangeExactTemplate", exact);
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema queryResultSchema = createQueryResultSchema(parameters);

        final List<Partition> partitions = createPostProcess(parameters, queryResultSchema);

        final TupleTag<MElement> defaultTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};
        final List<TupleTag<?>> outputTags = new ArrayList<>();

        outputTags.add(failuresTag);
        for(final Partition partition : partitions) {
            outputTags.add(partition.getOutputTag());
        }

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.AVRO);

        final PCollectionTuple outputs = input
                .apply("Process", ParDo
                        .of(new ProcessDoFn(parameters, partitions,
                                inputSchema, queryResultSchema, outputType,
                                defaultTag, failuresTag,
                                getLoggings(), getFailFast()))
                        .withOutputTags(defaultTag, TupleTagList.of(outputTags)));

        errorHandler.addError(outputs.get(failuresTag));

        MCollectionTuple outputTuple;
        if(partitions.isEmpty()) {
            final Schema outputSchema = createDefaultSchema(inputSchema, queryResultSchema)
                    .setup(outputType);
            final PCollection<MElement> output = outputs.get(defaultTag);
            outputTuple = MCollectionTuple.of(output, outputSchema.withType(outputType));
        } else {
            outputTuple = MCollectionTuple.empty(inputs.getPipeline());
            for(final Partition partition : partitions) {
                final Schema outputSchema = partition
                        .getOutputSchema()
                        .copy()
                        .setup(outputType);
                final PCollection<MElement> output = outputs.get(partition.getOutputTag());
                outputTuple = outputTuple.and(partition.getName(), output, outputSchema.withType(outputType));
            }
        }

        return outputTuple;
    }

    private static Schema createQueryResultSchema(final Parameters parameters) {
        if(parameters.query.sql != null) {
            return BigtableSchemaUtil.convertSchema(parameters.projectId, parameters.instanceId, parameters.query.sql);
        } else if(parameters.query.keyRange != null || !parameters.query.filter.isJsonNull()) {
            return BigtableSchemaUtil.createSchema(parameters.columns);
        } else {
            throw new IllegalModuleException("");
        }
    }

    private static Schema createDefaultSchema(final Schema inputSchema, final Schema queryResultSchema) {
        return Schema.builder()
                .withField("input", Schema.FieldType.element(inputSchema))
                .withField("results", Schema.FieldType.array(Schema.FieldType.element(queryResultSchema)))
                .build();
    }

    private static List<Partition> createPostProcess(
            final Parameters parameters,
            final Schema inputSchema) {

        if(parameters.postProcess.partitions != null && parameters.postProcess.partitions.isJsonArray()) {
            return Partition.of(parameters.postProcess.partitions, inputSchema);
        } else if(parameters.postProcess.select != null && parameters.postProcess.select.isJsonArray()) {
            final List<Partition> partitions = new ArrayList<>();
            final Partition partition = Partition.of("", parameters.postProcess.filter, parameters.postProcess.select, parameters.postProcess.flattenField, inputSchema);
            partitions.add(partition);
            return partitions;
        } else {
            return new ArrayList<>();
        }
    }

    private static class ProcessDoFn extends DoFn<MElement, MElement> {

        private final String projectId;
        private final String instanceId;
        private final String tableId;

        // query by filter
        private final KeyRange keyRange;
        private final String rowFilterJson;
        // query by sql
        private final String sql;

        private final Schema inputSchema;
        private final Schema queryResultSchema;
        private final Schema defaultOutputSchema;
        private final DataType outputType;
        private final Map<String, BigtableSchemaUtil.ColumnFamilyProperties> families;

        // postProcessing
        private List<Partition> partitions;

        private final String appProfileId;
        private final Boolean flowControl;


        private final Boolean failFast;
        private final TupleTag<BadRecord> failuresTag;
        private final TupleTag<MElement> defaultTag;

        private final Map<String, Logging> loggings;

        private transient BigtableDataClient client;
        private transient Template filterTemplate;

        ProcessDoFn(
                final Parameters parameters,
                final List<Partition> partitions,
                final Schema inputSchema,
                final Schema queryResultSchema,
                final DataType outputType,
                final TupleTag<MElement> defaultTag,
                final TupleTag<BadRecord> failuresTag,
                final List<Logging> loggings,
                final boolean failFast) {

            this.projectId = parameters.projectId;
            this.instanceId = parameters.instanceId;
            this.tableId = parameters.tableId;
            this.keyRange = parameters.query.keyRange;
            this.rowFilterJson = Optional.ofNullable(parameters.query.filter).map(JsonElement::toString).orElse(null);
            this.sql = parameters.query.sql;
            this.families = BigtableSchemaUtil.toMap(parameters.columns);
            this.partitions = partitions;
            this.inputSchema = inputSchema;
            this.queryResultSchema = queryResultSchema;
            this.defaultOutputSchema = createDefaultSchema(inputSchema, queryResultSchema);
            this.outputType = outputType;
            this.appProfileId = parameters.appProfileId;
            this.flowControl = parameters.flowControl;

            this.failFast = failFast;
            this.failuresTag = failuresTag;
            this.defaultTag = defaultTag;

            this.loggings = Logging.map(loggings);
        }

        @Setup
        public void setup() throws Exception {
            final BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId);

            if(appProfileId != null) {
                builder.setAppProfileId(appProfileId);
            }
            if(flowControl != null) {
                builder.setBulkMutationFlowControl(flowControl);
            }

            if(keyRange != null) {
                keyRange.setup();
            }

            if(rowFilterJson != null) {
                this.filterTemplate = TemplateUtil.createStrictTemplate("bigtableFilterTemplate", rowFilterJson);
            }

            for(final Partition partition : partitions) {
                partition.setup();
            }

            this.client = BigtableDataClient.create(builder.build());
        }

        @Teardown
        public void teardown() {
            if(client != null) {
                client.close();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                if(partitions.isEmpty()) {
                    final List<MElement> results = read(input);
                    final Map<String, Object> values = new HashMap<>();
                    values.put("input", input.asPrimitiveMap());
                    values.put("results", results);
                    final MElement output = MElement.of(values, c.timestamp());
                    c.output(output.convert(defaultOutputSchema, outputType));
                } else {
                    for(final Partition partition : partitions) {
                        if(!partition.match(input)) {
                            continue;
                        }
                        final List<MElement> results = read(input);
                        final List<MElement> outputs = partition.executeStateful(results, c.timestamp());
                        for(final MElement output : outputs) {
                            c.output(partition.getOutputTag(), output.convert(partition.getOutputSchema(), outputType));
                        }
                    }
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("bigtable transform error: " + e.getMessage(), input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private List<MElement> read(final MElement input) {
            if(sql != null) {
                return readBySql(input);
            } else {
                return readByFilter(input);
            }
        }

        private List<MElement> readBySql(final MElement input) {
            try(final ResultSet resultSet = client.executeQuery(Statement.newBuilder(sql).build())) {
                final List<MElement> results = new ArrayList<>();
                while(resultSet.next()) {
                    final MElement result = BigtableSchemaUtil.convert(resultSet, input.getTimestamp());
                    results.add(result);
                }
                return results;
            }
        }

        private List<MElement> readByFilter(final MElement input) {
            final Map<String, Object> inputValues = input.asStandardMap(inputSchema, null);

            final Query query = Query.create(TableId.of(tableId));
            if(keyRange != null) {
                if(keyRange.exact != null) {
                    final String exact = TemplateUtil.executeStrictTemplate(keyRange.exactTemplate, inputValues);
                    query.rowKey(exact);
                } else if(keyRange.prefix != null) {
                    final String prefix = TemplateUtil.executeStrictTemplate(keyRange.prefixTemplate, inputValues);
                    query.prefix(prefix);
                } else if(keyRange.start != null || keyRange.end != null) {
                    final String start = keyRange.start != null ? TemplateUtil.executeStrictTemplate(keyRange.startTemplate, inputValues) : null;
                    final String end   = keyRange.end   != null ? TemplateUtil.executeStrictTemplate(keyRange.endTemplate, inputValues) : null;
                    query.range(start, end);
                }
            }

            if(rowFilterJson != null) {
                final String filterText = TemplateUtil.executeStrictTemplate(filterTemplate, inputValues);
                final JsonElement filterElement = new Gson().fromJson(filterText, JsonElement.class);
                final Filters.Filter filter = BigtableUtil.createFilter(filterElement);
                query.filter(filter);
            }

            final List<MElement> results = new ArrayList<>();
            for(final Row row : client.readRows(query)) {
                final MElement result = BigtableSchemaUtil.convert(row, families, input.getTimestamp());
                results.add(result);
            }
            return results;
        }

    }

    /*
    private static class QueryDoFn extends DoFn<MElement, MElement> {

        private final String projectId;
        private final String instanceId;
        private final String tableId;

        private final KeyRange keyRange;
        private final String rowFilterJson;

        private final Schema inputSchema;
        private final Schema filterResultSchema;
        private final Map<String, BigtableSchemaUtil.ColumnFamilyProperties> families;

        private final String appProfileId;
        private final Boolean flowControl;

        private final String postProcessingSql;

        private final Boolean failFast;
        private final TupleTag<MElement> failuresTag;

        private transient BigtableDataClient client;
        private transient Template filterTemplate;


        private transient List<MElement> elements;
        private transient Planner planner;
        private transient PreparedStatement statement;


        QueryDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final Schema filterResultSchema,
                final Boolean failFast,
                final TupleTag<MElement> failuresTag) {

            this.projectId = parameters.projectId;
            this.instanceId = parameters.instanceId;
            this.tableId = parameters.tableId;
            this.keyRange = parameters.keyRange;
            this.rowFilterJson = Optional.ofNullable(parameters.filter).map(JsonElement::toString).orElse(null);
            this.families = BigtableSchemaUtil.toMap(parameters.columns);
            this.inputSchema = inputSchema;
            this.filterResultSchema = filterResultSchema;
            this.appProfileId = parameters.appProfileId;
            this.flowControl = parameters.flowControl;
            this.postProcessingSql = parameters.postProcessingSql;

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() throws Exception {
            final BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId);

            if(appProfileId != null) {
                builder.setAppProfileId(appProfileId);
            }
            if(flowControl != null) {
                builder.setBulkMutationFlowControl(flowControl);
            }

            if(keyRange != null) {
                keyRange.setup();
            }

            if(rowFilterJson != null) {
                this.filterTemplate = TemplateUtil.createStrictTemplate("bigtableFilterTemplate", rowFilterJson);
            }

            this.client = BigtableDataClient.create(builder.build());
            this.elements = new ArrayList<>();

            if(postProcessingSql != null) {
                final MemorySchema memorySchema = MemorySchema.create("schema", List.of(
                        MemorySchema.createTable("INPUT", filterResultSchema, elements)
                ));
                this.planner = com.mercari.solution.util.pipeline.Query.createPlanner(memorySchema);
                this.statement = com.mercari.solution.util.pipeline.Query.createStatement(planner, postProcessingSql);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                final Iterable<Row> stream = read(input);
                final MElement output;
                if(postProcessingSql != null) {
                    output = process(stream, c.timestamp());
                } else {
                    output = filter(stream, c.timestamp());
                }
                c.output(output);
            } catch (final Throwable e) {
                String errorMessage = MFailure.convertThrowableMessage(e);
                LOG.error("SQL query error: {}, {} for input: {}", e, errorMessage, input);
                if(failFast) {
                    throw new IllegalStateException(errorMessage, e);
                }
                //final MFailure failure = MFailure.of(jobName, moduleName, element.toString(), errorMessage, c.timestamp());
                //if(outputFailure) {
                //    c.output(failuresTag, failureElement.toElement(c.timestamp()));
                //}
                //c.output(failuresTag, failure.toElement(c.timestamp()));
            }
        }

        private Iterable<Row> read(final MElement input) {
            final Map<String, Object> inputValues = input.asStandardMap(inputSchema, null);

            final Query query = Query.create(TableId.of(tableId));
            if(keyRange != null) {
                if(keyRange.exact != null) {
                    final String exact = TemplateUtil.executeStrictTemplate(keyRange.exactTemplate, inputValues);
                    query.rowKey(exact);
                } else if(keyRange.prefix != null) {
                    final String prefix = TemplateUtil.executeStrictTemplate(keyRange.prefixTemplate, inputValues);
                    query.prefix(prefix);
                } else if(keyRange.start != null || keyRange.end != null) {
                    final String start = keyRange.start != null ? TemplateUtil.executeStrictTemplate(keyRange.startTemplate, inputValues) : null;
                    final String end   = keyRange.end   != null ? TemplateUtil.executeStrictTemplate(keyRange.endTemplate, inputValues) : null;
                    query.range(start, end);
                }
            }

            if(rowFilterJson != null) {
                final String filterText = TemplateUtil.executeStrictTemplate(filterTemplate, inputValues);
                final JsonElement filterElement = new Gson().fromJson(filterText, JsonElement.class);
                final Filters.Filter filter = BigtableUtil.createFilter(filterElement);
                query.filter(filter);
            }

            return client.readRows(query);
        }

        private MElement filter(final Iterable<Row> stream, final Instant timestamp) {
            final List<Map<String, Object>> results = new ArrayList<>();
            for (final Row row : stream) {
                final Map<String, Object> primitiveValues = BigtableSchemaUtil.toPrimitiveValues(row, families);
                results.add(primitiveValues);
            }
            final Map<String, Object> output = new HashMap<>();
            output.put("results", results);
            return MElement.of(output, timestamp);
        }

        private MElement process(final Iterable<Row> stream, final Instant timestamp) {
            this.elements.clear();
            for (final Row row : stream) {
                final Map<String, Object> primitiveValues = BigtableSchemaUtil.toPrimitiveValues(row, families);
                final MElement element = MElement.of(primitiveValues, timestamp);
                elements.add(element);
            }
            try(final ResultSet resultSet = statement.executeQuery()) {
                final List<Map<String, Object>> results = CalciteSchemaUtil.convert(resultSet);
                final Map<String, Object> values = new HashMap<>();
                values.put("results", results);
                return MElement.of(values, timestamp);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Teardown
        public void teardown() {
            if(client != null) {
                client.close();
            }
        }

    }
     */

}
