package com.mercari.solution.module.transform;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.BigtableUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.sql.calcite.MemorySchema;
import freemarker.template.Template;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Planner;
import org.joda.time.Instant;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Transform.Module(name="bigtable")
public class BigtableTransform extends Transform {

    private static class Parameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String tableId;

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

    private static class PostProcess implements Serializable {

        private String start;
        private String end;
        private String prefix;

        private Set<String> templateArgs;

        private transient Template startTemplate;
        private transient Template endTemplate;
        private transient Template prefixTemplate;

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

        }

    }

    @Override
    public MCollectionTuple expand(
            MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema filterResultSchema = BigtableSchemaUtil.createSchema(parameters.columns);
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema;
        if(parameters.postProcessingSql != null) {
            final Schema queryResultSchema = com.mercari.solution.util.pipeline.Query.createQueryResultSchema(
                    List.of("INPUT"), List.of(filterResultSchema), parameters.postProcessingSql);
            final Schema.FieldType queryResultType = Schema.FieldType.element(queryResultSchema);

            if(parameters.postProcessingFlatten) {
                outputSchema = Schema.builder(queryResultSchema)
                        .build();
            } else {
                outputSchema = Schema.builder()
                        .withField("results", Schema.FieldType.array(queryResultType))
                        .build();
            }
        } else {
            outputSchema = Schema.builder()
                    .withField("results", Schema.FieldType.array(Schema.FieldType.element(filterResultSchema)))
                    .build();
        }

        final TupleTag<MElement> failuresTag = new TupleTag<>(){};

        final PCollection<MElement> output = input
                .apply("Query", ParDo.of(new QueryDoFn(
                        parameters, inputSchema, filterResultSchema, getFailFast(), failuresTag)));
        return MCollectionTuple
                .of(output, outputSchema);
    }

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

}
