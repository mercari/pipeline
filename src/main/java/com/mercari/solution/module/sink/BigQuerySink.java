package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.model.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.gcp.BigQueryUtil;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


@Sink.Module(name="bigquery")
public class BigQuerySink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private static class Parameters implements Serializable {

        private String projectId;
        private String datasetId;
        private String tableId;
        private String table;

        private BigQueryIO.Write.WriteDisposition writeDisposition;
        private BigQueryIO.Write.CreateDisposition createDisposition;
        private BigQueryIO.Write.Method method;
        private RowMutationInformation.MutationType mutationType;
        private Boolean outputResult;

        // for table creation
        private String partitioning;
        private String partitioningField;
        private List<String> clusteringFields;
        private List<String> primaryKeyFields;

        private Boolean skipInvalidRows;
        private Boolean ignoreUnknownValues;
        private Boolean ignoreInsertIds;
        private Boolean withExtendedErrorInfo;
        private FailedInsertRetryPolicy failedInsertRetryPolicy;

        private List<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions;
        private Boolean autoSchemaUpdate;

        private String kmsKey;

        private Boolean optimizedWrites;
        private Boolean autoSharding;
        private Long triggeringFrequencySecond;
        private Integer numStorageWriteApiStreams;
        private Boolean withoutValidation;
        private BigQueryUtil.WriteFormat writeFormat;

        private String customGcsTempLocation;

        @Deprecated
        private String clustering;

        public TableReference getTableReference() {
            return new TableReference()
                    .setProjectId(projectId)
                    .setDatasetId(datasetId)
                    .setTableId(tableId);
        }

        private void validate(MCollectionTuple inputs) {
            if(this.table == null && this.datasetId == null) {
                throw new IllegalModuleException("parameters.datasetId, tableId is missing");
            }
        }

        private void setDefaults(final PInput input, final MPipeline.Runner runner) {
            if(this.datasetId == null) {
                final String str;
                if(this.table.contains(":")) {
                    str = this.table.replaceFirst(":", ".");
                } else {
                    str = this.table;
                }
                final String[] strs = str.split("\\.");
                if(strs.length == 3) {
                    this.projectId = strs[0];
                    this.datasetId = strs[1];
                    this.tableId = strs[2];
                } else if(strs.length == 2) {
                    this.projectId = strs[0];
                    this.datasetId = strs[1];
                }
            }
            if(projectId == null) {
                this.projectId = OptionUtil.getDefaultProject();
            }
            if(this.writeDisposition == null) {
                this.writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_EMPTY;
            }
            if(this.createDisposition == null) {
                this.createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
            }
            if(this.method == null) {
                this.method = BigQueryIO.Write.Method.DEFAULT;
            }
            if(this.clusteringFields == null) {
                this.clusteringFields = new ArrayList<>();
                if(this.clustering != null) {
                    Collections.addAll(this.clusteringFields, this.clustering.split(","));
                }
            }
            if(this.primaryKeyFields == null) {
                this.primaryKeyFields = new ArrayList<>();
            }

            if(this.skipInvalidRows == null) {
                this.skipInvalidRows = false;
            }
            if(this.ignoreUnknownValues == null) {
                this.ignoreUnknownValues = false;
            }
            if(this.ignoreInsertIds == null) {
                this.ignoreInsertIds = false;
            }
            if(this.withExtendedErrorInfo == null) {
                this.withExtendedErrorInfo = false;
            }

            if(this.optimizedWrites == null) {
                this.optimizedWrites = false;
            }
            if(BigQueryIO.Write.Method.FILE_LOADS.equals(this.method)
                    || BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(this.method)
                    || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(this.method)) {
                if(this.triggeringFrequencySecond == null) {
                    if(OptionUtil.isStreaming(input)) {
                        this.triggeringFrequencySecond = 10L;
                    }
                } else {
                    if(!OptionUtil.isStreaming(input)) {
                        LOG.warn("parameters.triggeringFrequencySecond must not be set in batch mode");
                        this.triggeringFrequencySecond = null;
                    }
                }
                if(this.numStorageWriteApiStreams == null) {
                    if(!BigQueryIO.Write.Method.FILE_LOADS.equals(this.method)
                            && OptionUtil.isStreaming(input)) {
                        LOG.warn("parameters.numStorageWriteApiStreams must be set when using storage write api");
                        this.autoSharding = true;
                    }
                }
            }
            if(this.autoSharding == null) {
                this.autoSharding = false;
            }
            if(customGcsTempLocation == null) {
                if(MPipeline.Runner.direct.equals(runner)) {
                    customGcsTempLocation = input.getPipeline().getOptions().getTempLocation();
                }
            }
            if(this.withoutValidation == null) {
                this.withoutValidation = false;
            }

            if(outputResult == null) {
                outputResult = !OptionUtil.isStreaming(input);
            }

        }
    }

    private enum FailedInsertRetryPolicy {
        always,
        never,
        retryTransientErrors
    }


    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(inputs);
        parameters.setDefaults(inputs, getRunner());

        final Schema inputSchema = Union.createUnionSchema(inputs, getUnion());

        final WriteResult writeResult = write(inputs, parameters, errorHandler);

        return createOutputs(inputs, writeResult, parameters, inputSchema);
    }

    private WriteResult write(
            final MCollectionTuple inputs,
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        final PCollection<MElement> elements = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final boolean isStreaming = OptionUtil.isStreaming(inputs);

        final Schema inputSchema = Union.createUnionSchema(inputs, getUnion());

        //final TableSchema destinationTableSchema = TemplateUtil.isTemplateText(parameters.table) ? null : BigQueryUtil.getTableSchemaFromTable(parameters.getTableReference());
        /*
        final TableSchema insertTableSchema = Optional
                .ofNullable(destinationTableSchema)
                .orElseGet(() -> ElementToTableRowConverter.convertSchema(inputSchema));

         */
        final Schema insertTableSchema = inputSchema;

        final List<String> templateVariables = TemplateUtil.extractTemplateArgs(parameters.table, inputSchema);

        final BigQueryUtil.WriteFormat writeDataType = Optional
                .ofNullable(parameters.writeFormat)
                .orElseGet(() -> BigQueryUtil.getPreferWriteFormat(parameters.method, isStreaming));
        return switch (writeDataType) {
            case row -> {
                BigQueryIO.Write<Row> write = BigQueryIO
                        .<Row>write()
                        .useBeamSchema();
                final SerializableFunction<Row, String> destinationFunction = createDestinationFunction(inputSchema, parameters.table, templateVariables);
                write = applyParameters(write, parameters, insertTableSchema, isStreaming, destinationFunction, errorHandler);
                yield elements
                        .apply("ConvertToRow", ParDo.of(new ToRowDoFn(inputSchema)))
                        .setCoder(RowCoder.of(inputSchema.getRow().getSchema()))
                        .apply("WriteRow", write);
            }
            case avro -> {
                BigQueryIO.Write<GenericRecord> write = BigQueryIO
                        .writeGenericRecords()
                        .withAvroSchemaFactory(TableRowToAvroConverter::convertSchema)
                        .useAvroLogicalTypes();
                final SerializableFunction<GenericRecord, String> destinationFunction = createDestinationFunction(inputSchema, parameters.table, templateVariables);
                write = applyParameters(write, parameters, insertTableSchema, isStreaming, destinationFunction, errorHandler);
                yield elements
                        .apply("ConvertToAvro", ParDo.of(new ToAvroDoFn(inputSchema)))
                        .setCoder(AvroCoder.of(inputSchema.getAvro().getSchema()))
                        .apply("WriteAvro", write);
            }
            case avrofile -> {
                BigQueryIO.Write<MElement> write = BigQueryIO
                        .<MElement>write()
                        .withAvroFormatFunction(ElementToAvroConverter::convert)
                        .withAvroSchemaFactory(TableRowToAvroConverter::convertSchema)
                        .useAvroLogicalTypes();
                final SerializableFunction<MElement, String> destinationFunction = createDestinationFunction(inputSchema, parameters.table, templateVariables);
                write = applyParameters(write, parameters, insertTableSchema, isStreaming, destinationFunction, errorHandler);
                yield elements
                        .apply("WriteAvroFile", write);
            }
            case json -> {
                BigQueryIO.Write<TableRow> write = BigQueryIO
                        .writeTableRows();
                final SerializableFunction<TableRow, String> destinationFunction = createDestinationFunction(inputSchema, parameters.table, templateVariables);
                write = applyParameters(write, parameters, insertTableSchema, isStreaming, destinationFunction, errorHandler);
                yield elements
                        .apply("ConvertToTableRow", ParDo.of(new ToTableRowDoFn(inputSchema)))
                        .apply("WriteTableRow", write);
            }
        };
    }

    private MCollectionTuple createOutputs(
            final MCollectionTuple inputs,
            final WriteResult writeResult,
            final Parameters parameters,
            final Schema inputSchema) {

        final boolean isStreamingInsert =
                BigQueryIO.Write.Method.STREAMING_INSERTS.equals(parameters.method)
                        || (BigQueryIO.Write.Method.DEFAULT.equals(parameters.method) && OptionUtil.isStreaming(inputs));
        final boolean isStorageApiInsert =
                BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.method)
                        || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(parameters.method);

        final PCollection<MElement> result;
        final PCollection<MElement> failure;
        final Schema resultSchema;
        if(isStreamingInsert) {
            if(parameters.outputResult) {
                result = writeResult.getSuccessfulInserts()
                        .apply("ConvertSuccessfulInserts", ParDo.of(new SuccessfullInsertDoFn()));
                resultSchema = inputSchema;
            } else {
                result = null;
                resultSchema = null;
            }
            if(parameters.withExtendedErrorInfo) {
                failure = writeResult.getFailedInsertsWithErr()
                        .apply("ConvertFailureRecordWithError", ParDo.of(new FailedRecordWithErrorDoFn(getJobName(), getName())))
                        .setCoder(ElementCoder.of(MFailure.schema()));
            } else {
                failure = writeResult.getFailedInserts()
                        .apply("ConvertFailureRecordInsert", ParDo.of(new FailedRecordDoFn(getJobName(), getName())))
                        .setCoder(ElementCoder.of(MFailure.schema()));
            }
        } else if(isStorageApiInsert) {
            if(parameters.outputResult) {
                result = writeResult.getSuccessfulStorageApiInserts()
                        .apply("ConvertSuccessfulApiInsert", ParDo.of(new SuccessfullInsertDoFn()));
                resultSchema = inputSchema;
            } else {
                result = null;
                resultSchema = null;
            }
            failure = writeResult.getFailedStorageApiInserts()
                    .apply("ConvertFailureRecordStorage", ParDo.of(new FailedStorageApiRecordDoFn(getJobName(), getName())))
                    .setCoder(ElementCoder.of(MFailure.schema()));
        } else {
            if(parameters.outputResult) {
                result = writeResult.getSuccessfulTableLoads()
                        .apply("ConvertSuccessfulTableLoads", ParDo.of(new SuccessfulTableLoadsDoFn()));
                resultSchema = BigQueryUtil.getTableDefinitionSchema();
            } else {
                result = null;
                resultSchema = null;
            }
            failure = writeResult.getFailedInserts()
                    .apply("ConvertFailureRecordInsert", ParDo.of(new FailedRecordDoFn(getJobName(), getName())))
                    .setCoder(ElementCoder.of(MFailure.schema()));
        }

        if(result == null) {
            return MCollectionTuple
                    .done(PDone.in(inputs.getPipeline()));
        } else {
            return MCollectionTuple
                    .of(result, resultSchema);
        }
    }

    private static class ToRowDoFn extends DoFn<MElement, Row> {

        private final Schema schema;

        ToRowDoFn(final Schema schema) {
            this.schema = schema;
        }

        @Setup
        public void setup() {
            this.schema.setup(DataType.ROW);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final Row row = ElementToRowConverter.convert(schema, element);
            c.output(row);
        }
    }

    private static class ToAvroDoFn extends DoFn<MElement, GenericRecord> {

        private final Schema schema;

        ToAvroDoFn(final Schema schema) {
            this.schema = schema;
        }

        @Setup
        public void setup() {
            this.schema.setup(DataType.AVRO);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final GenericRecord record = ElementToAvroConverter.convert(schema, element);
            c.output(record);
        }

    }

    private static class ToTableRowDoFn extends DoFn<MElement, TableRow> {

        private final Schema schema;

        ToTableRowDoFn(final Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final TableRow tableRow = ElementToTableRowConverter.convert(schema, element);
            c.output(tableRow);
        }
    }

    private static <InputT> SerializableFunction<InputT, String>  createDestinationFunction(
            final Schema schema,
            final String destination,
            final Collection<String> variables) {

        return (InputT input) -> {
            if(variables == null || variables.isEmpty()) {
                return destination;
            }
            final Map<String, Object> values = switch (input) {
                case MElement element -> element.asStandardMap(schema, variables);
                case GenericRecord record -> AvroSchemaUtil.asStandardMap(record, variables);
                case Row row -> RowSchemaUtil.asStandardMap(row, variables);
                case TableRow tableRow -> tableRow;
                default -> throw new IllegalArgumentException();
            };
            TemplateUtil.setFunctions(values);
            return TemplateUtil.executeStrictTemplate(destination, values);
        };
    }

        /*
    private static final SerializableFunction<MElement, TableRow> convertTableRowFunction = (MElement element) -> switch (element.getType()) {
        case ROW -> RowToTableRowConverter.convert((Row) element.getValue());
        case AVRO -> RecordToTableRowConverter.convert((GenericRecord) element.getValue());
        case STRUCT -> StructToTableRowConverter.convert((Struct) element.getValue());
        case DOCUMENT -> DocumentToTableRowConverter.convert((Document) element.getValue());
        case ENTITY -> EntityToTableRowConverter.convertWithoutKey((Entity) element.getValue());
        default -> throw new IllegalArgumentException();
    };
     */

    /*
    public static class BigQueryMutationWrite extends PTransform<PCollection<UnifiedMutation>, PCollection<GenericRecord>> {

        private final String name;
        private final BigQuerySinkParameters parameters;
        private final Map<String, String> tableSchemas;
        private final List<FCollection<?>> waitCollections;

        private FCollection<?> collection;

        private BigQueryMutationWrite(
                final String name,
                final FCollection<?> collection,
                final BigQuerySinkParameters parameters,
                final Map<String, TableSchema> tableSchemas,
                final List<FCollection<?>> waitCollections) {

            this.name = name;
            this.collection = collection;
            this.parameters = parameters;
            this.tableSchemas = tableSchemas.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> TableRowToRecordConverter.convertSchema(e.getValue()).toString()));
            this.waitCollections = waitCollections;

            LOG.info("init: " + this.tableSchemas);
        }

        public PCollection<GenericRecord> expand(final PCollection<UnifiedMutation> input) {
            this.parameters.validate();
            this.parameters.setDefaults(input);

            final PCollection<UnifiedMutation> waited;
            if(waitCollections == null) {
                waited = input;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = input
                        .apply("Wait", Wait.on(waits))
                        .setCoder(input.getCoder());
            }

            final String projectId = parameters.getTable().split("\\.")[0];
            final Map<String,List<String>> primaryKeyFields = BigQueryUtil
                    .getPrimaryKeyFieldsFromDataset(parameters.getTable(), projectId);

            final BigQueryIO.Write<UnifiedMutation> write = BigQueryIO
                    .<UnifiedMutation>write()
                    .to(new MutationDynamicDestinationFunc(parameters.getTable(), tableSchemas))
                    .withFormatFunction((UnifiedMutation mutation) -> {
                        return mutation.toTableRow(primaryKeyFields.get(mutation.getTable()));
                    })
                    .withRowMutationInformationFn(UnifiedMutation::toRowMutationInformation)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                    .withExtendedErrorInfo();

            final WriteResult writeResult = waited.apply("WriteTableRow", write);

            return writeResult.getFailedStorageApiInserts()
                    .apply("ConvertFailureSARecord", ParDo.of(new FailedStorageApiRecordDoFn(name, collection.getAvroSchema().toString())))
                    .setCoder(AvroCoder.of(collection.getAvroSchema()));
        }

    }

     */

    private static <InputT> BigQueryIO.Write<InputT> applyParameters(
            final BigQueryIO.Write<InputT> base,
            final Parameters parameters,
            final Schema tableSchema,
            final boolean isStreaming,
            final SerializableFunction<InputT, String> destinationFunction,
            final MErrorHandler errorHandler) {

        final String table = parameters.table;

        BigQueryIO.Write<InputT> write = base
                .withTableDescription("Auto Generated at " + Instant.now())
                .withWriteDisposition(parameters.writeDisposition)
                .withCreateDisposition(parameters.createDisposition)
                .withMethod(parameters.method);

        if(TemplateUtil.isTemplateText(table)) {
            write = write.to(new DynamicDestinationFunc<>(tableSchema, destinationFunction, parameters));
        } else {
            write = write.to(table);
            if(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED.equals(parameters.createDisposition)) {
                write = write.withSchema(ElementToTableRowConverter.convertSchema(tableSchema));
            }
            if(parameters.partitioning != null) {
                final TimePartitioning timePartitioning = new TimePartitioning()
                        .setType(parameters.partitioning.trim());
                if(parameters.partitioningField != null) {
                    timePartitioning.setField(parameters.partitioningField);
                }
                write = write.withTimePartitioning(timePartitioning);
            }
            if(!parameters.clusteringFields.isEmpty()) {
                final Clustering clustering = new Clustering().setFields(parameters.clusteringFields);
                write = write.withClustering(clustering);
            }
        }

        if(!parameters.primaryKeyFields.isEmpty()) {
            write = write.withPrimaryKey(parameters.primaryKeyFields);
        }

        if(parameters.kmsKey != null) {
            write = write.withKmsKey(parameters.kmsKey.trim());
        }

        if(parameters.optimizedWrites) {
            write = write.optimizedWrites();
        }

        if(parameters.schemaUpdateOptions != null && !parameters.schemaUpdateOptions.isEmpty()) {
            write = write.withSchemaUpdateOptions(new HashSet<>(parameters.schemaUpdateOptions));
        }
        if(parameters.autoSchemaUpdate != null) {
            if(BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.method)) {
                write = write.withAutoSchemaUpdate(parameters.autoSchemaUpdate);
            } else {
                LOG.warn("BigQuery sink autoSchemaUpdate parameter is applicable for only STORAGE_WRITE_API method");
            }
        }

        if(parameters.customGcsTempLocation != null) {
            write = write.withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(parameters.customGcsTempLocation));
        }

        if(parameters.withoutValidation != null && parameters.withoutValidation) {
            write = write.withoutValidation();
        }

        if(isStreaming) {
            // For streaming mode options
            if(parameters.skipInvalidRows) {
                write = write.skipInvalidRows();
            }
            if(parameters.ignoreUnknownValues) {
                write = write.ignoreUnknownValues();
            }
            if(parameters.ignoreInsertIds) {
                write = write.ignoreInsertIds();
            }
            if(parameters.withExtendedErrorInfo) {
                write = write.withExtendedErrorInfo();
            }
            if(!BigQueryIO.Write.Method.FILE_LOADS.equals(parameters.method)) {
                if(parameters.failedInsertRetryPolicy != null) {
                    write = switch (parameters.failedInsertRetryPolicy) {
                        case always -> write.withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry());
                        case never -> write.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry());
                        case retryTransientErrors -> write.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());
                    };
                }
            }

            switch (parameters.method) {
                case FILE_LOADS, STORAGE_WRITE_API -> {
                    write = write
                            .withTriggeringFrequency(Duration.standardSeconds(parameters.triggeringFrequencySecond));
                    if (parameters.autoSharding) {
                        write = write.withAutoSharding();
                    } else if (parameters.numStorageWriteApiStreams != null) {
                        write = write.withNumStorageWriteApiStreams(parameters.numStorageWriteApiStreams);
                    }
                }
                case DEFAULT, STREAMING_INSERTS -> {
                    if (parameters.autoSharding) {
                        write = write.withAutoSharding();
                    }
                }
            }

        } else {
            // For batch mode options
            switch (parameters.method) {
                case STORAGE_WRITE_API, STORAGE_API_AT_LEAST_ONCE -> {
                    if (parameters.numStorageWriteApiStreams != null) {
                        write = write.withNumStorageWriteApiStreams(parameters.numStorageWriteApiStreams);
                    }
                }
            }
        }

        /*
        if(errorHandler != null) {
            write = write.withErrorHandler(errorHandler);
        }
         */
        errorHandler.apply(write);

        return write;
    }

    private static class SuccessfulTableLoadsDoFn extends DoFn<TableDestination, MElement> {

        SuccessfulTableLoadsDoFn() {

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final TableDestination tableDestination = c.element();
            if(tableDestination == null) {
                return;
            }
            final MElement output = BigQueryUtil.convertToElement(tableDestination);
            c.output(output);
        }

    }

    private static class SuccessfullInsertDoFn extends DoFn<TableRow, MElement> {

        SuccessfullInsertDoFn() {

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final TableRow tableRow = c.element();
            // TODO
            LOG.info("tableRow: " + tableRow);
        }

    }

    private static class FailedRecordDoFn extends DoFn<TableRow, MElement> {

        private final String jobName;
        private final String moduleName;
        private final Counter errorCounter;

        FailedRecordDoFn(final String jobName, final String moduleName) {
            this.jobName = jobName;
            this.moduleName = moduleName;
            this.errorCounter = Metrics.counter(moduleName, "bigquery_sink_insert_error");
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final TableRow tableRow = c.element();
            final MFailure failureElement = MFailure
                    .of(jobName, moduleName, tableRow.toString(), "", c.timestamp());
            c.output(failureElement.toElement(c.timestamp()));

            LOG.error("Failed to insert row: {}", tableRow);
        }

    }

    private static class FailedRecordWithErrorDoFn extends DoFn<BigQueryInsertError, MElement> {

        private final String job;
        private final String name;
        private final Counter errorCounter;

        FailedRecordWithErrorDoFn(final String job, final String name) {
            this.job = job;
            this.name = name;
            this.errorCounter = Metrics.counter(name, "elements_insert_error");
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final TableRow tableRow = c.element().getRow();

            final JsonArray jsonArray = new JsonArray();
            final TableDataInsertAllResponse.InsertErrors errors = c.element().getError();
            for(final ErrorProto error : c.element().getError().getErrors()) {
                final JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("message", error.getMessage());
                jsonObject.addProperty("reason", error.getReason());
                jsonObject.addProperty("location", error.getLocation());
                jsonObject.addProperty("debugInfo", error.getDebugInfo());
                jsonArray.add(jsonObject);
            }

            final JsonObject failureObject = new JsonObject();
            failureObject.add("errors", jsonArray);
            failureObject.addProperty("index", errors.getIndex());

            final MFailure failureElement = MFailure.of(
                    job, name, tableRow.toString(), failureObject.toString(), c.timestamp());

            c.output(failureElement.toElement(c.timestamp()));

            LOG.error("Failed to insert row: " + tableRow + " with error message: " + failureObject);
        }

    }

    private static class FailedStorageApiRecordDoFn extends DoFn<BigQueryStorageApiInsertError, MElement> {

        private final String job;
        private final String name;
        private final Counter errorCounter;

        FailedStorageApiRecordDoFn(final String job, final String name) {
            this.job = job;
            this.name = name;
            this.errorCounter = Metrics.counter(name, "elements_insert_error");
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final BigQueryStorageApiInsertError error = c.element();
            final TableRow tableRow = error.getRow();
            final MFailure failureElement = MFailure.of(
                    job, name, tableRow.toString(), error.getErrorMessage(), c.timestamp());

            c.output(failureElement.toElement(c.timestamp()));

            LOG.error("Failed to write record: " + tableRow + " with error message: " + error.getErrorMessage());
        }

    }

    private static class DynamicDestinationFunc<T> extends DynamicDestinations<T, String> {

        private final Schema tableSchema;
        private final SerializableFunction<T, String> destinationFunction;
        private final String partitioningType;
        private final String partitioningField;
        private final List<String> clusteringFields;

        public DynamicDestinationFunc(
                final Schema tableSchema,
                final SerializableFunction<T, String> destinationFunction,
                final Parameters parameters) {

            this.tableSchema = tableSchema;
            this.destinationFunction = destinationFunction;
            this.partitioningType = parameters.partitioning;
            this.partitioningField = parameters.partitioningField;
            this.clusteringFields = parameters.clusteringFields;
        }

        @Override
        public String getDestination(ValueInSingleWindow<T> element) {
            final T input = element.getValue();
            return destinationFunction.apply(input);
        }

        @Override
        public TableDestination getTable(String destination) {
            if(BigQueryUtil.isPartitioningTable(destination)) {
                final TimePartitioning timePartitioning = new TimePartitioning();
                if(partitioningType != null) {
                    timePartitioning.setType(partitioningType);
                    if(partitioningField != null) {
                        timePartitioning.setField(partitioningField);
                    }
                }
                if(clusteringFields != null && !clusteringFields.isEmpty()) {
                    final Clustering clustering = new Clustering().setFields(clusteringFields);
                    return new TableDestination(destination, null, timePartitioning, clustering);
                }
                return new TableDestination(destination, null, timePartitioning);
            } else {
                return new TableDestination(destination, null);
            }
        }

        @Override
        public TableSchema getSchema(String destination) {
            return ElementToTableRowConverter.convertSchema(tableSchema);
        }

    }

    private static class ConvertRowMutationDoFn extends DoFn<UnifiedMutation, RowMutation> {

        private final Map<String, List<String>> primaryKeyFields;

        public ConvertRowMutationDoFn(final Map<String, List<String>> primaryKeyFields) {
            this.primaryKeyFields = primaryKeyFields;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final List<String> pk = primaryKeyFields.get(c.element().getTable());
        }

    }

    private static class MutationDynamicDestinationFunc extends DynamicDestinations<UnifiedMutation, String> {

        private final String dataset;
        private final Map<String, String> tableSchemas;
        public MutationDynamicDestinationFunc(final String dataset, final Map<String, String> tableSchemas) {
            this.dataset = dataset;
            this.tableSchemas = tableSchemas;
        }

        @Override
        public String getDestination(ValueInSingleWindow<UnifiedMutation> element) {
            return String.format("%s.%s", dataset, element.getValue().getTable());
        }
        @Override
        public TableDestination getTable(String destination) {
            return new TableDestination(destination, null);
        }
        @Override
        public TableSchema getSchema(String destination) {
            final String tableName = extractTableName(destination);
            if(tableName == null) {
                throw new IllegalArgumentException("illegal destination: " + destination);
            }
            if(!tableSchemas.containsKey(tableName)) {
                throw new IllegalArgumentException("tableSchemas does not contains tableName: " + tableName + " in tableSchemas: " + tableSchemas);
            }
            final org.apache.avro.Schema schema = AvroSchemaUtil.convertSchema(tableSchemas.get(tableName));
            return AvroToTableRowConverter.convertSchema(schema);
        }

        private String extractTableName(final String destination) {
            final String[] strs = destination.split("\\.");
            if(strs.length < 3) {
                throw new IllegalArgumentException("Illegal destination: " + destination);
            }
            return strs[2];
        }

    }

}
