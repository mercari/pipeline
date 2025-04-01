package com.mercari.solution.module.source;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.gcp.BigQueryUtil;
import com.mercari.solution.util.gcp.ParameterManagerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.MicroBatch;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Source.Module(name="bigquery")
public class BigQuerySource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);

    private static final Counter ERROR_COUNTER = Metrics.counter("bigquery_source", "error");;

    private static class BigQuerySourceParameters implements Serializable {

        // for query read
        private String query;
        private BigQueryIO.TypedRead.QueryPriority queryPriority;
        private String queryTempDataset;
        private String queryLocation;

        // for table read
        private String projectId;
        private String datasetId;
        private String table;
        private List<String> fields;
        private String rowRestriction;

        // for microBatch parameter
        private MicroBatch.MicroBatchParameter microBatch;

        // for view parameter
        private ViewParameter view;

        // common
        private Mode mode;
        private String kmsKey;
        private BigQueryIO.TypedRead.Method method;
        private DataFormat format;
        private String queryRunProjectId;

        //@Deprecated
        //private String table; // use projectId, datasetId, tableId


        private static class ViewParameter implements Serializable {

            private String keyField;
            private Integer intervalMinute;

            public List<String> validate(String name) {

                final List<String> errorMessages = new ArrayList<>();
                if(keyField == null) {
                    errorMessages.add("bigquery source module[" + name + "].view requires 'keyField' parameter");
                }
                return errorMessages;
            }

            private void setDefaults() {
                if(intervalMinute == null) {
                    intervalMinute = 60;
                }
            }

        }

        private void validate(final String name) {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(mode != null) {
                switch (mode) {
                    case batch -> {
                        if(query == null && (table == null && datasetId == null)) {
                            errorMessages.add("parameters.query or table is required");
                        }
                    }
                    case microBatch -> {
                        if(microBatch == null) {
                            errorMessages.add("parameters.microBatch is required if mode is 'microBatch'");
                        } else {
                            errorMessages.addAll(microBatch.validate(name));
                        }
                    }
                    case view -> {
                        if(query == null) {
                            errorMessages.add("parameters.query or table is required");
                        }
                        if(view == null) {
                            errorMessages.add("parameters.view is required if mode is 'microBatch'");
                        } else {
                            errorMessages.addAll(view.validate(name));
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final PInput input) {
            if(projectId == null) {
                projectId = DataflowOptions.getProject(input.getPipeline().getOptions());
            }
            if(queryPriority == null) {
                this.queryPriority = BigQueryIO.TypedRead.QueryPriority.INTERACTIVE;
            }
            if(mode == null) {
                mode = Mode.batch;
            }
            if(queryRunProjectId == null) {
                queryRunProjectId = DataflowOptions.getProject(input.getPipeline().getOptions());
            }
            if(view != null) {
                view.setDefaults();
            }
        }
    }

    private enum Mode {
        batch,
        microBatch,
        view
    }


    @Override
    public MCollectionTuple expand(PBegin begin) {

        final BigQuerySourceParameters parameters = getParameters(BigQuerySourceParameters.class);
        parameters.validate(getName());
        parameters.setDefaults(begin);

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<MElement> failureTag = new TupleTag<>(){};

        return switch (parameters.mode) {
            case batch -> {
                final KV<org.apache.avro.Schema, BigQueryIO.TypedRead<GenericRecord>> schemaAndRead = createRead(
                        parameters, getTemplateArgs(), getRunner());
                final org.apache.avro.Schema outputAvroSchema = schemaAndRead.getKey();
                final PCollection<GenericRecord> records = begin
                        .apply("ReadBigQuery", schemaAndRead.getValue())
                        .setCoder(AvroCoder.of(outputAvroSchema));

                final Schema outputSchema = Schema.of(outputAvroSchema);
                final PCollection<MElement> elements = records
                        .apply("Format", ParDo.of(new FormatDoFn()))
                        .setCoder(ElementCoder.of(outputSchema));

                if(getTimestampAttribute() != null) {
                    if(!outputSchema.hasField(getTimestampAttribute())) {
                        throw new IllegalArgumentException("BigQuery source module: " + getName() + " is specified timestampAttribute: " + getTimestampAttribute() + ", but not found in input schema: " + outputAvroSchema);
                    }
                }

                yield MCollectionTuple
                        .of(elements, outputSchema);
            }
            case view -> {
                final String query;
                if(parameters.query.startsWith("gs://")) {
                    final String rawQuery = StorageUtil.readString(parameters.query);
                    query = TemplateUtil.executeStrictTemplate(rawQuery, getTemplateArgs());
                } else {
                    query = parameters.query;
                }
                parameters.query = query;
                final ViewSource source = new ViewSource(getJobName(), getName(), parameters, outputTag, failureTag);
                final PCollectionTuple outputs = begin.apply("View", source);
                yield MCollectionTuple
                        .of(outputs.get(outputTag), source.schema)
                        .failure(outputs.get(failureTag));
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private class FormatDoFn extends DoFn<GenericRecord, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement output = MElement.of(c.element(), c.timestamp());
            if(getTimestampAttribute() != null) {
                final Instant eventTime = output.getAsJodaInstant(getTimestampAttribute());
                final MElement outputWithTimestamp = output.withEventTime(eventTime);
                c.outputWithTimestamp(outputWithTimestamp, eventTime);
            } else {
                c.output(output);
            }
        }
    }

    private static KV<org.apache.avro.Schema, BigQueryIO.TypedRead<GenericRecord>> createRead(
            final BigQuerySourceParameters parameters,
            final Map<String, Object> templateArgs,
            final MPipeline.Runner runner) {

        BigQueryIO.TypedRead<GenericRecord> read = BigQueryIO
                .read(SchemaAndRecord::getRecord)
                .useAvroLogicalTypes()
                .withoutValidation();

        if(parameters.kmsKey != null) {
            read = read.withKmsKey(parameters.kmsKey);
        }

        if(parameters.query != null) {
            final String query;
            if(parameters.query.startsWith("gs://")) {
                LOG.info("query parameter is GCS path: {}", parameters.query);
                final String rawQuery = StorageUtil.readString(parameters.query);
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else if(ParameterManagerUtil.isParameterVersionResource(parameters.query)) {
                LOG.info("query parameter is Parameter Manager resource: {}", parameters.query);
                final ParameterManagerUtil.Version version = ParameterManagerUtil.getParameterVersion(parameters.query);
                if(version.payload == null) {
                    throw new IllegalArgumentException("query resource does not exists for: " + parameters.query);
                }
                query = new String(version.payload, StandardCharsets.UTF_8);
            } else {
                query = parameters.query;
            }

            read = read
                    .fromQuery(query)
                    .usingStandardSql()
                    .withQueryPriority(parameters.queryPriority);

            if(parameters.queryLocation != null) {
                read = read.withQueryLocation(parameters.queryLocation);
            }
            if(parameters.queryTempDataset != null) {
                if(parameters.queryTempDataset.contains(".")) {
                    final String[] strs = parameters.queryTempDataset.split("\\.", 2);
                    read = read.withQueryTempProjectAndDataset(strs[0], strs[1]);
                } else {
                    read = read.withQueryTempDataset(parameters.queryTempDataset);
                }
            }

            final TableSchema tableSchema = BigQueryUtil.getTableSchemaFromQuery(parameters.queryRunProjectId, query);
            final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(tableSchema);

            final BigQueryIO.TypedRead.Method method = Optional
                    .ofNullable(parameters.method)
                    .orElseGet(() -> BigQueryUtil.getPreferReadMethod(tableSchema));

            read = read.withMethod(method);

            return KV.of(avroSchema, read.withCoder(AvroGenericCoder.of(avroSchema)));
        } else if(parameters.table != null || (parameters.datasetId != null)) {
            final TableReference tableReference = createTableReference(parameters);
            read = read
                    .from(tableReference)
                    .withoutValidation();
            if(parameters.fields != null) {
                read = read.withSelectedFields(parameters.fields);
            }
            if(parameters.rowRestriction != null) {
                read = read.withRowRestriction(parameters.rowRestriction);
            }
            if(parameters.format != null) {
                read = read.withFormat(parameters.format);
            }
            final BigQueryIO.TypedRead.Method method = Optional
                    .ofNullable(parameters.method)
                    .orElse(BigQueryIO.TypedRead.Method.DIRECT_READ);
            read = read.withMethod(method);

            return switch (runner) {
                case direct -> {
                    final TableSchema tableSchema = BigQueryUtil.getTableSchemaFromTable(tableReference);
                    final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(tableSchema);
                    yield KV.of(avroSchema, read.withCoder(AvroGenericCoder.of(avroSchema)));
                }
                case dataflow -> {
                    final org.apache.avro.Schema avroSchema = BigQueryUtil.getTableSchemaFromTableStorage(
                            tableReference, parameters.queryRunProjectId, parameters.fields, parameters.rowRestriction);
                    yield KV.of(avroSchema, read.withCoder(AvroGenericCoder.of(avroSchema)));
                }
                default -> throw new IllegalArgumentException();
            };
        } else {
            throw new IllegalArgumentException("bigquery module support only query or table");
        }
    }

    private static class ViewSource extends PTransform<PBegin, PCollectionTuple> {

        private final String jobName;
        private final String moduleName;
        private final BigQuerySourceParameters parameters;
        private final TupleTag<MElement> outputTag;
        private final TupleTag<MElement> failuresTag;

        private transient Schema schema;

        ViewSource(
                final String jobName,
                final String moduleName,
                final BigQuerySourceParameters parameters,
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

            final TableSchema tableSchema = BigQueryUtil.getTableSchemaFromQuery(
                    parameters.queryRunProjectId, parameters.query);
            this.schema = Schema.of(tableSchema);

            final PCollection<Long> sequence;
            if(OptionUtil.isStreaming(begin)) {
                sequence = begin
                        .apply("Generate", GenerateSequence
                                .from(0)
                                .withRate(1, Duration.standardMinutes(parameters.view.intervalMinute)));
            } else {
                sequence = begin
                        .apply("Create", Create.of(0L).withCoder(VarLongCoder.of()));
            }

            return sequence
                    .apply(ParDo.of(new QueryMapDoFn(jobName, moduleName, parameters, failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        private static class QueryMapDoFn extends DoFn<Long, MElement> {

            private final String job;
            private final String name;
            private final BigQuerySourceParameters parameters;
            private final TupleTag<MElement> failuresTag;

            private transient TableSchema schema;

            QueryMapDoFn(
                    final String job,
                    final String name,
                    final BigQuerySourceParameters parameters,
                    final TupleTag<MElement> failuresTag) {

                this.job = job;
                this.name = name;
                this.parameters = parameters;
                this.failuresTag = failuresTag;
            }

            @Setup
            public void setup() {
                try {
                    this.schema = BigQueryUtil.getTableSchemaFromQuery(
                            parameters.queryRunProjectId, parameters.query);
                } catch (final Throwable e) {
                    ERROR_COUNTER.inc();
                    LOG.error("BigQuery source view setup error: {}", MFailure.convertThrowableMessage(e));
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                try {
                    long startMillis = Instant.now().getMillis();
                    final Bigquery bigquery = BigQueryUtil.getBigquery();
                    final QueryRequest queryRequest = new QueryRequest()
                            .setQuery(parameters.query)
                            .setUseLegacySql(false);
                    if(parameters.queryLocation != null) {
                        queryRequest.setLocation(parameters.queryLocation);
                    }

                    final List<TableRow> tableRows = BigQueryUtil.queryBatch(
                            bigquery,
                            parameters.queryRunProjectId,
                            queryRequest);

                    final Map<String, Object> map = new HashMap<>();
                    for(final TableRow tableRow : tableRows) {
                        final Map<String, Object> values = BigQueryUtil.parseAsPrimitiveValues(schema, tableRow);
                        final String key = Optional.ofNullable(values.get(parameters.view.keyField)).map(Object::toString).orElse("");
                        map.put(key, values);
                    }
                    final MElement element = MElement.of(map, c.timestamp());
                    c.output(element);

                    LOG.info("BigQuery source view query took: {} millis", Instant.now().getMillis() - startMillis);
                } catch (final Throwable e) {
                    ERROR_COUNTER.inc();
                    LOG.error("BigQuery source view error: {}", MFailure.convertThrowableMessage(e));
                    final MElement failure = MFailure
                            .of(job, name, "view query: " + parameters.query, e, c.timestamp())
                            .toElement(c.timestamp());
                    c.output(failuresTag, failure);
                }

            }

        }
    }

    private static TableReference createTableReference(final BigQuerySourceParameters parameters) {

        final TableReference tableReference = new TableReference();
        if(parameters.table != null) {
            final String[] table = parameters.table.trim().replaceAll(":", ".").split("\\.");
            if(table.length == 3) {
                tableReference
                        .setProjectId(table[0])
                        .setDatasetId(table[1])
                        .setTableId(table[2]);
            } else if(table.length == 2) {
                final String project = parameters.queryRunProjectId;
                tableReference
                        .setProjectId(project)
                        .setDatasetId(table[0])
                        .setTableId(table[1]);
            } else if(table.length == 1 && parameters.datasetId != null) {
                final String projectId = Optional
                        .ofNullable(parameters.projectId)
                        .orElse(parameters.queryRunProjectId);
                tableReference
                        .setProjectId(projectId)
                        .setDatasetId(parameters.datasetId)
                        .setTableId(parameters.table);
            } else {
                throw new IllegalArgumentException("Illegal table parameter: " + parameters.table + ". should contains at least dataset and table (ex: `dataset_id.table`)");
            }
        } else {
            throw new IllegalArgumentException("parameters.table must not be null");
        }
        return tableReference;
    }

}