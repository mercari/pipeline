package com.mercari.solution.module.sink;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Write;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.converter.ElementToDocumentConverter;
import freemarker.template.Template;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


@Sink.Module(name="firestore")
public class FirestoreSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSink.class);

    private static class Parameters implements Serializable {

        private String projectId;
        private String databaseId;
        private String collection;
        private List<String> nameFields;
        private String nameTemplate;
        private Boolean delete;
        private Boolean failFast;
        private String separator;

        private RpcQos rpcQos;


        public String getProjectId() {
            return projectId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public String getCollection() {
            return collection;
        }

        public List<String> getNameFields() {
            return nameFields;
        }

        public String getNameTemplate() {
            return nameTemplate;
        }

        public Boolean getDelete() {
            return delete;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public String getSeparator() {
            return separator;
        }

        public RpcQos getRpcQos() {
            return rpcQos;
        }

        private void validate(final String name) {
            if((this.collection == null || this.nameFields == null) && this.nameTemplate == null) {
                //throw new IllegalArgumentException("Firestore sink module requires name parameter!");
            }
        }

        private void setDefaults(final PInput input) {
            if(this.projectId == null) {
                this.projectId = OptionUtil.getDefaultProject();
            }
            if(this.databaseId == null) {
                this.databaseId = FirestoreUtil.DEFAULT_DATABASE_NAME;
            }
            if(!FirestoreUtil.DEFAULT_DATABASE_NAME.equals(this.databaseId)) {
                input.getPipeline().getOptions().as(FirestoreOptions.class)
                        .setFirestoreDb(this.databaseId);
            }
            if(this.nameFields == null) {
                this.nameFields = new ArrayList<>();
            }
            if(this.delete == null) {
                delete = false;
            }
            if(this.failFast == null) {
                this.failFast = true;
            }
            if(this.separator == null) {
                this.separator = "#";
            }
            if(this.rpcQos == null) {
                this.rpcQos = new RpcQos();
            }
            this.rpcQos.setDefaults(input);
        }
    }

    private static class RpcQos implements Serializable {

        private Integer batchInitialCount;
        private Integer batchMaxCount;
        private Integer batchTargetLatency;
        private Integer initialBackoff;
        private Integer maxAttempts;
        private Integer overloadRatio;
        private Integer samplePeriod;
        private Integer samplePeriodBucketSize;
        private Integer throttleDuration;
        private Integer hintMaxNumWorkers;

        public void setDefaults(final PInput input) {
            if(this.hintMaxNumWorkers == null) {
                this.hintMaxNumWorkers = DataflowOptions.getMaxNumWorkers(input);
            }
            if(this.hintMaxNumWorkers < 1) {
                this.hintMaxNumWorkers = 10;
            }
        }

        public RpcQosOptions create() {

            final RpcQosOptions.Builder builder = RpcQosOptions.defaultOptions().toBuilder();
            if(batchInitialCount != null) {
                builder.withBatchInitialCount(this.batchInitialCount);
            }
            if(batchMaxCount != null) {
                builder.withBatchMaxCount(this.batchMaxCount);
            }
            if(batchTargetLatency != null) {
                builder.withBatchTargetLatency(Duration.standardSeconds(this.batchTargetLatency));
            }
            if(initialBackoff != null) {
                builder.withInitialBackoff(Duration.standardSeconds(this.initialBackoff));
            }
            if(maxAttempts != null) {
                builder.withMaxAttempts(this.maxAttempts);
            }
            if(overloadRatio != null) {
                builder.withOverloadRatio(this.overloadRatio);
            }
            if(samplePeriod != null) {
                builder.withSamplePeriod(Duration.standardSeconds(this.samplePeriod));
            }
            if(samplePeriodBucketSize != null) {
                builder.withSamplePeriodBucketSize(Duration.standardSeconds(this.samplePeriodBucketSize));
            }
            if(throttleDuration != null) {
                builder.withThrottleDuration(Duration.standardSeconds(this.throttleDuration));
            }
            if(hintMaxNumWorkers != null) {
                builder.withHintMaxNumWorkers(this.hintMaxNumWorkers);
            }

            return builder.build();
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults(inputs);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<Write> writes = input
                .apply("ConvertToDocument", ParDo.of(new ConvertWriteDoFn(parameters, inputSchema)));

        if(getFailFast()) {
            final PCollection<MElement> output = writes
                    .apply("WriteDocument", FirestoreIO.v1().write()
                            .batchWrite()
                            .withRpcQosOptions(parameters.getRpcQos().create())
                            .build())
                    .apply("Format", ParDo.of(new SummaryDoFn()));
            return MCollectionTuple.of(output, createSummarySchema());
        } else {
            final PCollection<BadRecord> badRecords = writes
                    .apply("WriteDocument", FirestoreIO.v1().write()
                            .batchWrite()
                            .withRpcQosOptions(parameters.getRpcQos().create())
                            .withDeadLetterQueue()
                            .build())
                    .apply("Format", ParDo.of(new FailureDoFn()));
            errorHandler.addError(badRecords);
            return MCollectionTuple.empty(inputs.getPipeline());
        }

    }

    private static class ConvertWriteDoFn extends DoFn<MElement, Write> {

        private final Schema inputSchema;

        private final String project;
        private final String database;
        private final String collection;
        private final List<String> nameFields;
        private final String nameTemplateText;
        private final boolean delete;
        private final String separator;

        private transient Template nameTemplate;

        ConvertWriteDoFn(
                final Parameters parameters,
                final Schema inputSchema) {

            this.inputSchema = inputSchema;
            this.project = parameters.getProjectId();
            this.database = parameters.getDatabaseId();
            this.collection = parameters.getCollection();
            this.nameFields = parameters.getNameFields();
            this.nameTemplateText = parameters.getNameTemplate();
            this.delete = parameters.getDelete();
            this.separator = parameters.getSeparator();
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
            if(nameTemplateText != null) {
                this.nameTemplate = TemplateUtil.createStrictTemplate("firestoreSinkNameTemplate", nameTemplateText);
            } else {
                this.nameTemplate = null;
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            final String name;
            if(nameFields.isEmpty() && nameTemplate == null) {
                final String defaultNameValue = input.getAsString(FirestoreUtil.NAME_FIELD);
                if(defaultNameValue == null) {
                    name = createName(UUID.randomUUID().toString());
                } else if(defaultNameValue.startsWith("projects/")) {
                    name = defaultNameValue;
                } else {
                    name = createName(defaultNameValue);
                }
            } else if(nameTemplate == null) {
                final String fieldValue = nameFields.stream()
                        .map(nameField -> input.getAsString(nameField))
                        .collect(Collectors.joining(separator));
                name = createName(fieldValue);
            } else {
                final Map<String, Object> data = input.asStandardMap(inputSchema, null);
                TemplateUtil.setFunctions(data);
                final String path = TemplateUtil.executeStrictTemplate(nameTemplate, data);
                name = createName(path);
            }

            if(delete) {
                final Write delete = Write.newBuilder()
                        .setDelete(name)
                        .build();
                c.output(delete);
            } else {
                final Document document = ElementToDocumentConverter
                        .convertBuilder(inputSchema, input)
                        .setName(name)
                        .build();
                final Write write = Write.newBuilder()
                        .setUpdate(document)
                        .build();
                c.output(write);
            }
        }

        private String createName(final String nameString) {
            if(collection == null) {
                return FirestoreUtil.createName(project, database, nameString);
            } else {
                return FirestoreUtil.createName(project, database, collection, nameString);
            }
        }
    }

    private static class SummaryDoFn extends DoFn<FirestoreV1.WriteSuccessSummary, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final FirestoreV1.WriteSuccessSummary successSummary = c.element();
            if(successSummary == null) {
                return;
            }
            final MElement output = MElement.builder()
                    .withInt32("numWrites", successSummary.getNumWrites())
                    .withInt64("numBytes", successSummary.getNumBytes())
                    .withEventTime(c.timestamp())
                    .build();
            c.output(output);
        }

    }

    private static class FailureDoFn extends DoFn<FirestoreV1.WriteFailure, BadRecord> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final FirestoreV1.WriteFailure writeFailure = c.element();
            if(writeFailure == null) {
                return;
            }
            // TODO
        }

    }

    private static Schema createSummarySchema() {
        return Schema.builder()
                .withField("numWrites", Schema.FieldType.INT32)
                .withField("numBytes", Schema.FieldType.INT64)
                .build();
    }

}
