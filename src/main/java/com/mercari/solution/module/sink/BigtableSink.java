package com.mercari.solution.module.sink;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.*;
import freemarker.template.Template;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Sink.Module(name="bigtable")
public class BigtableSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSink.class);

    private static class BigtableSinkParameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String tableId;

        private String rowKey;
        private List<BigtableSchemaUtil.ColumnFamilyProperties> columns;

        // default config
        private BigtableSchemaUtil.Format format;
        private BigtableSchemaUtil.MutationOp mutationOp;
        private BigtableSchemaUtil.TimestampType timestampType;

        private Boolean withWriteResults;

        // performance control config
        private String appProfileId;
        private Boolean flowControl;
        private Long maxBytesPerBatch;
        private Long maxElementsPerBatch;
        private Long maxOutstandingBytes;
        private Long maxOutstandingElements;

        private Integer operationTimeoutSecond;
        private Integer attemptTimeoutSecond;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("parameters.projectId must not be null");
            }
            if(instanceId == null) {
                errorMessages.add("parameters.instanceId must not be null");
            }
            if(tableId == null) {
                errorMessages.add("parameters.tableId must not be null");
            }
            if(rowKey == null) {
                errorMessages.add("parameters.rowKey must not be null");
            }
            if(columns == null || columns.isEmpty()) {
                if(!BigtableSchemaUtil.MutationOp.DELETE_FROM_ROW.equals(mutationOp)) {
                    errorMessages.add("parameters.columns must not be empty");
                }
            } else {
                for(int i=0; i<columns.size(); i++) {
                    errorMessages.addAll(columns.get(i).validate(i));
                }
            }
            if(maxBytesPerBatch != null) {
                if(maxBytesPerBatch <= 0) {
                    errorMessages.add("parameters.maxBytesPerBatch must be over zero");
                }
            }
            if(maxElementsPerBatch != null) {
                if(maxElementsPerBatch <= 0) {
                    errorMessages.add("parameters.maxElementsPerBatch must be over zero");
                }
            }
            if(maxOutstandingBytes != null) {
                if(maxOutstandingBytes <= 0) {
                    errorMessages.add("parameters.maxOutstandingBytes must be over zero");
                }
            }
            if(maxOutstandingElements != null) {
                if(maxOutstandingElements <= 0) {
                    errorMessages.add("parameters.maxOutstandingElements must be over zero");
                }
            }

            if(operationTimeoutSecond != null) {
                if(operationTimeoutSecond <= 0) {
                    errorMessages.add("parameters.operationTimeoutSecond must be over zero");
                }
            }
            if(attemptTimeoutSecond != null) {
                if(attemptTimeoutSecond <= 0) {
                    errorMessages.add("parameters.attemptTimeoutSecond must be over zero");
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(format == null) {
                format = BigtableSchemaUtil.Format.bytes;
            }
            if(mutationOp == null) {
                mutationOp = BigtableSchemaUtil.MutationOp.SET_CELL;
            }
            if(timestampType == null) {
                timestampType = BigtableSchemaUtil.TimestampType.server;
            }
            if(columns == null) {
                columns = new ArrayList<>();
            }
            for(var column : columns) {
                column.setDefaults(format, mutationOp, timestampType);
            }
            if(withWriteResults == null) {
                withWriteResults = false;
            }
            if(flowControl == null) {
                flowControl = false;
            }
        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final BigtableSinkParameters parameters = getParameters(BigtableSinkParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final PCollection<KV<ByteString, Iterable<Mutation>>> mutation = input
                .apply("ToMutation", ParDo.of(new MutationDoFn(parameters, inputSchema)));
        final BigtableIO.Write write = createWrite(parameters);

        if(parameters.withWriteResults) {
            final PCollection<BigtableWriteResult> writeResults = mutation
                    .apply("WriteWithResult", write.withWriteResults());
            return null;
        } else {
            final PDone done = mutation
                    .apply("Write", write);
            return MCollectionTuple.done(done);
        }
    }

    private static BigtableIO.Write createWrite(
            final BigtableSinkParameters parameters) {

        BigtableIO.Write write = BigtableIO
                .write()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withTableId(parameters.tableId)
                .withoutValidation();

        if(parameters.appProfileId != null) {
            write = write.withAppProfileId(parameters.appProfileId);
        }
        if(parameters.flowControl != null) {
            write = write.withFlowControl(parameters.flowControl);
        }
        if(parameters.maxBytesPerBatch != null) {
            write = write.withMaxBytesPerBatch(parameters.maxBytesPerBatch);
        }
        if(parameters.maxElementsPerBatch != null) {
            write = write.withMaxElementsPerBatch(parameters.maxElementsPerBatch);
        }
        if(parameters.maxOutstandingBytes != null) {
            write = write.withMaxOutstandingBytes(parameters.maxOutstandingBytes);
        }
        if(parameters.maxOutstandingElements != null) {
            write = write.withMaxOutstandingElements(parameters.maxOutstandingElements);
        }
        if(parameters.operationTimeoutSecond != null) {
            write = write.withOperationTimeout(Duration.standardSeconds(parameters.operationTimeoutSecond));
        }
        if(parameters.attemptTimeoutSecond != null) {
            write = write.withAttemptTimeout(Duration.standardSeconds(parameters.attemptTimeoutSecond));
        }

        return write;
    }

    private static class MutationDoFn extends DoFn<MElement, KV<ByteString, Iterable<Mutation>>> {

        private final String rowKey;
        private final List<BigtableSchemaUtil.ColumnFamilyProperties> columns;
        private final BigtableSchemaUtil.MutationOp mutationOp;
        private final Schema inputSchema;

        private final Set<String> valueArgs;
        private final Set<String> templateArgs;

        private transient org.apache.avro.Schema avroSchema;

        private transient Template templateRowKey;


        public MutationDoFn(final BigtableSinkParameters parameters, final Schema inputSchema) {
            this.rowKey = parameters.rowKey;
            this.columns = parameters.columns;
            this.mutationOp = parameters.mutationOp;
            this.inputSchema = inputSchema;

            this.templateArgs = new HashSet<>();
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(rowKey, inputSchema));
            this.valueArgs = new HashSet<>();
            if(columns != null && !columns.isEmpty()) {
                for(var column : columns) {
                    this.valueArgs.addAll(column.extractValueArgs());
                    this.templateArgs.addAll(column.extractTemplateArgs(inputSchema));
                }
            }
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
            this.templateRowKey = TemplateUtil.createStrictTemplate("rowKeyTemplate", rowKey);
            for(var column : columns) {
                column.setupSink();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }

            final Map<String,Object> templateVariables = element.asStandardMap(inputSchema, templateArgs);
            templateVariables.put("_timestamp", DateTimeUtil.toInstant(c.timestamp().getMillis() * 1000L));
            final String rowKeyString = TemplateUtil.executeStrictTemplate(templateRowKey, templateVariables);
            final ByteString rowKey = ByteString.copyFrom(rowKeyString, StandardCharsets.UTF_8);

            if(BigtableSchemaUtil.MutationOp.DELETE_FROM_ROW.equals(mutationOp)) {
                final Mutation mutation = Mutation.newBuilder()
                        .setDeleteFromRow(Mutation.DeleteFromRow.newBuilder()
                                .build())
                        .build();
                final KV<ByteString, Iterable<Mutation>> output = KV.of(rowKey, List.of(mutation));
                c.output(output);
                return;
            }

            final Map<String, Object> primitiveValues = element.asPrimitiveMap(valueArgs);
            final List<Mutation> mutations = BigtableSchemaUtil.toMutations(columns, primitiveValues, templateVariables, c.timestamp());
            final KV<ByteString, Iterable<Mutation>> output = KV.of(rowKey, mutations);
            c.output(output);
        }

    }

}
