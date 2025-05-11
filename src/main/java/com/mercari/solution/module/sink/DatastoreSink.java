package com.mercari.solution.module.sink;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.converter.ElementToEntityConverter;
import freemarker.template.Template;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Sink.Module(name="datastore")
public class DatastoreSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSink.class);

    private static class Parameters implements Serializable {

        private String projectId;
        private String kind;
        private List<String> keyFields;
        private String keyTemplate;
        private Boolean delete;
        private List<String> excludeFromIndexFields;
        private Boolean enableRampupThrottling;

        private String separator;

        public void validate() {
            if(projectId == null) {
                throw new IllegalModuleException("parameters.projectId must not be null");
            }
        }

        public void setDefaults() {
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(delete == null) {
                delete = false;
            }
            if(excludeFromIndexFields == null) {
                excludeFromIndexFields = new ArrayList<>();
            }
            if(enableRampupThrottling == null) {
                enableRampupThrottling = false;
            }
            if(separator == null) {
                separator = "#";
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

        final String execEnvProject = inputs.getPipeline().getOptions().as(GcpOptions.class).getProject();

        final PCollection<MElement> input = inputs.apply("Union", Union.flatten()
                .withWaits(getWaits())
                .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<Entity> entities = input
                .apply("ConvertEntity", ParDo
                        .of(new EntityDoFn(inputSchema, parameters)));

        final String projectId = Optional.ofNullable(parameters.projectId).orElse(execEnvProject);

        if(parameters.delete) {
            final DatastoreV1.DeleteEntity delete = DatastoreIO
                    .v1()
                    .deleteEntity()
                    .withProjectId(projectId);
            final PDone done = entities.apply("DeleteEntity", delete);
            return MCollectionTuple.done(done);
        } else {
            final DatastoreV1.Write write;
            if(parameters.enableRampupThrottling) {
                write = DatastoreIO.v1().write()
                        .withProjectId(projectId);
            } else {
                write = DatastoreIO.v1().write()
                        .withRampupThrottlingDisabled()
                        .withProjectId(projectId);
            }
            final PCollection<MElement> output = entities
                    .apply("WriteEntity", write.withResults())
                    .apply("Format", ParDo.of(new SummaryWriteDoFn()));
            return MCollectionTuple.of(output, createSummarySchema());
        }

    }

    private static class EntityDoFn extends DoFn<MElement, Entity> {

        private final Schema inputSchema;
        private final String kind;
        private final List<String> keyFields;
        private final String keyTemplate;
        private final List<String> excludeFromIndexFields;

        private final String separator;

        private transient Template templateKey;

        public EntityDoFn(final Schema inputSchema,
                          final Parameters parameters) {

            this.inputSchema = inputSchema;
            this.kind = parameters.kind;
            this.keyFields = parameters.keyFields;
            this.keyTemplate = parameters.keyTemplate;
            this.excludeFromIndexFields = parameters.excludeFromIndexFields;
            this.separator = parameters.separator;
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
            if(keyTemplate != null) {
                this.templateKey = TemplateUtil.createStrictTemplate("keyTemplate", keyTemplate);
            } else {
                this.templateKey = null;
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final Entity.Builder builder = ElementToEntityConverter.convertBuilder(inputSchema, element, excludeFromIndexFields);

            // Generate key
            final com.google.datastore.v1.Key key;
            if(keyFields != null && keyFields.size() == 1
                    && builder.containsProperties(keyFields.get(0))
                    && builder.getPropertiesOrThrow(keyFields.get(0)).getValueTypeCase().equals(Value.ValueTypeCase.INTEGER_VALUE)) {

                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setId(builder.getPropertiesOrThrow(keyFields.get(0)).getIntegerValue())
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();;
            } else if((keyFields != null && !keyFields.isEmpty()) || templateKey != null) {
                final String keyString;
                if(keyFields != null && !keyFields.isEmpty()) {
                    final StringBuilder sb = new StringBuilder();
                    for (final String keyField : keyFields) {
                        final String keyValue = element.getAsString(keyField);
                        sb.append(keyValue);
                        sb.append(separator);
                    }
                    sb.deleteCharAt(sb.length() - separator.length());
                    keyString = sb.toString();
                } else {
                    final Map<String,Object> data = element.asStandardMap(inputSchema, null);
                    TemplateUtil.setFunctions(data);
                    keyString = TemplateUtil.executeStrictTemplate(templateKey, data);
                }

                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setName(keyString)
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();
            } else if(builder.getKey() != null) {
                final int index = builder.getKey().getPathCount() - 1;
                final com.google.datastore.v1.Key.PathElement pathElement = builder
                        .getKey()
                        .getPathList()
                        .get(index)
                        .toBuilder()
                        .setKind(kind)
                        .build();
                key = builder.getKey().toBuilder().setPath(index, pathElement).build();
            } else {
                final String keyString = UUID.randomUUID().toString();
                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setName(keyString)
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();
            }

            final Entity entity = builder.setKey(key).build();
            c.output(entity);
        }

    }

    private static class SummaryWriteDoFn extends DoFn<DatastoreV1.WriteSuccessSummary, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final DatastoreV1.WriteSuccessSummary successSummary = c.element();
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

    private static Schema createSummarySchema() {
        return Schema.builder()
                .withField("numWrites", Schema.FieldType.INT32)
                .withField("numBytes", Schema.FieldType.INT64)
                .build();
    }

}
