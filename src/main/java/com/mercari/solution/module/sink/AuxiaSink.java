package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.crm.AuxiaUtil;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Sink.Module(name="auxia")
public class AuxiaSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(AuxiaSink.class);

    private static class Parameters implements Serializable {

        private AuxiaUtil.Type type;
        private String field;
        private String projectId;
        private String eventName;

        private PubSubSinkParameters pubsub;

        private Set<String> excludeFields;

        private void validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("projectId must not be null");
            }
            switch (Optional.ofNullable(type).orElse(AuxiaUtil.Type.element)) {
                case json -> {
                    if(field == null) {
                        errorMessages.add("field must not be null if type is json");
                    } else if(!inputSchema.hasField(field)) {
                        errorMessages.add("field: " + field + " does not exist in input schema: " + inputSchema);
                    } else {
                        switch (inputSchema.getField(field).getFieldType().getType()) {
                            case string, json, bytes -> {}
                            default -> errorMessages.add("field: " + field + " type must be json, string or bytes : " + inputSchema);
                        }
                    }
                }
                case element -> {
                    if(!inputSchema.hasField("user_id")) {
                        errorMessages.add("required field 'user_id' does not exist in input schema: " + inputSchema.getFields());
                    }
                    if(inputSchema.hasField("events")) {
                        final Schema eventSchema = inputSchema.getField("events").getFieldType().getArrayValueType().getElementSchema();
                        if(!eventSchema.hasField("client_event_timestamp")) {
                            errorMessages.add("required field 'client_event_timestamp' does not exist in event schema: " + eventSchema.getFields());
                        }
                        if(!eventSchema.hasField("event_name") && eventName == null) {
                            errorMessages.add("required field 'event_name' does not exist in event schema: " + eventSchema.getFields());
                        }
                    } else {
                        if(!inputSchema.hasField("client_event_timestamp")) {
                            errorMessages.add("required field 'client_event_timestamp' does not exist in input schema: " + inputSchema.getFields());
                        }
                        if(!inputSchema.hasField("event_name") && eventName == null) {
                            errorMessages.add("required field 'event_name' does not exist in input schema: " + inputSchema.getFields());
                        }
                    }
                    final AuxiaUtil.EventConverter converter = AuxiaUtil.createElementConverter(projectId, eventName, inputSchema, excludeFields);
                    errorMessages.addAll(converter.validate(inputSchema));
                }
            }

            if(pubsub == null) {
                errorMessages.add("pubsub must not be null");
            } else {
                errorMessages.addAll(pubsub.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }

        }

        private void setDefaults() {
            if(type == null) {
                this.type = AuxiaUtil.Type.element;
            }
            if(pubsub != null) {
                this.pubsub.setDefaults();
            }
            if(excludeFields == null) {
                this.excludeFields = new HashSet<>();
            }
        }
    }

    private static class PubSubSinkParameters implements Serializable {

        private String topic;
        private Map<String, String> attributes;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(topic == null) {
                errorMessages.add("pubsub.topic must not be null");
            } else if(!PubSubUtil.isTopicResource(topic)) {
                errorMessages.add("pubsub.topic is illegal: " + topic);
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(attributes == null) {
                attributes = new HashMap<>();
            }
        }

        private PubsubIO.Write<org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage> createPububWrite(
                final MErrorHandler errorHandler) {

            PubsubIO.Write<org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage> write = PubsubIO
                    .writeMessages()
                    .to(topic);
            if (maxBatchSize != null) {
                write = write.withMaxBatchSize(maxBatchSize);
            }
            if (maxBatchBytesSize != null) {
                write = write.withMaxBatchBytesSize(maxBatchBytesSize);
            }

            errorHandler.apply(write);

            return write;
        }
    }

    public MCollectionTuple expand(
            final @NotNull MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        parameters.validate(inputSchema);
        parameters.setDefaults();

        final TupleTag<PubsubMessage> outputTag = new TupleTag<>(){};
        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};

        final PCollectionTuple messagesWithFailures = input
                .apply("Format", ParDo
                        .of(new PubSubFormatDoFn(
                                getJobName(), getName(),
                                parameters, inputSchema, inputs.getAllInputs(),
                                AuxiaUtil.getFileDescriptorSet().toByteArray(),
                                getLoggings(),
                                failureTag, getFailFast()))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(messagesWithFailures.get(failureTag));

        final PCollection<PubsubMessage> messages = messagesWithFailures.get(outputTag);
        final PDone done = messages
                .apply("Publish", parameters.pubsub.createPububWrite(errorHandler));
        return MCollectionTuple
                .done(done);
    }

    private static class PubSubFormatDoFn extends DoFn<MElement, PubsubMessage> {

        private final String jobName;
        private final String name;

        private final List<String> inputNames;
        private final Schema inputSchema;

        private final AuxiaUtil.Type type;
        private final String field;
        private final String auxiaProjectId;
        private final String auxiaEventName;

        private final byte[] bytes;

        private final Map<String, String> attributes;
        private final List<String> attributeTemplateArgs;

        private final Set<String> excludeFields;

        private final Map<String, Logging> logging;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private transient AuxiaUtil.EventConverter converter;
        private transient Map<String, Template> attributeTemplates;


        PubSubFormatDoFn(
                final String jobName,
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final List<String> inputNames,
                final byte[] bytes,
                final List<Logging> logging,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;

            this.type = parameters.type;
            this.field = parameters.field;
            this.auxiaProjectId = parameters.projectId;
            this.auxiaEventName = parameters.eventName;

            this.inputSchema = inputSchema;
            this.inputNames = inputNames;

            this.bytes = bytes;

            this.attributes = new HashMap<>(parameters.pubsub.attributes);
            this.attributeTemplateArgs = new ArrayList<>();
            for(final Map.Entry<String, String> entry : this.attributes.entrySet()) {
                this.attributeTemplateArgs.addAll(
                        TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema));
            }

            this.excludeFields = parameters.excludeFields;
            LOG.info("attributeTemplateArgs: {}", this.attributeTemplateArgs);

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.converter = switch (type) {
                case json -> AuxiaUtil.createJsonConverter(auxiaProjectId, auxiaEventName, field, excludeFields);
                case element -> AuxiaUtil.createElementConverter(auxiaProjectId, auxiaEventName, inputSchema, excludeFields);
            };
            this.converter.setup(bytes);

            this.attributeTemplates = new HashMap<>();
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                final String templateName = "auxiaSinkAttributeTemplate" + entry.getKey();
                final Template template = TemplateUtil.createStrictTemplate(templateName, entry.getValue());
                this.attributeTemplates.put(entry.getKey(), template);
            }

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);
                final List<AuxiaUtil.Event> events = converter.convert(input, c.timestamp());
                for(final AuxiaUtil.Event event : events) {
                    final Map<String, String> attributes = getAttributes(input);
                    if(event.insertId != null) {
                        attributes.put("id", event.insertId);
                    }
                    final PubsubMessage pubsubMessage = new PubsubMessage(event.message.toByteArray(), attributes, event.insertId, null);
                    c.output(pubsubMessage);
                    Logging.log(LOG, logging, "output", pubsubMessage.toString());
                }
            } catch (final Throwable e) {
                final String source = inputNames.get(input.getIndex());
                final BadRecord badRecord = processError("Failed to create pubsub message from input: " + source, input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private Map<String, String> getAttributes(final MElement element) {
            final Map<String, String> attributeMap = new HashMap<>();
            if(attributeTemplates == null || attributeTemplates.isEmpty()) {
                return attributeMap;
            }

            final Map<String, Object> values = element.asPrimitiveMap(attributeTemplateArgs);
            for(final Map.Entry<String, Template> entry : attributeTemplates.entrySet()) {
                try {
                    final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
                    if (value == null) {
                        continue;
                    }
                    attributeMap.put(entry.getKey(), value);
                } catch (final Throwable e) {
                    LOG.error("template: {}, error: {}", entry.getKey(), MFailure.convertThrowableMessage(e));
                }
            }
            return attributeMap;
        }

    }

}
