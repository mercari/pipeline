package com.mercari.solution.module.source;

import com.google.api.services.pubsub.model.SeekResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.schema.MessageSchemaUtil;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Source.Module(name="pubsub")
public class PubSubSource extends Source {

    private static class Parameters {

        private String topic;
        private String subscription;

        private String idAttribute;
        private SeekParameters seek;

        private Serialize.Format format;
        private AdditionalFieldsParameters additionalFields;
        private Boolean outputOriginal;
        private Boolean outputExcluded;
        private Boolean deserializeOriginal;
        private String charset;

        private JsonElement attributeFilter;

        // single processing
        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        // partitions processing
        private Boolean exclusive;
        private JsonArray partitions;


        private void validate(final PBegin begin, final Schema schema) {

            if(!OptionUtil.isStreaming(begin)) {
                throw new IllegalArgumentException("PubSub source module only support streaming mode.");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(topic == null && subscription == null) {
                errorMessages.add("parameters.topic or subscription is required");
            } else if(topic != null && subscription != null) {
                errorMessages.add("parameters should take one of topic or subscription");
            } else if(subscription != null) {
                if(!PubSubUtil.isSubscriptionResource(subscription)) {
                    errorMessages.add("parameters.subscription is illegal format: " + subscription);
                }
            } else {
                if(!PubSubUtil.isTopicResource(topic)) {
                    errorMessages.add("parameters.topic is illegal format: " + topic);
                }
            }
            if(format != null) {
                switch (format) {
                    case protobuf -> {
                        if(schema == null) {
                            errorMessages.add("schema is required if format is protobuf");
                        } else if(schema.getProtobuf() == null) {
                            errorMessages.add("schema.protobuf is required if format is protobuf");
                        } else if(schema.getProtobuf().getMessageName() == null || schema.getProtobuf().getDescriptorFile() == null) {
                            errorMessages.add("schema.protobuf.messageName and descriptorFile are required if format is protobuf");
                        }
                    }
                    case avro -> {
                        if(schema == null) {
                            errorMessages.add("schema is required if format is avro");
                        } else if(schema.getAvro() == null) {
                            errorMessages.add("schema.avro is required if format is avro");
                        }
                    }
                }
            }

            if(seek != null) {
                if(subscription == null) {
                    errorMessages.add("parameters.subscription is required if seek is used");
                }
                errorMessages.addAll(seek.validate());
            }
            if(additionalFields != null) {
                errorMessages.addAll(additionalFields.validate());
            }

            if(charset != null) {
                try {
                    final Charset c = Charset.forName(charset);
                } catch (Throwable e) {
                    errorMessages.add("failed to set charset: " + charset + ", cause: " + e.getMessage());
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(format == null) {
                format = Serialize.Format.message;
            }
            if(seek != null) {
                seek.setDefaults();
            }
            if(additionalFields != null) {
                additionalFields.setDefaults();
            }
            if(outputOriginal == null) {
                outputOriginal = false;
            }
            if(outputExcluded == null) {
                outputExcluded = false;
            }
            if(deserializeOriginal == null) {
                deserializeOriginal = false;
            }
            if(charset == null) {
                charset = StandardCharsets.UTF_8.name();
            }
            if(exclusive == null) {
                exclusive = true;
            }
        }

    }

    private static class SeekParameters implements Serializable {

        private String time;
        private String snapshot;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(time == null && snapshot == null) {
                errorMessages.add("parameters.seek requires time or snapshot");
            } else if(snapshot != null) {
                if(!PubSubUtil.isSnapshotResource(snapshot)) {
                    errorMessages.add("parameters.seek.snapshot is illegal: " + snapshot);
                }
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(time.equals("current_timestamp")) {
                this.time = Instant.now().toString();
            }
        }
    }

    private static class AdditionalFieldsParameters implements Serializable {

        private String topic;
        private String id;
        private String timestamp;
        private String orderingKey;
        private Map<String, String> attributes;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        private void setDefaults() {
            if(attributes == null) {
                attributes = new HashMap<>();
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(begin, getSchema());
        parameters.setDefaults();

        /*
        if(parameters.topic != null && parameters.subscription != null) {
            final Pubsub pubsub = PubSubUtil.pubsub();
            if(PubSubUtil.existsSubscription(pubsub, parameters.subscription)) {
                PubSubUtil.deleteSubscription(pubsub, parameters.subscription);
            }
            PubSubUtil.createSubscription(pubsub, parameters.topic, parameters.subscription);
        }
        */

        if(parameters.seek != null) {
            try {
                final SeekResponse seekResponse = PubSubUtil.seek(parameters.subscription, parameters.seek.time, parameters.seek.snapshot);
                LOG.info("PubSub source module {} executed seek request: {} for subscription: {}, response: {}",
                        getName(),
                        Optional.ofNullable(parameters.seek.time).orElse(parameters.seek.snapshot),
                        parameters.subscription,
                        seekResponse);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to seek subscription: " + parameters.subscription, e);
            }
        }

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.AVRO);

        final Schema inputDeserializedSchema = createDeserializedInputSchema(parameters, getSchema());

        final List<Partition> partitions;
        if(parameters.partitions == null || !parameters.partitions.isJsonArray()) {
            partitions = new ArrayList<>();
            final Partition partition = Partition.of("", parameters.filter, parameters.select, parameters.flattenField, inputDeserializedSchema);
            partitions.add(partition);
        } else {
            partitions = Partition.of(parameters.partitions, inputDeserializedSchema);
        }

        final TupleTag<MElement> originalTag = new TupleTag<>() {};
        final TupleTag<MElement> excludedTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};
        final List<TupleTag<?>> outputTags = new ArrayList<>();

        outputTags.add(excludedTag);
        outputTags.add(failuresTag);
        for(final Partition partition : partitions) {
            outputTags.add(partition.getOutputTag());
        }

        final Filter attributeFilter = Filter.of(parameters.attributeFilter);

        final Serialize serialize = Serialize.of(parameters.format, getSchema());

        final PCollectionTuple outputs = begin
                .apply("Read", createRead(parameters, getTimestampAttribute(), errorHandler))
                .apply("Format", ParDo
                        .of(new OutputDoFn(parameters, inputDeserializedSchema, outputType,
                                attributeFilter, serialize, partitions,
                                originalTag, excludedTag, failuresTag,
                                getLoggings(), getFailFast()))
                        .withOutputTags(originalTag, TupleTagList.of(outputTags)));

        errorHandler.addError(outputs.get(failuresTag));

        MCollectionTuple outputTuple;
        if(partitions.size() == 1) {
            final Partition partition = partitions.getFirst();
            final Schema outputSchema = partition
                    .getOutputSchema()
                    .copy()
                    .setup(outputType);
            final PCollection<MElement> output = outputs.get(partition.getOutputTag());
            outputTuple = MCollectionTuple.of(output, outputSchema.withType(outputType));
        } else {
            outputTuple = MCollectionTuple.empty(begin.getPipeline());
            for(final Partition partition : partitions) {
                final Schema outputSchema = partition
                        .getOutputSchema()
                        .copy()
                        .setup(outputType);
                final PCollection<MElement> output = outputs.get(partition.getOutputTag());
                outputTuple = outputTuple.and(partition.getName(), output, outputSchema.withType(outputType));
            }
        }

        if(parameters.outputOriginal) {
            final Schema originalSchema;
            if(parameters.deserializeOriginal) {
                originalSchema = inputDeserializedSchema.withType(outputType);
            } else {
                originalSchema = createMessageSchema().withType(DataType.MESSAGE);
            }
            outputTuple = outputTuple
                    .and("original", outputs.get(originalTag), originalSchema);
        }
        if(parameters.outputExcluded) {
            outputTuple = outputTuple
                    .and("excluded", outputs.get(excludedTag), inputDeserializedSchema.withType(outputType));
        }

        return outputTuple;
    }

    private static PubsubIO.Read<PubsubMessage> createRead(
            final Parameters parameters,
            final String timestampAttribute,
            final MErrorHandler errorHandler) {

        PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributesAndMessageIdAndOrderingKey();
        if (parameters.topic != null) {
            read = read.fromTopic(parameters.topic);
        } else if (parameters.subscription != null) {
            read = read.fromSubscription(parameters.subscription);
        }

        if (parameters.idAttribute != null) {
            read = read.withIdAttribute(parameters.idAttribute);
        }
        if (timestampAttribute != null) {
            read = read.withTimestampAttribute(timestampAttribute);
        }

        errorHandler.apply(read);

        return read;
    }

    private static Schema createDeserializedInputSchema(Parameters parameters, Schema schema) {
        if(Serialize.Format.message.equals(parameters.format)) {
            return createMessageSchema().withType(DataType.MESSAGE);
        }

        final Schema.Builder builder = Schema.builder(schema);
        if(parameters.additionalFields != null) {
            if(parameters.additionalFields.id != null) {
                builder.withField(parameters.additionalFields.id, Schema.FieldType.STRING);
            }
            if(parameters.additionalFields.topic != null) {
                builder.withField(parameters.additionalFields.topic, Schema.FieldType.STRING.withNullable(true));
            }
            if(parameters.additionalFields.orderingKey != null) {
                builder.withField(parameters.additionalFields.orderingKey, Schema.FieldType.STRING.withNullable(true));
            }
            if(parameters.additionalFields.timestamp != null) {
                builder.withField(parameters.additionalFields.timestamp, Schema.FieldType.TIMESTAMP);
            }
            if(parameters.additionalFields.attributes != null) {
                for(final Map.Entry<String, String> entry : parameters.additionalFields.attributes.entrySet()) {
                    builder.withField(entry.getKey(), Schema.FieldType.STRING.withNullable(true));
                }
            }
        }
        return builder.build();
    }

    private static class OutputDoFn extends DoFn<PubsubMessage, MElement> {

        private final DataType outputType;
        private final Map<String, Logging> loggings;

        private final Filter attributeFilter;
        private final Serialize serialize;
        private final List<Partition> partitions;
        private final AdditionalFieldsParameters messageFields;
        private final Schema inputSchema;

        private final boolean exclusive;
        private final boolean outputOriginal;
        private final boolean outputExcluded;
        private final boolean deserializeOriginal;
        private final boolean failFast;

        private final TupleTag<MElement> originalTag;
        private final TupleTag<MElement> excludedTag;
        private final TupleTag<BadRecord> failuresTag;

        OutputDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final DataType outputType,
                final Filter attributeFilter,
                final Serialize serialize,
                final List<Partition> partitions,
                final TupleTag<MElement> originalTag,
                final TupleTag<MElement> excludedTag,
                final TupleTag<BadRecord> failuresTag,
                final List<Logging> loggings,
                final boolean failFast) {

            this.attributeFilter = attributeFilter;
            this.serialize = serialize;
            this.partitions = partitions;
            this.messageFields = parameters.additionalFields;
            this.outputType = outputType;
            this.inputSchema = inputSchema;

            this.exclusive = parameters.exclusive;
            this.outputOriginal = parameters.outputOriginal;
            this.outputExcluded = parameters.outputExcluded;
            this.deserializeOriginal = parameters.deserializeOriginal;
            this.failFast = failFast;
            this.originalTag = originalTag;
            this.excludedTag = excludedTag;
            this.failuresTag = failuresTag;

            this.loggings = Logging.map(loggings);
        }

        @Setup
        public void setup() {
            this.attributeFilter.setup();
            this.serialize.setupDeserialize();
            for(final Partition partition : partitions) {
                partition.setup();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                return;
            }
            try {
                if(loggings.containsKey("input")) {
                    Logging.log(LOG, loggings, "input", MessageSchemaUtil.toJsonString(message));
                }

                if(outputOriginal && !deserializeOriginal) {
                    final MElement element = MElement.of(message, c.timestamp());
                    c.output(originalTag, element);
                }

                final Map attributes = message.getAttributeMap();
                if(attributes != null && !attributes.isEmpty() && !attributeFilter.filter(attributes)) {
                    return;
                }

                final Map<String, Object> additionalValues = createMessageFields(messageFields, message, c.timestamp());
                MElement deserialized = serialize
                        .deserialize(message.getPayload(), c.timestamp())
                        .withSchema(inputSchema);
                if(!additionalValues.isEmpty()) {
                    deserialized = deserialized
                            .merge(additionalValues)
                            .withSchema(inputSchema.withType(DataType.ELEMENT));
                }

                if(outputOriginal && deserializeOriginal) {
                    final MElement deserialized_ = deserialized
                            .convert(inputSchema.withType(outputType));
                    c.output(originalTag, deserialized_);
                }

                boolean outputted = false;
                for(final Partition partition : partitions) {
                    if(!partition.match(deserialized)) {
                        continue;
                    }
                    final List<MElement> outputs = partition.execute(deserialized, c.timestamp());
                    for(final MElement output : outputs) {
                        final MElement output_ = output.convert(partition.getOutputSchema().withType(outputType));
                        c.output(partition.getOutputTag(), output_);
                        if(loggings.containsKey("output")) {
                            final String text = "partition: " + partition.getName() + ", output: " + output_.toString();
                            Logging.log(LOG, loggings, "output", text);
                        }
                    }
                    outputted = true;
                    if(exclusive) {
                        return;
                    }
                }
                if(outputExcluded && !outputted) {
                    final MElement output_ = deserialized.convert(inputSchema.withType(outputType));
                    c.output(excludedTag, output_);
                    if(loggings.containsKey("output")) {
                        final String text = "excluded output: " + output_.toString();
                        Logging.log(LOG, loggings, "output", text);
                    }
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("pubsub source error: " + e.getMessage(), MElement.of(message, c.timestamp()), e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private static Map<String, Object> createMessageFields(
                final AdditionalFieldsParameters messageFields,
                final PubsubMessage message,
                final Instant timestamp) {

            final Map<String, Object> additionalValues = new HashMap<>();
            if(messageFields == null) {
                return additionalValues;
            }
            if(messageFields.topic != null) {
                additionalValues.put(messageFields.topic, message.getTopic());
            }
            if(messageFields.id != null) {
                additionalValues.put(messageFields.id, message.getMessageId());
            }
            if(messageFields.timestamp != null) {
                additionalValues.put(messageFields.timestamp, timestamp.getMillis() * 1000L);
            }
            if(messageFields.orderingKey != null) {
                additionalValues.put(messageFields.orderingKey, message.getOrderingKey());
            }
            if(!messageFields.attributes.isEmpty()) {
                for(final Map.Entry<String, String> entry : messageFields.attributes.entrySet()) {
                    additionalValues.put(entry.getValue(), message.getAttribute(entry.getKey()));
                }
            }
            return additionalValues;
        }

    }

    private static Schema createMessageSchema() {
        return Schema.builder()
                .withField("topic", Schema.FieldType.STRING)
                .withField("messageId", Schema.FieldType.STRING)
                .withField("orderingKey", Schema.FieldType.STRING.withNullable(true))
                .withField("attributes", Schema.FieldType.map(Schema.FieldType.STRING.withNullable(true)).withNullable(true))
                .withField("payload", Schema.FieldType.BYTES.withNullable(true))
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .withField("eventTime", Schema.FieldType.TIMESTAMP)
                .build();
    }

}
