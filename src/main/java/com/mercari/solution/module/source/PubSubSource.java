package com.mercari.solution.module.source;

import com.google.api.services.pubsub.model.SeekResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.*;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Unnest;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.MessageSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Source.Module(name="pubsub")
public class PubSubSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

    private static final Counter ERROR_COUNTER = Metrics.counter("pubsub_source", "error");;

    private static class Parameters implements Serializable {

        private String topic;
        private String subscription;

        private String idAttribute;
        private SeekParameters seek;

        private Format format;
        private AdditionalFieldsParameters additionalFields;
        private Boolean outputOriginal;
        private String charset;

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;


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
                format = Format.message;
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
            if(charset == null) {
                charset = StandardCharsets.UTF_8.name();
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

    private enum Format {
        json,
        avro,
        protobuf,
        message
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {

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

        if(hasFailures()) {
            try(final ErrorHandler.BadRecordErrorHandler<?> errorHandler = registerErrorHandler(begin)) {
                return expand(begin, parameters, errorHandler);
            }
        } else {
            return expand(begin, parameters, null);
        }
    }

    private MCollectionTuple expand(
            final PBegin begin,
            final Parameters parameters,
            final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};
        final TupleTag<MElement> originalTag = new TupleTag<>() {};

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.ELEMENT);

        final Schema inputSchema = createDeserializedInputSchema(parameters, getSchema());
        final Schema outputSchema;
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, inputSchema.getFields());
        if (selectFunctions.isEmpty()) {
            outputSchema = inputSchema
                    .copy()
                    .withType(outputType)
                    .setup(outputType);
        } else {
            outputSchema = SelectFunction
                    .createSchema(selectFunctions, parameters.flattenField)
                    .withType(outputType);
        }

        final List<TupleTag<?>> outputTags = new ArrayList<>();
        outputTags.add(failuresTag);
        if (parameters.outputOriginal) {
            outputTags.add(originalTag);
        }

        final PCollectionTuple outputs = begin
                .apply("Read", createRead(parameters, getTimestampAttribute(), errorHandler))
                .apply("Format", ParDo
                        .of(new OutputDoFn(getJobName(), getName(),
                                inputSchema, outputSchema, parameters,
                                outputType, getLoggings(), selectFunctions,
                                getFailFast(), failuresTag, originalTag))
                        .withOutputTags(outputTag, TupleTagList.of(outputTags)));

        if(errorHandler != null) {
            final PCollection<BadRecord> failures = outputs.get(failuresTag);
            errorHandler.addErrorCollection(failures);
        }

        final MCollectionTuple outputTuple = MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);

        if (parameters.outputOriginal) {
            final Schema originalSchema = createMessageSchema().withType(DataType.MESSAGE);
            return outputTuple
                    .and("original", outputs.get(originalTag), originalSchema);
        } else {
            return outputTuple;
        }
    }

    private static PubsubIO.Read<PubsubMessage> createRead(
            final Parameters parameters,
            final String timestampAttribute,
            final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {

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

        if(errorHandler != null) {
            read = read.withErrorHandler(errorHandler);
        }

        return read;
    }

    private static Schema createDeserializedInputSchema(Parameters parameters, Schema schema) {
        if(Format.message.equals(parameters.format)) {
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

        private final String jobName;
        private final String moduleName;

        private final Format format;
        private final AdditionalFieldsParameters messageFields;

        private final Map<String, Logging> loggings;
        private final DataType outputType;

        private final Filter filter;
        private final Select select;
        private final Unnest unnest;

        private final String charset;

        private final boolean failFast;
        private final boolean outputOriginal;
        private final TupleTag<BadRecord> failuresTag;
        private final TupleTag<MElement> originalTag;

        // for deserialize message
        //// for non format
        private final List<Schema.Field> fields;

        //// for avro format
        //// https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
        private final String avroSchemaJson;
        private transient GenericDatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder decoder = null;

        //// for protobuf format
        private final String descriptorFile;
        private final String messageName;
        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

        // for select result output schema
        private final Schema outputSchema;

        OutputDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema inputSchema,
                final Schema outputSchema,
                final Parameters parameters,
                final DataType outputType,
                final List<Logging> loggings,
                // select
                final List<SelectFunction> selectFunctions,
                // failures
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag,
                final TupleTag<MElement> originalTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.format = parameters.format;
            this.messageFields = parameters.additionalFields;
            this.outputType = outputType;
            this.loggings = Logging.map(loggings);

            this.filter = Filter.of(parameters.filter);
            this.select = Select.of(selectFunctions);
            this.unnest = Unnest.of(parameters.flattenField);

            this.charset = parameters.charset;

            this.failFast = failFast;
            this.outputOriginal = parameters.outputOriginal;
            this.failuresTag = failuresTag;
            this.originalTag = originalTag;

            switch (format) {
                case protobuf -> {
                    this.fields = inputSchema.getFields();
                    this.descriptorFile = inputSchema.getProtobuf().getDescriptorFile();
                    this.messageName = inputSchema.getProtobuf().getMessageName();
                    this.avroSchemaJson = null;
                }
                case avro -> {
                    this.fields = inputSchema.getFields();
                    this.descriptorFile = null;
                    this.messageName = null;
                    this.avroSchemaJson = inputSchema.getAvro().getJson();
                }
                case message -> {
                    this.fields = new ArrayList<>();
                    this.descriptorFile = null;
                    this.messageName = null;
                    this.avroSchemaJson = null;
                }
                default -> {
                    this.fields = inputSchema.getFields();
                    this.descriptorFile = null;
                    this.messageName = null;
                    this.avroSchemaJson = null;
                }
            }

            this.outputSchema = outputSchema;
        }

        @Setup
        public void setup() {
            switch (format) {
                case avro -> {
                    this.datumReader = new GenericDatumReader<>(AvroSchemaUtil.convertSchema(avroSchemaJson));
                }
                case protobuf -> {
                    long start = java.time.Instant.now().toEpochMilli();
                    final Descriptors.Descriptor descriptor = getOrLoadDescriptor(
                            descriptors, printers, messageName, descriptorFile);
                    long end = java.time.Instant.now().toEpochMilli();
                    LOG.info("Finished setup PubSub source Output DoFn {} ms, thread id: {}, with descriptor: {}",
                            (end - start),
                            Thread.currentThread().getId(),
                            descriptor.getFullName());
                }
            }
            this.outputSchema.setup(outputType);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                return;
            }
            try {
                Logging.log(LOG, loggings, "input", MessageSchemaUtil.toJsonString(message));
                if(outputOriginal) {
                    final MElement element = MElement.of(message, c.timestamp());
                    c.output(originalTag, element);
                }
                final Map attributes = message.getAttributeMap();
                if(!filter.filter(attributes)) {
                    return;
                }
                final MElement output = switch (format) {
                    case message -> MElement.of(message, c.timestamp());
                    case json -> parseJson(message, c.timestamp());
                    case avro -> parseAvro(message, c.timestamp());
                    case protobuf -> parseProtobuf(message, c.timestamp());
                };

                Logging.log(LOG, loggings, "output", output);
                c.output(output);
            } catch (final Throwable e) {
                ERROR_COUNTER.inc();
                String errorMessage = FailureUtil.convertThrowableMessage(e);
                LOG.error("pubsub source parse error: {}, {} for message: {}", e, errorMessage, message);
                if(failFast) {
                    throw new IllegalStateException(errorMessage, e);
                }
                final BadRecord badRecord = FailureUtil.createBadRecord(message, "", e);
                c.output(failuresTag, badRecord);
            }
        }

        private MElement parseJson(final PubsubMessage message, Instant timestamp) {
            final byte[] content = message.getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            if (select.useSelect()) {
                Map<String, Object> values = JsonToElementConverter.convert(fields, json);
                values = select.select(values, timestamp);
                return switch (outputType) {
                    case AVRO -> MElement.of(ElementToAvroConverter.convert(outputSchema.getAvroSchema(), values), timestamp);
                    case ROW -> MElement.of(ElementToRowConverter.convert(outputSchema.getRowSchema(), values), timestamp);
                    default -> MElement.of(values, timestamp);
                };
            } else {
                return switch (outputType) {
                    case AVRO -> MElement.of(JsonToAvroConverter.convert(outputSchema.getAvroSchema(), json), timestamp);
                    case ROW -> MElement.of(JsonToRowConverter.convert(outputSchema.getRowSchema(), json), timestamp);
                    default -> MElement.of(JsonToElementConverter.convert(fields, json), timestamp);
                };
            }
        }

        private MElement parseAvro(final PubsubMessage message, Instant timestamp) throws IOException {
            final byte[] bytes = message.getPayload();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            GenericRecord record = new GenericData.Record(datumReader.getSchema());
            record = datumReader.read(record, decoder);
            if (select.useSelect()) {
                final MElement values = MElement.of(record, timestamp);
                final Map<String, Object> output = select.select(values, timestamp);
                return switch (outputType) {
                    case AVRO -> MElement.of(addMessageFields(
                            ElementToAvroConverter.convertBuilder(outputSchema.getAvroSchema(), output),
                            messageFields
                            , message, timestamp), timestamp);
                    case ROW -> MElement.of(ElementToRowConverter.convert(outputSchema.getRowSchema(), output), timestamp);
                    default -> MElement.of(output, timestamp);
                };
            } else {
                return switch (outputType) {
                    case ROW -> MElement.of(AvroToRowConverter.convert(outputSchema.getRowSchema(), record), timestamp);
                    default -> MElement.of(record, timestamp);
                };
            }
        }

        private MElement parseProtobuf(final PubsubMessage message, Instant timestamp) {
            final byte[] bytes = message.getPayload();
            final Descriptors.Descriptor descriptor = Optional
                    .ofNullable(descriptors.get(messageName))
                    .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorFile));
            final JsonFormat.Printer printer = printers.get(messageName);

            if(select.useSelect()) {
                Map<String, Object> values = ProtoToElementConverter.convert(fields, descriptor, bytes, printer);
                values = addMessageFields(values, messageFields, message, timestamp);
                final Map<String, Object> output = select.select(values, timestamp);
                return switch (outputType) {
                    case AVRO -> MElement.of(ElementToAvroConverter.convert(outputSchema.getAvroSchema(), output), timestamp);
                    case ROW -> MElement.of(ElementToRowConverter.convert(outputSchema.getRowSchema(), output), timestamp);
                    default -> MElement.of(output, timestamp);
                };
            } else {
                return switch (outputType) {
                    case AVRO -> MElement.of(addMessageFields(
                            ProtoToAvroConverter.convertBuilder(outputSchema.getAvroSchema(), descriptor, bytes, printer),
                            messageFields, message, timestamp),timestamp);
                    case ROW -> MElement.of(ProtoToRowConverter.convert(outputSchema.getRowSchema(), descriptor, bytes, printer), timestamp);
                    default -> MElement.of(addMessageFields(
                            ProtoToElementConverter.convert(fields, descriptor, bytes, printer),
                            messageFields, message, timestamp), timestamp);
                };
            }
        }

        private static Map<String, Object> addMessageFields(
                final Map<String, Object> values,
                final AdditionalFieldsParameters messageFields,
                final PubsubMessage message,
                final Instant timestamp) {

            if(messageFields != null) {
                if(messageFields.topic != null) {
                    values.put(messageFields.topic, message.getTopic());
                }
                if(messageFields.id != null) {
                    values.put(messageFields.id, message.getMessageId());
                }
                if(messageFields.timestamp != null) {
                    values.put(messageFields.timestamp, timestamp.getMillis() * 1000L);
                }
                if(messageFields.orderingKey != null) {
                    values.put(messageFields.orderingKey, message.getOrderingKey());
                }
                if(!messageFields.attributes.isEmpty()) {
                    for(final Map.Entry<String, String> entry : messageFields.attributes.entrySet()) {
                        values.put(entry.getValue(), message.getAttribute(entry.getKey()));
                    }
                }
            }
            return values;
        }

        private static GenericRecord addMessageFields(
                final GenericRecordBuilder builder,
                final AdditionalFieldsParameters messageFields,
                final PubsubMessage message,
                final Instant timestamp) {

            if(messageFields != null) {
                if(messageFields.topic != null) {
                    builder.set(messageFields.topic, message.getTopic());
                }
                if(messageFields.id != null) {
                    builder.set(messageFields.id, message.getMessageId());
                }
                if(messageFields.timestamp != null) {
                    builder.set(messageFields.timestamp, timestamp.getMillis() * 1000L);
                }
                if(messageFields.orderingKey != null) {
                    builder.set(messageFields.orderingKey, message.getOrderingKey());
                }
                if(!messageFields.attributes.isEmpty()) {
                    for(final Map.Entry<String, String> entry : messageFields.attributes.entrySet()) {
                        builder.set(entry.getValue(), message.getAttribute(entry.getKey()));
                    }
                }
            }
            return builder.build();
        }

        private MFailure createFailureElement(
                final ProcessContext c,
                final PubsubMessage message,
                final Throwable e) {

            final JsonObject input = new JsonObject();
            input.addProperty("messageId", message.getMessageId());
            input.addProperty("orderingKey", message.getOrderingKey());
            input.addProperty("topic", message.getTopic());
            if(message.getAttributeMap() != null) {
                final JsonObject attributes = new JsonObject();
                for(final Map.Entry<String, String> entry : message.getAttributeMap().entrySet()) {
                    attributes.addProperty(entry.getKey(), entry.getValue());
                }
                input.add("attributes", attributes);
            }
            return MFailure
                    .of(jobName, moduleName, input.toString(), e, c.timestamp());
        }

        private String createInputJson(
                final ProcessContext c,
                final PubsubMessage message,
                final Throwable e) {


            final JsonObject input = new JsonObject();
            input.addProperty("messageId", message.getMessageId());
            input.addProperty("orderingKey", message.getOrderingKey());
            input.addProperty("topic", message.getTopic());
            if(message.getAttributeMap() != null) {
                final JsonObject attributes = new JsonObject();
                for(final Map.Entry<String, String> entry : message.getAttributeMap().entrySet()) {
                    attributes.addProperty(entry.getKey(), entry.getValue());
                }
                input.add("attributes", attributes);
            }
            return input.toString();
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

    private synchronized static Descriptors.Descriptor getOrLoadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String messageName,
            final String descriptorPath) {

        if(descriptors.containsKey(messageName)) {
            final Descriptors.Descriptor descriptor = descriptors.get(messageName);
            if(descriptor != null) {
                return descriptor;
            } else {
                descriptors.remove(messageName);
            }
        }
        loadDescriptor(descriptors, printers, messageName, descriptorPath);
        return descriptors.get(messageName);
    }

    private synchronized static void loadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String messageName,
            final String descriptorPath) {

        if(descriptors.containsKey(messageName) && descriptors.get(messageName) == null) {
            descriptors.remove(messageName);
        }

        if(!descriptors.containsKey(messageName)) {
            final byte[] bytes = StorageUtil.readBytes(descriptorPath);
            final Map<String, Descriptors.Descriptor> map = ProtoSchemaUtil.getDescriptors(bytes);
            if(!map.containsKey(messageName)) {
                throw new IllegalArgumentException();
            }

            descriptors.put(messageName, map.get(messageName));

            final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
            map.forEach((k, v) -> builder.add(v));
            final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());
            printers.put(messageName, printer);

            LOG.info("setup pubsub source module. protoMessage: {} loaded", messageName);
        }
    }

}
