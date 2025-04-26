package com.mercari.solution.module.sink;

import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import com.mercari.solution.util.schema.converter.ElementToProtoConverter;
import freemarker.template.Template;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Sink.Module(name="pubsub")
public class PubSubSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

    private static final String ATTRIBUTE_NAME_ID = "__id";
    private static final String ATTRIBUTE_NAME_SOURCE = "__source";
    private static final String ATTRIBUTE_NAME_EVENT_TIME = "__timestamp";
    private static final String ATTRIBUTE_NAME_TOPIC = "__topic";

    private static class Parameters implements Serializable {

        private String topic;
        private Format format;
        private Map<String, String> attributes;
        private String idAttribute;
        private String timestampAttribute;
        private List<String> idAttributeFields;
        private List<String> orderingKeyFields;
        private String payloadField;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;

        private Boolean useDestinationSchema;

        private void validate(final Schema schema) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.topic == null) {
                errorMessages.add("parameters.topic must not be null");
            }
            if(this.format == null) {
                errorMessages.add("parameters.format must not be null");
            } else {
                switch (format) {
                    case protobuf -> {
                        if(schema.getProtobuf() == null) {
                            errorMessages.add("parameters.protobufDescriptor must not be null when set format `protobuf`");
                        }
                        if(schema.getProtobuf().getDescriptorFile() == null) {
                            errorMessages.add("parameters.protobufMessageName must not be null when set format `protobuf`");
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(this.attributes == null) {
                this.attributes = new HashMap<>();
            }
            if(this.idAttributeFields == null) {
                this.idAttributeFields = new ArrayList<>();
            }
            if(this.orderingKeyFields == null) {
                this.orderingKeyFields = new ArrayList<>();
            } else if(!this.orderingKeyFields.isEmpty() && (this.maxBatchSize == null || this.maxBatchSize != 1)) {
                LOG.warn("pubsub sink module maxBatchSize must be 1 when using orderingKeyFields. ref: https://issues.apache.org/jira/browse/BEAM-13148");
                this.maxBatchSize = 1;
            }
            if(useDestinationSchema == null) {
                useDestinationSchema = false;
            }
        }
    }

    private enum Format {
        message,
        avro,
        json,
        protobuf
    }

    /*
    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        if(hasFailures()) {
            try(final ErrorHandler.BadRecordErrorHandler<?> errorHandler = registerErrorHandler(inputs)) {
                return expand(inputs, errorHandler);
            }
        } else {
            return expand(inputs, null);
        }
    }

     */

    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema;
        if(getSchema() != null) {
            outputSchema = getSchema();
        } else if(Optional.ofNullable(parameters.useDestinationSchema).orElse(false)) {
            final org.apache.avro.Schema topicSchema = PubSubUtil.getSchemaFromTopic(parameters.topic);
            outputSchema = Schema.of(topicSchema);
        } else {
            outputSchema = inputSchema;
        }

        parameters.validate(outputSchema);
        parameters.setDefaults();

        if(getUnion().each) {
            for(final Map.Entry<String, PCollection<MElement>> entry : inputs.getAll().entrySet()) {
                final String inputName = entry.getKey();
                final List<String> inputNames = List.of(inputName);
                final PCollection<MElement> input = entry.getValue();

                final TupleTag<PubsubMessage> outputTag = new TupleTag<>(){};
                final TupleTag<BadRecord> failureTag = new TupleTag<>(){};

                final PCollectionTuple outputs = input
                        .apply("ToMessage_" + inputName, ParDo
                                .of(new OutputDoFn(
                                        parameters, inputSchema, outputSchema, inputNames, getFailFast(), failureTag))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
                final PubsubIO.Write<PubsubMessage> write = createWrite(parameters, errorHandler);
                outputs.get(outputTag).apply("Publish_" + inputName, write);

                errorHandler.addError(outputs.get(failureTag));
            }

            return MCollectionTuple
                    .done(PDone.in(inputs.getPipeline()));
        } else {
            final List<String> inputNames = inputs.getAllInputs();
            final PCollection<MElement> input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));

            final TupleTag<PubsubMessage> outputTag = new TupleTag<>(){};
            final TupleTag<BadRecord> failureTag = new TupleTag<>(){};

            final PCollectionTuple outputs = input
                    .apply("ToMessage", ParDo
                            .of(new OutputDoFn(parameters, inputSchema, outputSchema, inputNames, getFailFast(), failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));

            final PDone done = outputs.get(outputTag)
                    .apply("Publish", createWrite(parameters, errorHandler));

            errorHandler.addError(outputs.get(failureTag));

            return MCollectionTuple
                    .done(done);
        }
    }

    private static class OutputDoFn extends DoFn<MElement, PubsubMessage> {

        // for protobuf
        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();// Collections.synchronizedMap(new HashMap<>());
        private static final Map<String, org.apache.avro.Schema> avroSchemas = new HashMap<>();
        private static final Map<String, DatumWriter<GenericRecord>> writers = new HashMap<>();

        private final Parameters parameters;
        private final Schema outputSchema;
        private final List<String> inputNames;

        private final boolean isDynamicTopic;
        private final List<String> topicTemplateArgs;
        private final List<String> attributeTemplateArgs;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        // for avro
        private transient DatumWriter<GenericRecord> writer;
        private transient BinaryEncoder encoder;

        private transient Template topicTemplate;
        private transient Map<String, Template> attributeTemplates;

        OutputDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.parameters = parameters;
            this.outputSchema = outputSchema;
            this.inputNames = inputNames;

            this.isDynamicTopic = TemplateUtil.isTemplateText(parameters.topic);
            this.topicTemplateArgs = TemplateUtil.extractTemplateArgs(parameters.topic, inputSchema);
            this.attributeTemplateArgs = new ArrayList<>();
            for(final Map.Entry<String, String> entry : parameters.attributes.entrySet()) {
                this.attributeTemplateArgs.addAll(
                        TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema));
            }

            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            outputSchema.setup();
            if(isDynamicTopic) {
                this.topicTemplate = TemplateUtil.createStrictTemplate("topicTemplate", parameters.topic);
            }
            this.attributeTemplates = new HashMap<>();
            for(Map.Entry<String, String> entry : parameters.attributes.entrySet()) {
                final String templateName = "pubsubSinkAttributeTemplate" + entry.getKey();
                final Template template = TemplateUtil.createStrictTemplate(templateName, entry.getValue());
                this.attributeTemplates.put(entry.getKey(), template);
            }
            switch (parameters.format) {
                case avro -> {
                    if(this.outputSchema.getAvro() == null) {
                        throw new IllegalArgumentException("schema.avro must not be null");
                    }
                    LOG.warn("outputSchema: " + this.outputSchema.getAvroSchema().toString());
                    this.writer = new GenericDatumWriter<>(this.outputSchema.getAvroSchema());
                    /*
                    long start = java.time.Instant.now().toEpochMilli();
                    final org.apache.avro.Schema avroSchema = getOrLoadAvroSchema(avroSchemas, writers, outputSchema.getAvro());
                    long end = java.time.Instant.now().toEpochMilli();
                    LOG.info("Finished setup PubSub sink Output DoFn took {} ms with avro schema: {}", (end - start), avroSchema);

                     */
                }
                case protobuf -> {
                    if(this.outputSchema.getProtobuf() == null) {
                        throw new IllegalArgumentException("schema.protobuf must not be null");
                    }
                    long start = java.time.Instant.now().toEpochMilli();
                    final Descriptors.Descriptor descriptor = getOrLoadDescriptor(descriptors, outputSchema.getProtobuf());
                    long end = java.time.Instant.now().toEpochMilli();
                    LOG.info("Finished setup PubSub sink Output DoFn took {} ms with descriptor: {}", (end - start), descriptor.getFullName());
                }
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if (input == null) {
                return;
            }

            try {
                final Map<String, String> attributeMap = getAttributes(input);
                if(DataType.MESSAGE.equals(input.getType()) && input.getValue() instanceof PubsubMessage) {
                    final PubsubMessage original = (PubsubMessage) input.getValue();
                    if(parameters.attributes.isEmpty()) {
                        c.output(original);
                    } else {
                        final PubsubMessage message = new PubsubMessage(
                                original.getPayload(), attributeMap, original.getMessageId(), original.getOrderingKey());
                        c.output(message);
                    }
                    return;
                }

                final String messageId = getMessageId(input);
                final String orderingKey = getOrderingKey(input);
                final byte[] payload = switch (parameters.format) {
                    case json -> {
                        final JsonObject json = ElementToJsonConverter.convert(outputSchema, input.asPrimitiveMap());
                        yield json.toString().getBytes(StandardCharsets.UTF_8);
                    }
                    case avro -> {
                        final org.apache.avro.Schema avroSchema = Optional
                                .ofNullable(avroSchemas.get(outputSchema.getAvro().getFile()))
                                .orElseGet(() -> getOrLoadAvroSchema(avroSchemas, writers, outputSchema.getAvro()));
                        final GenericRecord record = ElementToAvroConverter.convert(avroSchema, input);
                        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                            encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
                            writer.write(record, encoder);
                            encoder.flush();
                            yield byteArrayOutputStream.toByteArray();
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to encode record: " + record.toString(), e);
                        }
                    }
                    case protobuf -> {
                        final Descriptors.Descriptor descriptor = Optional
                                .ofNullable(descriptors.get(outputSchema.getProtobuf().getMessageName()))
                                .orElseGet(() -> getOrLoadDescriptor(descriptors, outputSchema.getProtobuf()));
                        final DynamicMessage message = ElementToProtoConverter.convert(outputSchema, descriptor, input.asPrimitiveMap());
                        yield Optional
                                .ofNullable(message)
                                .map(DynamicMessage::toByteArray)
                                .orElse(null);
                    }
                    case message -> input.getAsBytes(parameters.payloadField).array();
                    default -> throw new IllegalArgumentException("Not supported pubsub sink format: " + parameters.format);
                };
                if (parameters.idAttribute != null) {
                    attributeMap.put(parameters.idAttribute, messageId);
                }
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, messageId, orderingKey);
                c.output(message);
            } catch (final Throwable e) {
                final String source = inputNames.get(input.getIndex());
                switch (input.getType()) {
                    case MESSAGE -> {
                        final PubsubMessage original = (PubsubMessage) input.getValue();
                        final Map<String, Object> json = new HashMap<>();
                        if(original != null) {
                            json.put("id", original.getMessageId());
                            json.put("source", source);
                            if(original.getAttributeMap() != null) {
                                json.put("attributes", original.getAttributeMap());
                            }
                        }
                        final BadRecord badRecord = processError("Failed to create pubsub message from input: " + source, json, e, failFast);
                        c.output(failureTag, badRecord);
                    }
                    default -> {
                        final BadRecord badRecord = processError("Failed to create pubsub message from input: " + source, input, e, failFast);
                        c.output(failureTag, badRecord);
                    }
                }

            }
        }

        private Map<String, String> getAttributes(MElement element) {
            final Map<String, String> attributeMap = new HashMap<>();
            if(element == null) {
                return attributeMap;
            }
            if(isDynamicTopic) {
                // When using dynamicTopic, insert the dynamically generated topic name of the destination into the attribute and refer to it when publishing.
                final String topic = TemplateUtil.executeStrictTemplate(topicTemplate, element.asPrimitiveMap(topicTemplateArgs));
                attributeMap.put(ATTRIBUTE_NAME_TOPIC, topic);
            }
            if(parameters.attributes == null || parameters.attributes.isEmpty()) {
                return attributeMap;
            }

            final Map<String, Object> values = element.asPrimitiveMap(attributeTemplateArgs);
            if(DataType.MESSAGE.equals(element.getType())) {
                final PubsubMessage message = (PubsubMessage) element.getValue();
                if(message.getAttributeMap() != null) {
                    attributeMap.putAll(message.getAttributeMap());
                }
                values.put(ATTRIBUTE_NAME_ID, Optional.ofNullable(message.getMessageId()).orElse(""));
            } else {
                values.put(ATTRIBUTE_NAME_ID, "");
            }
            values.put(ATTRIBUTE_NAME_EVENT_TIME, element.getEpochMillis());
            values.put(ATTRIBUTE_NAME_SOURCE, inputNames.get(element.getIndex()));
            TemplateUtil.setFunctions(values);
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

        private String getMessageId(MElement element) {
            if(DataType.MESSAGE.equals(element.getType())) {
                final PubsubMessage message = (PubsubMessage) element.getValue();
                return message.getMessageId();
            }
            if(parameters.idAttributeFields == null || parameters.idAttributeFields.isEmpty()) {
                return null;
            }
            return getAttributesAsString(element, parameters.idAttributeFields);
        }

        private String getOrderingKey(MElement element) {
            if(DataType.MESSAGE.equals(element.getType())) {
                final PubsubMessage message = (PubsubMessage) element.getValue();
                return message.getOrderingKey();
            }
            if(parameters.orderingKeyFields == null || parameters.orderingKeyFields.isEmpty()) {
                return null;
            }
            return getAttributesAsString(element, parameters.orderingKeyFields);
        }

        private String getAttributesAsString(final MElement value, final List<String> fields) {
            final StringBuilder sb = new StringBuilder();
            for(final String fieldName : fields) {
                final String fieldValue = value.getAsString(fieldName);
                sb.append(fieldValue == null ? "" : fieldValue);
                sb.append("#");
            }
            if(!sb.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }

    }

    private static PubsubIO.Write<PubsubMessage> createWrite(
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        PubsubIO.Write<PubsubMessage> write;
        if(TemplateUtil.isTemplateText(parameters.topic)) {
            write = PubsubIO.writeMessagesDynamic().to(topicFunction);
        } else {
            write = PubsubIO.writeMessages().to(parameters.topic);
        }

        if (parameters.idAttribute != null && parameters.idAttributeFields.isEmpty()) {
            write = write.withIdAttribute(parameters.idAttribute);
        }
        if (parameters.timestampAttribute != null) {
            write = write.withTimestampAttribute(parameters.timestampAttribute);
        }

        if (parameters.maxBatchSize != null) {
            write = write.withMaxBatchSize(parameters.maxBatchSize);
        }
        if (parameters.maxBatchBytesSize != null) {
            write = write.withMaxBatchBytesSize(parameters.maxBatchBytesSize);
        }

        errorHandler.apply(write);

        return write;
    }

    private static final SerializableFunction<ValueInSingleWindow<PubsubMessage>,String> topicFunction = (ValueInSingleWindow<PubsubMessage> m) -> {
        if(m == null || m.getValue() == null) {
            throw new IllegalArgumentException("pubsub input message must not be null");
        }
        if(m.getValue().getAttributeMap() == null || !m.getValue().getAttributeMap().containsKey(ATTRIBUTE_NAME_TOPIC)) {
            throw new IllegalStateException("pubsub destination topic is null");
        }
        return m.getValue().getAttributeMap().get(ATTRIBUTE_NAME_TOPIC);
    };

    private synchronized static org.apache.avro.Schema getOrLoadAvroSchema(
            final Map<String, org.apache.avro.Schema> avroSchemas,
            final Map<String, DatumWriter<GenericRecord>> writers,
            final Schema.AvroSchema avroSchema) {

        if(avroSchema.getSchema() != null) {
            return avroSchema.getSchema();
        }

        if(avroSchemas.containsKey(avroSchema.getFile())) {
            final org.apache.avro.Schema schema = avroSchemas.get(avroSchema.getFile());
            if(schema != null) {
                return schema;
            } else {
                avroSchemas.remove(avroSchema.getFile());
                writers.remove(avroSchema.getFile());
            }
        }
        if(avroSchema.getJson() != null) {
            final org.apache.avro.Schema schema = AvroSchemaUtil.convertSchema(avroSchema.getJson());
            avroSchemas.put(avroSchema.getFile(), schema);
            writers.put(avroSchema.getFile(), new GenericDatumWriter<>(schema));
            return schema;
        }

        final String schemaJson = StorageUtil.readString(avroSchema.getFile());
        final org.apache.avro.Schema schema = AvroSchemaUtil.convertSchema(schemaJson);
        avroSchemas.put(avroSchema.getFile(), schema);
        writers.put(avroSchema.getFile(), new GenericDatumWriter<>(schema));
        return schema;
    }

    private synchronized static Descriptors.Descriptor getOrLoadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Schema.ProtobufSchema protobufSchema) {

        LOG.info("PubSub sink call getOrLoadDescriptor");
        if(descriptors.containsKey(protobufSchema.getMessageName())) {
            final Descriptors.Descriptor descriptor = descriptors.get(protobufSchema.getMessageName());
            if(descriptor != null) {
                return descriptor;
            } else {
                descriptors.remove(protobufSchema.getMessageName());
            }
        }
        loadDescriptor(descriptors, protobufSchema);
        return descriptors.get(protobufSchema.getMessageName());
    }

    private synchronized static void loadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            Schema.ProtobufSchema protobufSchema) {

        if(descriptors.containsKey(protobufSchema.getMessageName()) && descriptors.get(protobufSchema.getMessageName()) == null) {
            descriptors.remove(protobufSchema.getMessageName());
        }

        if(!descriptors.containsKey(protobufSchema.getMessageName())) {
            final byte[] bytes = StorageUtil.readBytes(protobufSchema.getDescriptorFile());
            final Map<String, Descriptors.Descriptor> map = ProtoSchemaUtil.getDescriptors(bytes);
            if(!map.containsKey(protobufSchema.getMessageName())) {
                throw new IllegalArgumentException();
            }

            descriptors.put(protobufSchema.getMessageName(), map.get(protobufSchema.getMessageName()));

            LOG.info("PubSub sink loaded protoMessage: {} loaded", protobufSchema.getMessageName());
        } else {
            LOG.info("PubSub sink skipped to load protoMessage: {}", protobufSchema.getMessageName());
        }
    }
}
