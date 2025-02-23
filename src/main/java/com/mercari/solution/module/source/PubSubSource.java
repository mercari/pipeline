package com.mercari.solution.module.source;

import com.google.api.services.pubsub.model.SeekResponse;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.sql.calcite.MemorySchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Planner;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Source.Module(name="pubsub")
public class PubSubSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

    private static final Counter ERROR_COUNTER = Metrics.counter("pubsub_source", "error");;

    private static class PubSubSourceParameters implements Serializable {

        private String topic;
        private String subscription;

        private String idAttribute;
        private SeekParameters seek;

        private Format format;
        private AdditionalFieldsParameters additionalFields;

        private List<PartitionParameters> partitions;

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
            if(partitions != null) {
                for(int i=0; i<partitions.size(); i++) {
                    errorMessages.addAll(partitions.get(i).validate(i));
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
            if(partitions == null) {
                partitions = new ArrayList<>();
            } else {
                for(final PartitionParameters partition : partitions) {
                    partition.setDefaults();
                }
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

    private static class PartitionParameters implements Serializable {

        private String name;
        private JsonElement filter;
        private JsonElement schema;
        private Format format;
        private AdditionalFieldsParameters additionalFields;
        private JsonElement select;
        private String sql;

        private List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("parameters.partition[" + index + "].name must not be null");
            }
            if(filter == null || filter.isJsonNull()) {
                errorMessages.add("parameters.partition[" + index + "].filter must not be null");
            } else if(filter.isJsonPrimitive()) {
                errorMessages.add("parameters.partition[" + index + "].filter is illegal format: " + filter);
            }

            if(schema == null || schema.isJsonNull()) {
                if(!Format.json.equals(format)) {
                    errorMessages.add("parameters.partition[" + index + "].schema must not be null");
                }
            } else if(schema.isJsonPrimitive()) {
                errorMessages.add("parameters.partition[" + index + "].schema is illegal format: " + schema);
            }

            if(additionalFields != null) {
                errorMessages.addAll(additionalFields.validate());
            }

            return errorMessages;
        }

        private void setDefaults() {

        }

    }

    private static class Partition implements Serializable {

        private String filterString;
        private Schema inputSchema;
        private Format format;
        private AdditionalFieldsParameters messageFields;
        private String sql;

        private transient Filter.ConditionNode filter;

        private transient DatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder decoder = null;

        private transient List<MElement> elements;
        private transient Planner planner;
        private transient PreparedStatement statement;


        Partition(final PartitionParameters partitionParameters) {
            this.filterString = partitionParameters.filter.toString();
            this.inputSchema = Schema.parse(partitionParameters.schema);
            this.format = partitionParameters.format;
            this.messageFields = partitionParameters.additionalFields;
        }

        public void setup(
                final Map<String, Descriptors.Descriptor> descriptors,
                final Map<String, JsonFormat.Printer> printers) {

            this.filter = Filter.parse(filterString);
            switch (format) {
                case avro -> {
                    this.inputSchema.setup(DataType.AVRO);
                    this.datumReader = new GenericDatumReader<>(inputSchema.getAvroSchema());
                }
                case protobuf -> {
                    final Descriptors.Descriptor descriptor = getOrLoadDescriptor(
                            descriptors,
                            printers,
                            inputSchema.getProtobuf().getMessageName(),
                            inputSchema.getProtobuf().getDescriptorFile());
                }
                default -> {
                    this.inputSchema.setup();
                }
            }

            this.elements = new ArrayList<>();
            if(sql != null) {
                final MemorySchema memorySchema = MemorySchema.create("schema", List.of(
                        MemorySchema.createTable("INPUT", inputSchema, elements)
                ));
                //this.planner = Query.createPlanner(memorySchema);
                try {
                    //this.statement = Query.createStatement(planner, sql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public boolean filter(final Map<String, ?> standardValues) {
            return Filter.filter(filter, standardValues);
        }

        public List<MElement> process(final MElement element, final Instant timestamp) {
            if(sql == null) {
                return List.of(element);
            }

            this.elements.clear();
            this.elements.add(element);
            try(final ResultSet resultSet = statement.executeQuery()) {
                final List<MElement> outputs = new ArrayList<>();
                final List<Map<String, Object>> results = CalciteSchemaUtil.convert(resultSet);
                for(final Map<String, Object> result : results) {
                    final MElement output = MElement.of(result, timestamp);
                    outputs.add(output);
                }
                return outputs;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private enum Format {
        avro,
        json,
        protobuf,
        message
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {

        final PubSubSourceParameters parameters = getParameters(PubSubSourceParameters.class);
        parameters.validate(begin, getSchema());
        parameters.setDefaults();

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

        final PubsubIO.Read<PubsubMessage> read = createRead(parameters, getTimestampAttribute());
        final PCollection<PubsubMessage> pubsubMessages = begin.apply("Read", read);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<MElement> failuresTag = new TupleTag<>() {};
        if(parameters.partitions.isEmpty()) {
            final Schema outputSchema = createOutputSchema(parameters, getSchema());
            if(!getOutputFailure() || getFailFast()) {
                final PCollection<MElement> output = pubsubMessages
                        .apply("Format", ParDo
                                .of(new OutputDoFn(getJobName(), getName(), outputSchema, parameters.format, parameters.additionalFields,
                                        getFailFast(), getOutputFailure(), failuresTag)));
                return MCollectionTuple
                        .of(output, outputSchema);
            } else {
                final PCollectionTuple outputs = pubsubMessages
                        .apply("Format", ParDo
                                .of(new OutputDoFn(getJobName(), getName(), outputSchema, parameters.format, parameters.additionalFields,
                                        getFailFast(), getOutputFailure(), failuresTag))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

                return MCollectionTuple
                        .of(outputs.get(outputTag), outputSchema)
                        .failure(outputs.get(failuresTag));
            }
        } else {
            final List<Schema> inputSchemas = new ArrayList<>();
            for(final PartitionParameters partition : parameters.partitions) {
                final Schema schema = Schema.parse(partition.schema);
                inputSchemas.add(schema);
            }

            final PCollectionTuple outputs = pubsubMessages
                    .apply("PartitionFormat", ParDo
                            .of(new PartitionOutputDoFn(getJobName(), getName(), parameters.partitions,
                                    getFailFast(), failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            final Schema outputSchema = Union.createUnionSchema(inputSchemas);

            return MCollectionTuple
                    .of(outputs.get(outputTag), outputSchema)
                    .failure(outputs.get(failuresTag));
        }

    }

    private static PubsubIO.Read<PubsubMessage> createRead(
            final PubSubSourceParameters parameters,
            final String timestampAttribute) {
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
        return read;
    }

    private static Schema createOutputSchema(PubSubSourceParameters parameters, Schema schema) {
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

        private final Schema schema;
        private final Format format;
        private final AdditionalFieldsParameters messageFields;
        private final boolean failFast;
        private final boolean outputFailure;
        private final TupleTag<MElement> failuresTag;

        // for avro format
        // https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
        private transient DatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder decoder = null;

        // for protobuf format
        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

        OutputDoFn(
                final String jobName,
                final String moduleName,
                final Schema schema,
                final Format format,
                final AdditionalFieldsParameters messageFields,
                final boolean failFast,
                final boolean outputFailure,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.schema = schema;
            this.format = format;
            this.messageFields = messageFields;
            this.failFast = failFast;
            this.outputFailure = outputFailure;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            if(this.schema != null) {
                this.schema.setup();
            }

            switch (format) {
                case avro -> {
                    if(this.schema == null) {
                        throw new IllegalArgumentException("schema must not be null");
                    }
                    this.datumReader = new GenericDatumReader<>(schema.getAvro().getSchema());
                }
                case protobuf -> {
                    if(this.schema == null) {
                        throw new IllegalArgumentException("schema must not be null");
                    }
                    LOG.info("Start setup PubSub source Output DoFn thread id: {}", Thread.currentThread().getId());
                    long start = java.time.Instant.now().toEpochMilli();
                    final Descriptors.Descriptor descriptor = getOrLoadDescriptor(descriptors, printers, schema.getProtobuf().getMessageName(), schema.getProtobuf().getDescriptorFile());
                    long end = java.time.Instant.now().toEpochMilli();
                    LOG.info("Finished setup PubSub source Output DoFn {} ms, thread id: {}, with descriptor: {}", (end - start), Thread.currentThread().getId(), descriptor.getFullName());
                }
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                return;
            }
            try {
                final MElement element = switch (format) {
                    case message -> MElement.of(message, c.timestamp());
                    case json -> parseJson(message, c.timestamp());
                    case avro -> parseAvro(message, c.timestamp());
                    case protobuf -> parseProtobuf(message, c.timestamp());
                };
                c.output(element);
            } catch (final Throwable e) {
                ERROR_COUNTER.inc();
                final MFailure failureElement = createFailureElement(c, message, e);
                String errorMessage = MFailure.convertThrowableMessage(e);
                LOG.error("pubsub source parse error: {}, {} for message: {}", e, errorMessage, message);
                if(failFast) {
                    throw new IllegalStateException(errorMessage, e);
                }
                if(outputFailure) {
                    c.output(failuresTag, failureElement.toElement(c.timestamp()));
                }
            }
        }

        private MElement parseJson(final PubsubMessage message, Instant timestamp) {
            final byte[] content = message.getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            final Map<String, Object> map = JsonToMapConverter.convert(new Gson().fromJson(json, JsonElement.class));
            return MElement.of(map, timestamp);
        }

        private MElement parseAvro(final PubsubMessage message, Instant timestamp) throws IOException {
            final byte[] bytes = message.getPayload();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            final GenericRecord record = new GenericData.Record(schema.getAvro().getSchema());
            final GenericRecord output = datumReader.read(record, decoder);
            return MElement.of(output, timestamp);
        }

        private MElement parseProtobuf(final PubsubMessage message, Instant timestamp) {
            final byte[] bytes = message.getPayload();
            final String messageName = schema.getProtobuf().getMessageName();
            final Descriptors.Descriptor descriptor = Optional
                    .ofNullable(descriptors.get(schema.getProtobuf().getMessageName()))
                    .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, schema.getProtobuf().getDescriptorFile()));
            final JsonFormat.Printer printer = printers.get(messageName);

            final Map<String, Object> values = ProtoToElementConverter.convert(schema, descriptor, bytes, printer);

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

            return MElement.of(values, timestamp.getMillis());
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

    }

    private static class PartitionOutputDoFn extends DoFn<PubsubMessage, MElement> {

        private final String jobName;
        private final String moduleName;

        private List<Partition> partitions;

        private final boolean failFast;
        private final TupleTag<MElement> failuresTag;


        // for protobuf format
        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();


        PartitionOutputDoFn(
                final String jobName,
                final String moduleName,
                final List<PartitionParameters> partitionParameters,
                final boolean failFast,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.partitions = new ArrayList<>();
            for(final PartitionParameters partitionParameter : partitionParameters) {
                this.partitions.add(new Partition(partitionParameter));
            }

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            for(final Partition partition : partitions) {
                partition.setup(descriptors, printers);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                return;
            }

            for(final Partition partition : partitions) {
                try {
                    if(!partition.filter(message.getAttributeMap())) {
                        continue;
                    }
                    final MElement element = switch (partition.format) {
                        case message -> MElement.of(message, c.timestamp());
                        case json -> parseJson(partition, message, c.timestamp());
                        case avro -> parseAvro(partition, message, c.timestamp());
                        case protobuf -> parseProtobuf(partition, message, c.timestamp());
                    };

                    final List<MElement> outputs = partition.process(element, c.timestamp());
                    for(final MElement output : outputs) {
                        c.output(output);
                    }
                    return;
                } catch (final Throwable e) {

                }
            }
        }

        private MElement parseJson(
                final Partition partition,
                final PubsubMessage message,
                final Instant timestamp) {

            final byte[] content = message.getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            final Map<String, Object> map;
            if(partition.inputSchema != null) {
                map = JsonToElementConverter.convert(partition.inputSchema, json);
            } else {
                map = JsonToMapConverter.convert(new Gson().fromJson(json, JsonElement.class));
            }

            return MElement.of(map, timestamp);
        }

        private MElement parseAvro(
                final Partition partition,
                final PubsubMessage message,
                final Instant timestamp) throws IOException {

            final byte[] bytes = message.getPayload();
            partition.decoder = DecoderFactory.get().binaryDecoder(bytes, partition.decoder);
            final GenericRecord record = new GenericData.Record(partition.inputSchema.getAvroSchema());
            final GenericRecord output = partition.datumReader.read(record, partition.decoder);
            return MElement.of(output, timestamp);
        }

        private MElement parseProtobuf(
                final Partition partition,
                final PubsubMessage message,
                final Instant timestamp) {

            final byte[] bytes = message.getPayload();
            final String messageName = partition.inputSchema.getProtobuf().getMessageName();
            final Descriptors.Descriptor descriptor = Optional
                    .ofNullable(descriptors.get(messageName))
                    .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, partition.inputSchema.getProtobuf().getDescriptorFile()));
            final JsonFormat.Printer printer = printers.get(messageName);

            final Map<String, Object> values = ProtoToElementConverter.convert(partition.inputSchema, descriptor, bytes, printer);

            if(partition.messageFields != null) {
                if(partition.messageFields.topic != null) {
                    values.put(partition.messageFields.topic, message.getTopic());
                }
                if(partition.messageFields.id != null) {
                    values.put(partition.messageFields.id, message.getMessageId());
                }
                if(partition.messageFields.timestamp != null) {
                    values.put(partition.messageFields.timestamp, timestamp.getMillis() * 1000L);
                }
                if(partition.messageFields.orderingKey != null) {
                    values.put(partition.messageFields.orderingKey, message.getOrderingKey());
                }
                if(!partition.messageFields.attributes.isEmpty()) {
                    for(final Map.Entry<String, String> entry : partition.messageFields.attributes.entrySet()) {
                        values.put(entry.getValue(), message.getAttribute(entry.getKey()));
                    }
                }
            }

            return MElement.of(values, timestamp.getMillis());
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
