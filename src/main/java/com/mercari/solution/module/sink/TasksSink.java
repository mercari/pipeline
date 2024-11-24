package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionCoder;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import freemarker.template.Template;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.SetupTeardown;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Authenticator;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class TasksSink {

    private static final Logger LOG = LoggerFactory.getLogger(TasksSink.class);


    private static class TasksSinkParameters implements Serializable {

        private String queue;
        private Format format;
        private List<String> attributes;
        private String idAttribute;
        private String timestampAttribute;
        private List<String> idAttributeFields;
        private List<String> orderingKeyFields;

        private String protobufDescriptor;
        private String protobufMessageName;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;


        public Format getFormat() {
            return format;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public String getTimestampAttribute() {
            return timestampAttribute;
        }

        public List<String> getIdAttributeFields() {
            return idAttributeFields;
        }

        public List<String> getOrderingKeyFields() {
            return orderingKeyFields;
        }

        public String getProtobufDescriptor() {
            return protobufDescriptor;
        }

        public String getProtobufMessageName() {
            return protobufMessageName;
        }

        public Integer getMaxBatchSize() {
            return maxBatchSize;
        }

        public Integer getMaxBatchBytesSize() {
            return maxBatchBytesSize;
        }


        private void validate(final String name) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if (this.queue == null) {
                errorMessages.add("tasks sink module " + name + " requires queue parameter");
            }
            if (this.format == null) {
                errorMessages.add("tasks sink module " + name + " requires format parameter");
            } else {
                if (this.getFormat().equals(Format.protobuf)) {
                    if (this.getProtobufDescriptor() == null) {
                        errorMessages.add("pubsub sink module " + name + " parameter must contain protobufDescriptor when set format `protobuf`");
                    }
                    if (this.getProtobufMessageName() == null) {
                        errorMessages.add("pubsub sink module " + name + " parameter must contain protobufMessageName when set format `protobuf`");
                    }
                }
            }
            if (errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if (this.attributes == null) {
                this.attributes = new ArrayList<>();
            }
            if (this.idAttributeFields == null) {
                this.idAttributeFields = new ArrayList<>();
            }
            if (this.orderingKeyFields == null) {
                this.orderingKeyFields = new ArrayList<>();
            } else if (this.orderingKeyFields.size() > 0 && (this.maxBatchSize == null || this.maxBatchSize != 1)) {
                LOG.warn("pubsub sink module maxBatchSize must be 1 when using orderingKeyFields. ref: https://issues.apache.org/jira/browse/BEAM-13148");
                this.maxBatchSize = 1;
            }
        }

        public static TasksSinkParameters of(final JsonObject jsonObject, final String name) {
            final TasksSinkParameters parameters = new Gson().fromJson(jsonObject, TasksSinkParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("tasks sink module parameters must not be empty!");
            }
            parameters.validate(name);
            parameters.setDefaults();
            return parameters;
        }
    }

    private enum Format {
        avro,
        json,
        protobuf
    }

    public String getName () {
        return "tasks";
    }

    public Map<String, FCollection<?>> expand (
            final List<FCollection<?>> inputs,
            final SinkConfig config,
            final List<FCollection<?>>waits){

        if (inputs == null || inputs.size() == 0) {
            throw new IllegalArgumentException("tasks sink module requires input or inputs parameter");
        }

        /*
        try {
            config.outputAvroSchema(inputs.get(0).getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

         */

        final TasksSinkParameters parameters = TasksSinkParameters.of(config.getParameters(), config.getName());

        write(inputs, config, parameters, waits);

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final FCollection<?> collection = FCollection.update(inputs.get(0), config.getName(), (PCollection) inputs.get(0).getCollection());
        outputs.put(config.getName(), collection);
        return outputs;
    }

    private static PDone write(
            final List<FCollection<?>> inputs,
            final SinkConfig config,
            final TasksSinkParameters parameters,
            final List<FCollection<?>> waits) {

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final Map<String, org.apache.beam.sdk.schemas.Schema> inputSchemas = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.put(input.getName(), input.getSchema());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final Schema inputSchema = inputs.get(0).getAvroSchema();
        final Write write = new Write(inputSchema, parameters, inputTags, inputNames, inputTypes, waits);
        return tuple.apply(config.getName(), write);
    }

    private static class Write extends PTransform<PCollectionTuple, PDone> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        private final Schema schema;
        private final TasksSinkParameters parameters;

        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final List<FCollection<?>> waitCollections;

        private Write(
                final Schema schema,
                final TasksSinkParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final List<FCollection<?>> waits) {

            this.schema = schema;
            this.parameters = parameters;
            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.waitCollections = waits;
        }

        public PDone expand(final PCollectionTuple inputs) {

            final PCollection<UnionValue> unionValues = inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames));

            final PCollection<UnionValue> waited;
            if (waitCollections == null) {
                waited = unionValues;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = unionValues
                        .apply("Wait", Wait.on(waits))
                        .setCoder(unionValues.getCoder());
            }

            final Caller<UnionValue, UnionValue> caller = new TasksCaller();
            RequestResponseIO.of(caller, unionValues.getCoder());


            /*
            return waited
                    .apply("ToMessage", ParDo.of(new PubSubSink.WriteMulti.UnionPubsubMessageDoFn(parameters, schema.toString())))
                    .apply("PublishPubSub", write);

             */
            return null;
        }

        private static class TasksCaller implements Caller<UnionValue,UnionValue>, SetupTeardown {

            private static final String METADATA_ENDPOINT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
            private static final String TASKS_TASK_ENDPOINT = "https://cloudtasks.googleapis.com/v2/parent=projects/%s/locations/%s/queues/%s/tasks/%s";

            private transient HttpClient client;
            private transient String accessToken;

            @Override
            public void setup() throws UserCodeExecutionException {

                this.client = HttpClient.newBuilder().build();
                this.accessToken = getAccessToken();
            }

            @Override
            public void teardown() throws UserCodeExecutionException {

            }

            @Override
            public UnionValue call(UnionValue request) throws UserCodeExecutionException {
                try {
                    final String endpoint = String.format(TASKS_TASK_ENDPOINT, "");
                    final HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(new URI(endpoint))
                            .header("Authorization", "Bearer " + accessToken)
                            .POST(HttpRequest.BodyPublishers.ofString(""))
                            .build();
                    final HttpResponse<String> httpResponse = this.client
                            .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
                    if(!responseJson.isJsonObject()) {
                        throw new IllegalStateException("Illegal token response: " + responseJson);
                    }
                    final JsonObject jsonObject = responseJson.getAsJsonObject();

                } catch (URISyntaxException | IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }

            private JsonObject createRequest() {
                final JsonObject request = new JsonObject();
                {
                    final JsonObject task = new JsonObject();
                    task.addProperty("name", "");
                    task.addProperty("scheduleTime", "");
                    task.addProperty("dispatchDeadline", "");
                    {
                        final JsonObject httpRequest = new JsonObject();
                        httpRequest.addProperty("url", "");
                        httpRequest.addProperty("httpMethod", "");
                        httpRequest.addProperty("headers", "");
                        httpRequest.addProperty("body", "");
                        //httpRequest.addProperty("oauthToken", "");
                        //httpRequest.addProperty("oidcToken", "");
                        task.add("httpRequest", httpRequest);
                    }
                    request.add("task", task);
                }
                return request;
            }

            private String getAccessToken() {
                try {
                    final HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(new URI(METADATA_ENDPOINT))
                            .header("Metadata-Flavor", "Google")
                            .GET()
                            .build();
                    final HttpResponse<String> httpResponse = this.client
                            .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
                    if(!responseJson.isJsonObject()) {
                        throw new IllegalStateException("Illegal token response: " + responseJson);
                    }
                    final JsonObject jsonObject = responseJson.getAsJsonObject();
                    if(!jsonObject.has("")) {
                        throw new IllegalStateException("Illegal token response: " + responseJson);
                    }

                    return jsonObject.get("").getAsString();
                } catch (URISyntaxException | IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private static class UnionPubsubMessageDoFn extends DoFn<UnionValue, PubsubMessage> {

            private final Format format;
            private final List<String> attributes;
            private final String idAttribute;
            private final List<String> idAttributeFields;
            private final List<String> orderingKeyFields;

            private final String schemaString;
            private final String descriptorPath;
            private final String messageName;

            //private final List<String> topicTemplateArgs;

            private transient Schema schema;
            private transient DatumWriter<GenericRecord> writer;
            private transient BinaryEncoder encoder;
            private transient Descriptors.Descriptor descriptor;

            private transient Template topicTemplate;


            UnionPubsubMessageDoFn(
                    final TasksSinkParameters parameters,
                    final String schemaString) {

                this.format = parameters.getFormat();
                this.attributes = parameters.getAttributes();
                this.idAttribute = parameters.getIdAttribute();
                this.idAttributeFields = parameters.getIdAttributeFields();
                this.orderingKeyFields = parameters.getOrderingKeyFields();

                this.schemaString = schemaString;
                this.descriptorPath = parameters.getProtobufDescriptor();
                this.messageName = parameters.getProtobufMessageName();

                final Schema avroSchema = AvroSchemaUtil.convertSchema(schemaString);
                //this.topicTemplateArgs = TemplateUtil.extractTemplateArgs(this.topic, avroSchema);
            }

            @Setup
            public void setup() {
                switch (format) {
                    case avro -> {
                        this.schema = AvroSchemaUtil.convertSchema(schemaString);
                        this.writer = new GenericDatumWriter<>(schema);
                        this.encoder = null;
                    }
                    case protobuf -> {
                        final byte[] bytes = StorageUtil.readBytes(descriptorPath);
                        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
                        this.descriptor = descriptors.get(messageName);
                    }
                }

            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue element = c.element();
                final Map<String, String> attributeMap = getAttributes(element);
                final String messageId = getMessageId(element);
                final String orderingKey = getOrderingKey(element);
                final byte[] payload = switch (format) {
                    case json -> {
                        final String json = UnionValue.getAsJson(element);
                        yield Optional.ofNullable(json).map(s -> s.getBytes(StandardCharsets.UTF_8)).orElse(null);
                    }
                    case avro -> {
                        final GenericRecord record = UnionValue.getAsRecord(schema, element);
                        yield encode(record);
                    }
                    case protobuf -> {
                        final DynamicMessage message = UnionValue.getAsProtoMessage(descriptor, element);
                        yield Optional.ofNullable(message).map(DynamicMessage::toByteArray).orElse(null);
                    }
                };
                if (this.idAttribute != null) {
                    attributeMap.put(this.idAttribute, messageId);
                }
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, messageId, orderingKey);
                c.output(message);
            }

            private Map<String, String> getAttributes(UnionValue element) {
                final Map<String, String> attributeMap = new HashMap<>();

                if (attributes == null || attributes.size() == 0) {
                    return attributeMap;
                }
                for (final String attribute : attributes) {
                    final String value = element.getString(attribute);
                    if (value == null) {
                        continue;
                    }
                    attributeMap.put(attribute, value);
                }
                return attributeMap;
            }

            private String getMessageId(UnionValue element) {
                if (idAttributeFields == null || idAttributeFields.isEmpty()) {
                    return null;
                }
                return getAttributesAsString(element, idAttributeFields);
            }

            private String getOrderingKey(UnionValue element) {
                if (orderingKeyFields == null || orderingKeyFields.size() == 0) {
                    return null;
                }
                return getAttributesAsString(element, orderingKeyFields);
            }

            private String getAttributesAsString(final UnionValue value, final List<String> fields) {
                final StringBuilder sb = new StringBuilder();
                for (final String fieldName : fields) {
                    final String fieldValue = value.getString(fieldName);
                    sb.append(fieldValue == null ? "" : fieldValue);
                    sb.append("#");
                }
                if (sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                return sb.toString();
            }

            byte[] encode(GenericRecord record) {
                try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
                    writer.write(record, encoder);
                    encoder.flush();
                    return byteArrayOutputStream.toByteArray();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to encode record: " + record.toString(), e);
                }
            }

        }
    }
}