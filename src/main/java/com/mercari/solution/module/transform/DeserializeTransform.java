package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import com.mercari.solution.util.schema.converter.AvroToMapConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.AvroToElementConverter;
import com.mercari.solution.util.schema.converter.ProtoToElementConverter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;


@Transform.Module(name="deserialize")
public class DeserializeTransform extends Transform {

    private static class Parameters implements Serializable {

        private Format format;
        private String field;
        private String outputField;
        private Boolean flatten;

        // avro
        private String avroFile;

        // protobuf
        private String descriptorFile;
        private String messageName;

        private JsonElement filter;
        private JsonElement select;
        private String flattenField;

        public boolean useFilter() {
            return filter != null && (filter.isJsonObject() || filter.isJsonArray());
        }

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }

        private void validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(format == null) {
                errorMessages.add("parameters.format must not be null");
            }
            if(field == null) {
                errorMessages.add("parameters.field must not be null");
            } else if(inputSchema == null) {
                errorMessages.add("requires input schema");
            } else if(!inputSchema.hasField(field)) {
                errorMessages.add("parameters.field " + field + " not found input schema: " + inputSchema);
            } else if(!Schema.Type.bytes.equals(inputSchema.getField(field).getFieldType().getType())) {
                errorMessages.add("parameters." + field + " must be bytes type. but: " + inputSchema.getField(field).getFieldType());
            } else if(format != null) {
                switch (format) {
                    case avro -> {
                        if(avroFile == null) {
                            errorMessages.add("parameters.avroFile must not be null when format is avro");
                        }
                    }
                    case protobuf -> {
                        if(descriptorFile == null) {
                            errorMessages.add("parameters.descriptorFile must not be null when format is protobuf.");
                        }
                        if(messageName == null) {
                            errorMessages.add("parameters.messageName must not be null when format is protobuf.");
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(outputField == null) {
                outputField = field;
            }
            if(flatten == null) {
                flatten = false;
            }
        }

    }

    private enum Format {
        json,
        avro,
        protobuf
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        final Schema deserializedSchema = createDeserializedSchema(parameters, inputSchema);

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<MElement> failureTag = new TupleTag<>(){};
        final PCollectionTuple tuple = input
                .apply("Deserialize", ParDo
                        .of(new DeserializeDoFn(getJobName(), getName(), deserializedSchema, parameters, getFailFast(), getOutputFailure(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        final PCollection<MElement> deserialized = tuple
                .get(outputTag)
                .setCoder(ElementCoder.of(deserializedSchema));

        final PCollection<MElement> output;
        final PCollection<MElement> failure;
        final Schema outputSchema;
        if(parameters.useSelect()) {
            final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select.getAsJsonArray(), deserializedSchema.getFields());
            outputSchema = SelectFunction.createSchema(selectFunctions, parameters.flattenField);
            final Select.Transform select = Select
                    .of(getJobName(), getName(), parameters.filter, selectFunctions, parameters.flattenField, getLoggings(), getFailFast(), DataType.ELEMENT);
            final PCollectionTuple selected = deserialized
                    .apply("Select", select);
            output = selected.get(select.outputTag);
            failure = selected.has(select.failuresTag) ? selected.get(select.failuresTag) : null;
        } else {
            if(parameters.useFilter()) {
                final Filter.Transform filter = Filter.of(getJobName(), getName(), parameters.filter, inputSchema, getLoggings(), getFailFast());
                final PCollectionTuple filtered = deserialized
                        .apply("Filter", filter);
                output = filtered.get(filter.outputTag);
                failure = filtered.has(filter.failuresTag) ? filtered.get(filter.failuresTag) : null;
            } else {
                output = deserialized;
                failure = null;
            }
            outputSchema = deserializedSchema;
        }

        return MCollectionTuple
                .of(output, outputSchema);
    }

    private static Schema createDeserializedSchema(
            final Parameters parameters,
            final Schema inputSchema) {

        final List<Schema.Field> deserializedFields = switch (parameters.format) {
            case avro -> {
                final String avroJson = StorageUtil.readString(parameters.avroFile);
                final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(avroJson);
                yield AvroToElementConverter.convertFields(avroSchema.getFields());
            }
            case protobuf -> {
                final byte[] bytes = StorageUtil.readBytes(parameters.descriptorFile);
                final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
                final Descriptors.Descriptor descriptor = descriptors.get(parameters.messageName);
                if(descriptor == null) {
                    throw new IllegalStateException("descriptor for " + parameters.messageName + " is not found in descriptors: " + descriptors.keySet());
                }
                yield ProtoToElementConverter.convertFields(descriptor);
            }
            default -> throw new RuntimeException("Not supported deserialize format: " + parameters.format);
        };

        final List<Schema.Field> fields = new ArrayList<>();
        if(parameters.flatten) {
            fields.addAll(inputSchema.getFields());
            fields.addAll(deserializedFields);
        } else {
            final Schema.FieldType deserializedFieldType = Schema.FieldType.element(deserializedFields);
            for(final Schema.Field inputField : inputSchema.getFields()) {
                if(parameters.field.equalsIgnoreCase(inputField.getName())) {
                    if(!parameters.outputField.equals(parameters.field)) {
                        fields.add(inputField);
                    }
                    fields.add(Schema.Field.of(parameters.outputField, deserializedFieldType));
                } else {
                    fields.add(inputField);
                }
            }
        }
        return Schema.of(fields);
    }

    private static class DeserializeDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String moduleName;
        private final Schema outputSchema;
        private final Format format;

        private final String avroJson;
        private final String descriptorFile;
        private final String messageName;

        private final String field;
        private final String outputField;
        private final boolean flatten;

        private final boolean failFast;
        private final boolean outputFailure;
        private final TupleTag<MElement> failuresTag;

        // for avro format
        // https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
        private transient org.apache.avro.Schema avroSchema;
        private transient DatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder decoder = null;

        // for protobuf format
        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

        DeserializeDoFn(
                final String jobName,
                final String moduleName,
                final Schema outputSchema,
                final Parameters parameters,
                final boolean failFast,
                final boolean outputFailure,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.outputSchema = outputSchema;
            this.format = parameters.format;
            this.field = parameters.field;
            this.outputField = parameters.outputField;
            this.flatten = parameters.flatten;

            if(parameters.avroFile != null) {
                this.avroJson = StorageUtil.readString(parameters.avroFile);
            } else {
                this.avroJson = null;
            }

            this.descriptorFile = parameters.descriptorFile;
            this.messageName = parameters.messageName;

            this.failFast = failFast;
            this.outputFailure = outputFailure;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            switch (format) {
                case avro -> {
                    if(this.avroJson == null) {
                        throw new IllegalArgumentException("schema must not be null");
                    }
                    this.avroSchema = AvroSchemaUtil.convertSchema(avroJson);
                    this.datumReader = new GenericDatumReader<>(avroSchema);
                }
                case protobuf -> {
                    if(this.descriptorFile == null || messageName == null) {
                        throw new IllegalArgumentException("schema must not be null");
                    }
                    getOrLoadDescriptor(descriptors, printers, messageName, descriptorFile);
                }
                case json -> {

                }
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                final Map<String, Object> values = input.asPrimitiveMap();
                final Map<String, Object> output = switch (format) {
                    case json -> parseJson(input.getAsBytes(field));
                    case avro -> parseAvro(input.getAsBytes(field));
                    case protobuf -> parseProtobuf(input.getAsBytes(field));
                    default -> throw new IllegalArgumentException();
                };
                if(flatten) {
                    values.putAll(output);
                } else {
                    values.put(outputField, output);
                }
                final MElement element = MElement.of(outputSchema, values, c.timestamp());
                c.output(element);
            } catch (final Throwable e) {
                //ERROR_COUNTER.inc();
                final MFailure failure = MFailure.of(jobName, moduleName, input.toString(), e, c.timestamp());
                final String errorMessage = "Failed to deserialize input: " + input + ", error: " + failure.getError();
                LOG.error(errorMessage);
                if(failFast) {
                    throw new RuntimeException(errorMessage, e);
                }
                if(outputFailure) {
                    c.output(failuresTag, failure.toElement(c.timestamp()));
                }
            }
        }

        private Map<String, Object> parseJson(final ByteBuffer byteBuffer) {
            final byte[] content = byteBuffer.array();
            final String json = new String(content, StandardCharsets.UTF_8);
            return JsonToMapConverter.convert(new Gson().fromJson(json, JsonElement.class));
        }

        private Map<String, Object> parseAvro(final ByteBuffer byteBuffer) throws IOException {
            final byte[] bytes = byteBuffer.array();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            final GenericRecord record = new GenericData.Record(avroSchema);
            final GenericRecord output = datumReader.read(record, decoder);
            return AvroToMapConverter.convert(output);
        }

        private Map<String, Object> parseProtobuf(final ByteBuffer byteBuffer) {
            final byte[] bytes = byteBuffer.array();
            final Descriptors.Descriptor descriptor = Optional
                    .ofNullable(descriptors.get(messageName))
                    .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorFile));
            final JsonFormat.Printer printer = printers.get(descriptorFile);
            return ProtoToElementConverter.convert(descriptor, bytes, printer);
        }

    }

    private synchronized static Descriptors.Descriptor getOrLoadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String messageName,
            final String path) {

        if(descriptors.containsKey(messageName)) {
            final Descriptors.Descriptor descriptor = descriptors.get(messageName);
            if(descriptor != null) {
                return descriptor;
            } else {
                descriptors.remove(messageName);
            }
        }
        loadDescriptors(descriptors, printers, path);
        return descriptors.get(messageName);
    }

    private synchronized static void loadDescriptors(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String descriptorPath) {

        final long start = Instant.now().toEpochMilli();
        final byte[] bytes = StorageUtil.readBytes(descriptorPath);
        final long end = Instant.now().toEpochMilli();
        LOG.info("DeserializeTransform load descriptor file took {} ms, with descriptors: {}", (end - start), bytes.length);
        final Map<String, Descriptors.Descriptor> map = ProtoSchemaUtil.getDescriptors(bytes);
        descriptors.putAll(map);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        map.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());
        printers.put(descriptorPath, printer);
    }

}
