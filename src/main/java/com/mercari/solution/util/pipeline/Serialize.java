package com.mercari.solution.util.pipeline;

import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Serialize {

    private static final Logger LOG = LoggerFactory.getLogger(Serialize.class);

    public enum Format {
        json,
        avro,
        protobuf,
        unknown
    }

    private final Format format;

    // for non format
    private final List<Schema.Field> fields;

    // for avro format
    // https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
    private final String avroSchemaJson;
    private transient org.apache.avro.Schema avroSchema;
    private transient GenericDatumReader<GenericRecord> datumReader;
    private transient GenericDatumWriter<GenericRecord> datumWriter;
    private transient BinaryDecoder decoder = null;
    private transient BinaryEncoder encoder = null;

    // for protobuf format
    private final String descriptorFile;
    private final String messageName;
    private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
    private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

    public static Serialize of(
            final Format format,
            final Schema inputSchema) {
        return new Serialize(format, inputSchema);
    }

    Serialize(
            final Format format,
            final Schema inputSchema) {

        this.format = format;
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
            default -> {
                this.fields = inputSchema.getFields();
                this.descriptorFile = null;
                this.messageName = null;
                this.avroSchemaJson = null;
            }
        }
    }

    private void setupCommon() {
        switch (format) {
            case avro -> {
                this.avroSchema = AvroSchemaUtil.convertSchema(avroSchemaJson);
            }
            case protobuf -> {
                long start = java.time.Instant.now().toEpochMilli();
                final Descriptors.Descriptor descriptor = getOrLoadDescriptor(
                        descriptors, printers, messageName, descriptorFile);
                long end = java.time.Instant.now().toEpochMilli();
                LOG.info("Finished setup Output DoFn {} ms, thread id: {}, with descriptor: {}",
                        (end - start),
                        Thread.currentThread().getId(),
                        descriptor.getFullName());
            }
        }
    }

    public void setupDeserialize() {
        setupCommon();
        switch (format) {
            case avro -> {
                this.datumReader = new GenericDatumReader<>(this.avroSchema);
            }
            case protobuf -> {}
        }
    }

    public void setupSerialize() {
        setupCommon();
        switch (format) {
            case avro -> {
                this.datumWriter = new GenericDatumWriter<>(this.avroSchema);
            }
            case protobuf -> {}
        }
    }

    public byte[] serialize(final MElement element) {
        return switch (format) {
            case json -> serializeJson(element);
            case avro -> serializeAvro(element);
            case protobuf -> serializeProtobuf(element);
            default -> throw new IllegalArgumentException("not supported format: " + format);
        };
    }

    public MElement deserialize(byte[] bytes, final Instant timestamp) throws IOException {
        return switch (format) {
            case json -> deserializeJson(bytes, timestamp);
            case avro -> deserializeAvro(bytes, timestamp);
            case protobuf -> deserializeProtobuf(bytes, timestamp);
            default -> throw new IllegalArgumentException("not supported format: " + format);
        };
    }

    public MElement deserialize(byte[] bytes, final Instant timestamp, final DataType outputType) throws IOException {
        return switch (format) {
            case json -> switch (outputType) {
                case ELEMENT -> deserializeJson(bytes, timestamp);
                case AVRO -> deserializeJson(bytes, timestamp);
                default -> throw new IllegalArgumentException("");
            };
            case avro -> switch (outputType) {
                case AVRO -> deserializeAvro(bytes, timestamp);
                case ELEMENT -> deserializeAvro(bytes, timestamp);
                default -> throw new IllegalArgumentException("");
            };
            case protobuf -> switch(outputType) {
                case ELEMENT -> deserializeProtobuf(bytes, timestamp);
                case AVRO -> deserializeProtobuf(bytes, timestamp);
                default -> throw new IllegalArgumentException("");
            };
            default -> throw new IllegalArgumentException("not supported format: " + format);
        };
    }

    private byte[] serializeJson(final MElement element) {
        final JsonObject json = ElementToJsonConverter.convert(fields, element.asPrimitiveMap(), null);
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private MElement deserializeJson(final byte[] bytes, final Instant timestamp) {
        final String json = new String(bytes, StandardCharsets.UTF_8);
        return MElement.of(JsonToElementConverter.convert(fields, json), timestamp);
    }

    private byte[] serializeAvro(final MElement element) {
        final GenericRecord record = ElementToAvroConverter.convert(avroSchema, element);
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
            datumWriter.write(record, encoder);
            encoder.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode record: " + record.toString(), e);
        }
    }

    private MElement deserializeAvro(final byte[] bytes, final Instant timestamp) throws IOException {
        decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
        GenericRecord record = new GenericData.Record(avroSchema);
        record = datumReader.read(record, decoder);
        return MElement.of(record, timestamp);
    }

    private byte[] serializeProtobuf(final MElement element) {
        final Descriptors.Descriptor descriptor = Optional
                .ofNullable(descriptors.get(messageName))
                .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorFile));
        final DynamicMessage message = ElementToProtoConverter.convert(fields, descriptor, element.asPrimitiveMap());
        return message.toByteArray();
    }

    private MElement deserializeProtobuf(final byte[] bytes, final Instant timestamp) {
        final Descriptors.Descriptor descriptor = Optional
                .ofNullable(descriptors.get(messageName))
                .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorFile));
        final JsonFormat.Printer printer = printers.get(messageName);
        return MElement.of(ProtoToElementConverter.convert(fields, descriptor, bytes, printer), timestamp);
    }

    public synchronized static Descriptors.Descriptor getOrLoadDescriptor(
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

    public synchronized static void loadDescriptor(
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

            LOG.info("loaded protobuf descriptor. protoMessage: {} loaded", messageName);
        }
    }

    public synchronized static org.apache.avro.Schema getOrLoadAvroSchema(
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

}
