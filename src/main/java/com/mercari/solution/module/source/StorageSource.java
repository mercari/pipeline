package com.mercari.solution.module.source;

import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.aws.S3Util;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.CsvToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Source.Module(name="storage")
public class StorageSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSource.class);

    private static class Parameters implements Serializable {

        private String input;
        private List<String> inputs;
        private Format format;
        private String compression;

        // for parquet
        private List<String> fields;

        // for csv
        private String filterPrefix;
        private Integer skipHeaderLines;
        private String delimiter;

        // for AWS S3
        private S3Parameters s3;

        private static class S3Parameters implements Serializable {
            private String accessKey;
            private String secretKey;
            private String region;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if((inputs == null || inputs.isEmpty()) && input == null) {
                errorMessages.add("parameters.input or inputs is required");
            }
            if(this.format == null) {
                errorMessages.add("parameters.format must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(inputs == null) {
                inputs = new ArrayList<>();
            }
            if(inputs.isEmpty() && input != null) {
                inputs.add(input);
            } else if(input == null && !inputs.isEmpty()) {
                input = inputs.getFirst();
            }
            if(fields == null) {
                fields = new ArrayList<>();
            }
        }
    }

    public enum Format implements Serializable {
        avro,
        parquet,
        csv,
        json
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        return switch (parameters.format) {
            case avro, parquet -> {
                final PCollection<GenericRecord> record;
                final org.apache.avro.Schema outputAvroSchema;
                switch (parameters.format) {
                    case avro -> {
                        outputAvroSchema = getAvroSchema(parameters.input, getSchema(), parameters.s3);
                        if(parameters.inputs.size() > 1) {
                            PCollectionList<GenericRecord> list = PCollectionList.empty(begin.getPipeline());
                            int i = 0;
                            for(final String input : parameters.inputs) {
                                final PCollection<GenericRecord> p = begin
                                        .apply("ReadAvro" + i, AvroIO
                                                .readGenericRecords(outputAvroSchema)
                                                .from(input))
                                        .setCoder(AvroCoder.of(outputAvroSchema));
                                list = list.and(p);
                                i++;
                            }
                            record = list.apply("Flatten", Flatten.pCollections());
                        } else {
                            record = begin
                                    .apply("ReadAvro", AvroIO
                                            .readGenericRecords(outputAvroSchema)
                                            .from(parameters.input))
                                    .setCoder(AvroCoder.of(outputAvroSchema));
                        }
                    }
                    case parquet -> {
                        final org.apache.avro.Schema inputAvroSchema = getParquetSchema(parameters.input, getSchema(), parameters.s3);
                        if(parameters.fields.isEmpty()) {
                            outputAvroSchema = inputAvroSchema;
                        } else {
                            outputAvroSchema = createNullableSchema(inputAvroSchema, parameters.fields);
                        }
                        if (parameters.inputs.size() > 1) {
                            PCollectionList<GenericRecord> list = PCollectionList.empty(begin.getPipeline());
                            int i = 0;
                            for(final String input : parameters.inputs) {
                                final PCollection<GenericRecord> p = begin
                                        .apply("ReadAvro" + i, createParquetRead(input, inputAvroSchema, parameters))
                                        .setCoder(AvroCoder.of(outputAvroSchema));
                                list = list.and(p);
                                i++;
                            }
                            record = list.apply("Flatten", Flatten.pCollections());
                        } else {
                            record = begin
                                    .apply("ReadParquet", createParquetRead(parameters.input, inputAvroSchema, parameters))
                                    .setCoder(AvroCoder.of(outputAvroSchema));
                        }
                    }
                    default -> throw new IllegalArgumentException("Storage module not support format: " + parameters.format);
                }

                final PCollection<MElement> output = record
                        .apply("Format", ParDo
                                .of(new AvroFormatDoFn(getTimestampAttribute())));
                final Schema outputSchema = Schema.of(outputAvroSchema);

                yield MCollectionTuple
                        .of(output, outputSchema);
            }
            case csv, json -> {
                final String format = parameters.format.name().toUpperCase();
                final String readName = String.format("Read%sLine", format);

                PCollection<String> record;
                final Schema inputSchema = getSchema();
                if(parameters.inputs.size() > 1) {
                    PCollectionList<String> list = PCollectionList.empty(begin.getPipeline());
                    int i = 0;
                    for(final String input : parameters.inputs) {
                        final PCollection<String> lines = begin
                                .apply(readName + i, createTextRead(parameters, input))
                                .setCoder(StringUtf8Coder.of());
                        list = list.and(lines);
                        i++;
                    }
                    record = list.apply("Flatten", Flatten.pCollections());
                } else {
                    record = begin
                            .apply(readName, createTextRead(parameters, parameters.inputs.getFirst()))
                            .setCoder(StringUtf8Coder.of());
                }

                if (parameters.filterPrefix != null) {
                    final String filterPrefix = parameters.filterPrefix;
                    record = record
                            .apply("FilterPrefix", Filter.by(s -> s != null &&  !s.startsWith(filterPrefix)));
                }

                final PCollection<MElement> output = record
                        .apply("Convert",ParDo
                                .of(new TextFormatDoFn(getName(), inputSchema, parameters.format, getTimestampAttribute(), getSchema() == null)))
                        .setCoder(ElementCoder.of(inputSchema));

                yield MCollectionTuple
                        .of(output, inputSchema);
            }
        };
    }

    private static ParquetIO.Read createParquetRead(
            final String input,
            final org.apache.avro.Schema readSchema,
            final Parameters parameters) {

        ParquetIO.Read read = ParquetIO
                .read(readSchema)
                .from(input);

        if(!parameters.fields.isEmpty()) {
            final org.apache.avro.Schema projectionSchema = AvroSchemaUtil.toBuilder(readSchema, parameters.fields).endRecord();
            final org.apache.avro.Schema encodeSchema = createNullableSchema(readSchema, parameters.fields);
            read = read.withProjection(projectionSchema, encodeSchema);
        }
        return read;
    }

    private static TextIO.Read createTextRead(
            final Parameters parameters,
            final String input) {
        TextIO.Read read = TextIO.read().from(input);
        if(parameters.compression != null) {
            read = read.withCompression(Compression
                    .valueOf(parameters.compression.trim().toUpperCase()));
        }
        if(parameters.skipHeaderLines != null) {
            read = read.withSkipHeaderLines(parameters.skipHeaderLines);
        }
        if(parameters.delimiter != null) {
            read = read.withDelimiter(parameters.delimiter.getBytes(StandardCharsets.UTF_8));
        }
        return read;
    }

    private static org.apache.avro.Schema createNullableSchema(
            final org.apache.avro.Schema schema,
            final List<String> fields) {
        final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = SchemaBuilder
                .record(Optional.ofNullable(schema.getName()).orElse("root"))
                .namespace(schema.getNamespace())
                .fields();
        for(org.apache.avro.Schema.Field field : schema.getFields()) {
            if(fields.contains(field.name())) {
                builder.name(field.name()).type(field.schema()).noDefault();
            } else {
                builder.name(field.name()).type(AvroSchemaUtil.toNullable(field.schema())).noDefault();
            }
        }
        return builder.endRecord();
    }

    private static class AvroFormatDoFn extends DoFn<GenericRecord, MElement> {

        private final String timestampAttribute;

        AvroFormatDoFn(final String timestampAttribute) {
            this.timestampAttribute = timestampAttribute;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement output = MElement.of(c.element(), c.timestamp());
            if(timestampAttribute != null) {
                final Instant eventTime = output.getAsJodaInstant(timestampAttribute);
                final MElement outputWithTimestamp = output.withEventTime(eventTime);
                c.outputWithTimestamp(outputWithTimestamp, eventTime);
            } else {
                c.output(output);
            }
        }
    }


    private static class TextFormatDoFn extends DoFn<String, MElement> {

        private final String name;
        private final Schema inputSchema;
        private final Format format;
        private final String timestampAttribute;
        private final boolean rawSchema;

        TextFormatDoFn(
                final String name,
                final Schema inputSchema,
                final Format format,
                final String timestampAttribute,
                final boolean rawSchema) {

            this.name = name;
            this.inputSchema = inputSchema;
            this.format = format;
            this.timestampAttribute = timestampAttribute;
            this.rawSchema = rawSchema;
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> values;
            if(rawSchema) {
                values = new HashMap<>();
                values.put("text", c.element());
                values.put("name", name);
                values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
            } else if(Format.csv.equals(format)) {
                values = CsvToElementConverter.convert(inputSchema.getFields(), c.element());
            } else {
                values = JsonToElementConverter.convert(inputSchema.getFields(), c.element());
            }

            if(timestampAttribute != null) {
                final long eventTimeEpochMillis;
                if(values == null || !values.containsKey(timestampAttribute)) {
                    eventTimeEpochMillis = c.timestamp().getMillis();
                } else {
                    eventTimeEpochMillis = switch (values.get(timestampAttribute)) {
                        case Number n -> n.longValue() / 1000L;
                        case String s -> DateTimeUtil.toEpochMicroSecond(s) / 1000L;
                        case null, default -> c.timestamp().getMillis();
                    };
                }
                final MElement output = MElement.of(values, eventTimeEpochMillis);
                c.outputWithTimestamp(output, Instant.ofEpochMilli(eventTimeEpochMillis));
            } else {
                final MElement output = MElement.of(values, c.timestamp());
                c.output(output);
            }
        }

    }

    private static org.apache.avro.Schema getAvroSchema(
            final String input,
            final Schema inputSchema,
            final Parameters.S3Parameters s3) {

        if(inputSchema != null) {
            return inputSchema.getAvroSchema();
        }

        if(input.startsWith("gs://")) {
            final org.apache.avro.Schema avroSchema = StorageUtil.getAvroSchema(input);
            if(avroSchema != null) {
                return avroSchema;
            }
            return StorageUtil.listFiles(input)
                    .stream()
                    .map(StorageUtil::getAvroSchema)
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else if(input.startsWith("s3://")) {
            final S3Client client = S3Util.storage(s3.accessKey, s3.secretKey, s3.region);
            final org.apache.avro.Schema avroSchema = S3Util.getAvroSchema(client, input);
            if(avroSchema != null) {
                return avroSchema;
            }
            final String bucket = S3Util.getBucketName(input);
            return S3Util.listFiles(client, input)
                    .stream()
                    .map(object -> S3Util.getAvroSchema(client, bucket, object))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else {
            throw new IllegalArgumentException("Avro schema not found for input: " + input);
        }

    }

    private static org.apache.avro.Schema getParquetSchema(
            final String input,
            final Schema inputSchema,
            final Parameters.S3Parameters s3) {

        if(inputSchema != null) {
            return inputSchema.getAvroSchema();
        }

        if(input.startsWith("gs://")) {
            final org.apache.avro.Schema avroSchema = StorageUtil.getParquetSchema(input);
            if(avroSchema != null) {
                return avroSchema;
            }
            return StorageUtil.listFiles(input)
                    .stream()
                    .map(StorageUtil::getParquetSchema)
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else if(input.startsWith("s3://")) {
            final S3Client client = S3Util.storage(s3.accessKey, s3.secretKey, s3.region);
            final org.apache.avro.Schema avroSchema = S3Util.getParquetSchema(client, input);
            if(avroSchema != null) {
                return avroSchema;
            }
            final String bucket = S3Util.getBucketName(input);
            return S3Util.listFiles(client, input)
                    .stream()
                    .map(path -> S3Util.getParquetSchema(client, bucket, path))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else {
            throw new IllegalArgumentException("Avro schema not found for input: " + input);
        }

    }

}
