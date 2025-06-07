package com.mercari.solution.util;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.JsonObject;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.MessageSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.io.IOUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FailureUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FailureUtil.class);

    private static final String RESOURCE_BAD_RECORD_AVRO_SCHEMA_PATH = "/schema/avro/badrecord.avsc";
    private static final String RESOURCE_RUNTIME_BAD_RECORD_AVRO_SCHEMA_PATH = "/template/MPipeline/resources/schema/avro/badrecord.avsc";

    private FailureUtil() {}

    public static BadRecord createBadRecord(
            final PubsubMessage message,
            final String description,
            final Throwable e) {

        return createBadRecord(MElement.of(message, Instant.now()), description, e);
    }

    public static BadRecord createBadRecord(
            final Map<String, Object> primitiveValues,
            final String description,
            final Throwable e) {

        return createBadRecord(MElement.of(primitiveValues, Instant.now()), description, e);
    }

    public static BadRecord createBadRecord(
            final MElement input,
            final String description,
            final Throwable e) {

        final BadRecord.Record record = createRecord(input);
        final BadRecord.Failure failure = createFailure(description, e);
        return BadRecord.builder()
                .setRecord(record)
                .setFailure(failure)
                .build();
    }

    public static BadRecord.Failure createFailure(
            final String description,
            final Throwable e) {

        return createFailure(description, e.getMessage(), convertThrowableStackTrace(e));
    }

    public static BadRecord.Failure createFailure(
            final String description,
            final String exception,
            final String exceptionStacktrace) {

        final BadRecord.Failure.Builder builder = BadRecord.Failure.builder();
        builder.setDescription(description);
        builder.setException(exception);
        builder.setExceptionStacktrace(exceptionStacktrace);
        return builder.build();
    }

    public static BadRecord.Record createRecord(final MElement element) {
        try {
            return switch (element.getType()) {
                case ELEMENT -> {
                    final Map<String, Object> values = (Map<String, Object>) element.getValue();
                    final String json = MapToJsonConverter.convert(values);
                    yield createRecord("ElementCoder", new byte[0], json);
                }
                case AVRO -> {
                    final GenericRecord record = (GenericRecord) element.getValue();
                    byte[] bytes;
                    try {
                        bytes = AvroSchemaUtil.encode(record);
                    } catch (Throwable e) {
                        bytes = new byte[0];
                    }
                    String json;
                    try {
                        json = AvroToJsonConverter.convert(record);
                    } catch (Throwable e) {
                        json = null;
                    }
                    yield createRecord("AvroCoder", bytes, json);
                }
                case ROW -> {
                    final Row row = (Row) element.getValue();
                    final String json = RowToJsonConverter.convert(row);
                    byte[] bytes;
                    try {
                        bytes = RowSchemaUtil.encode(row);
                    } catch (Throwable e) {
                        bytes = new byte[0];
                    }
                    yield createRecord("RowCoder", bytes, json);
                }
                case PROTO -> {
                    final DynamicMessage message = (DynamicMessage) element.getValue();
                    byte[] bytes;
                    try {
                        bytes = message.toByteArray();
                    } catch (Throwable e) {
                        bytes = new byte[0];
                    }
                    String json;
                    try {
                        json = ProtoToJsonConverter.convert(message);
                    } catch (Throwable e) {
                        json = null;
                    }

                    yield createRecord("DynamicProtoCoder", bytes, json);
                }
                case STRUCT -> {
                    final String json = StructToJsonConverter.convert((Struct) element.getValue());
                    yield createRecord("SerializableCoder", new byte[0], json);
                }
                case DOCUMENT -> {
                    final String json = DocumentToJsonConverter.convert((Document) element.getValue());
                    yield createRecord("SerializableCoder", new byte[0], json);
                }
                case ENTITY -> {
                    final String json = EntityToJsonConverter.convert((Entity) element.getValue());
                    yield createRecord("SerializableCoder", new byte[0], json);
                }
                case MESSAGE -> {
                    final PubsubMessage message = (PubsubMessage) element.getValue();
                    byte[] bytes;
                    try {
                        bytes = MessageSchemaUtil.encode(message);
                    } catch (Throwable e) {
                        bytes = new byte[0];
                    }
                    String json;
                    try {
                        json = MessageSchemaUtil.toJsonString(message);
                    } catch (Throwable e) {
                        json = null;
                    }
                    yield createRecord("PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder", bytes, json);
                }
                default -> {
                    final JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("object", element.toString());
                    yield createRecord("SerializableCoder", new byte[0], jsonObject.toString());
                }
            };
        } catch (Throwable e) {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("error", e.getMessage());
            return createRecord("", new byte[0], jsonObject.toString());
        }
    }

    public static BadRecord.Record createRecord(
            final String coder,
            final byte[] bytes,
            final String json) {

        final BadRecord.Record.Builder builder = BadRecord.Record.builder();
        builder.setCoder(coder);
        builder.setEncodedRecord(bytes);
        builder.setHumanReadableJsonRecord(json);
        return builder.build();
    }

    public static String convertThrowableStackTrace(final Throwable e) {
        final StringBuilder sb = new StringBuilder();
        for(final StackTraceElement stackTrace : e.getStackTrace()) {
            sb.append(stackTrace.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String convertBadRecordFailureMessage(final BadRecord.Failure failure) {
        final StringBuilder sb = new StringBuilder();
        sb.append(failure.getDescription());
        sb.append("\n");
        sb.append(failure.getException());
        sb.append("\n");
        sb.append(failure.getExceptionStacktrace());
        return sb.toString();
    }

    public static String convertThrowableMessage(final Throwable e) {

        final StringBuilder sb = new StringBuilder();
        if(e.getMessage() != null) {
            sb.append(e.getMessage());
            sb.append("\n");
        } else if(e.getLocalizedMessage() != null) {
            sb.append(e.getLocalizedMessage());
            sb.append("\n");
        }
        if(e.getCause() != null) {
            sb.append(e.getCause());
            sb.append("\n");
        }
        for(final StackTraceElement stackTrace : e.getStackTrace()) {
            sb.append(stackTrace.toString());
            sb.append("\n");
        }

        return sb.toString();
    }

    public static SerializableFunction<AvroWriteRequest<BadRecord>, GenericRecord> createAvroConverter(
            final String jobName, final String moduleName) {
        return request -> convertToAvro(request.getSchema(), request.getElement(), jobName, moduleName, Instant.now());
    }

    public static GenericRecord convertToAvro(
            final org.apache.avro.Schema schema,
            final BadRecord badRecord,
            final String jobName,
            final String moduleName,
            final Instant eventTime) {

        final Map<String, Object> values = convertToMap(badRecord, jobName, moduleName, eventTime);
        return AvroSchemaUtil.create(schema,values);
    }

    public static Map<String, Object> convertToMap(
            final BadRecord badRecord,
            final String jobName,
            final String moduleName,
            final Instant eventTime) {

        final Map<String, Object> values = new HashMap<>();
        values.put("job", jobName);
        values.put("module", moduleName);
        {
            final Map<String, Object> recordValues = new HashMap<>();
            if(badRecord.getRecord() != null) {
                recordValues.put("coder", badRecord.getRecord().getCoder());
                recordValues.put("json", badRecord.getRecord().getHumanReadableJsonRecord());
                recordValues.put("bytes", Optional
                        .ofNullable(badRecord.getRecord().getEncodedRecord())
                        .map(ByteBuffer::wrap)
                        .orElse(null));
            }
            values.put("record", recordValues);
        }
        {
            final Map<String, Object> failureValues = new HashMap<>();
            if(badRecord.getFailure() != null) {
                failureValues.put("description", badRecord.getFailure().getDescription());
                failureValues.put("exception", badRecord.getFailure().getException());
                failureValues.put("stacktrace", badRecord.getFailure().getExceptionStacktrace());
            }
            values.put("failure", failureValues);
        }
        values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
        values.put("eventtime", eventTime.getMillis() * 1000L);
        return values;
    }

    public static Schema createBadRecordSchema() {
        try (final InputStream is = FailureUtil.class.getResourceAsStream(RESOURCE_BAD_RECORD_AVRO_SCHEMA_PATH)) {
            if(is == null) {
                LOG.info("BadRecord avro file is not found: " + RESOURCE_BAD_RECORD_AVRO_SCHEMA_PATH);
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_BAD_RECORD_AVRO_SCHEMA_PATH))) {
                    final String schemaJson = IOUtils.toString(iss,  StandardCharsets.UTF_8);
                    return AvroSchemaUtil.convertSchema(schemaJson);
                } catch (Throwable e) {
                    throw new IllegalArgumentException("BadRecord avro file is not found", e);
                }
            }
            final String schemaJson = IOUtils.toString(is,  StandardCharsets.UTF_8);
            return AvroSchemaUtil.convertSchema(schemaJson);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Not found event descriptor file", e);
        }
    }

}
