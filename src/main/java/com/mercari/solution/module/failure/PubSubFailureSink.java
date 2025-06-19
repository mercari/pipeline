package com.mercari.solution.module.failure;

import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FailureSink.Module(name="pubsub")
public class PubSubFailureSink extends FailureSink {

    private static class Parameters implements Serializable {

        private String topic;
        private Format format;
        private String idAttribute;
        private String timestampAttribute;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;

        private void validate(String name) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.topic == null) {
                errorMessages.add("parameters.topic must not be null");
            } else if(!PubSubUtil.isTopicResource(this.topic)) {
                errorMessages.add("parameters.topic is illegal format: " + this.topic);
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(name, "failures", errorMessages);
            }
        }

        private void setDefaults() {
            if(this.format == null) {
                this.format = Format.json;
            }
        }
    }

    public enum Format {
        json,
        avro
    }

    @Override
    public PDone expand(PCollection<BadRecord> input) {
        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        final org.apache.avro.Schema badRecordAvroSchema = FailureUtil.createBadRecordSchema();

        return input
                .apply("ToMessage", ParDo
                        .of(new OutputDoFn(jobName, moduleName, parameters, badRecordAvroSchema)))
                .apply("Write", createWrite(parameters));
    }

    private static class OutputDoFn extends DoFn<BadRecord, PubsubMessage> {

        private final String jobName;
        private final String moduleName;
        private final Format format;
        private final String badRecordAvroSchemaString;

        private transient List<Schema.Field> fields;
        private transient org.apache.avro.Schema badRecordAvroSchema;

        OutputDoFn(
                final String jobName,
                final String moduleName,
                final Parameters parameters,
                final org.apache.avro.Schema badRecordAvroSchema) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.format = parameters.format;
            this.badRecordAvroSchemaString = badRecordAvroSchema.toString();
        }

        @Setup
        public void setup() {
            this.fields = createBadRecordSchema().getFields();
            this.badRecordAvroSchema = AvroSchemaUtil.convertSchema(badRecordAvroSchemaString);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final BadRecord badRecord = c.element();
            if(badRecord == null) {
                return;
            }

            try {
                final byte[] bytes = switch (format) {
                    case json -> toJson(badRecord, c.timestamp());
                    case avro -> toAvro(badRecord, c.timestamp());
                };

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("job", jobName);
                attributes.put("module", moduleName);
                attributes.put("format", format.name());

                final PubsubMessage pubsubMessage = new PubsubMessage(bytes, attributes, null, null);
                c.output(pubsubMessage);
            } catch (final Throwable e) {
                FAILURE_ERROR_COUNTER.inc();
                LOG.error("Failed to send bad record: {} to pubsub cause: {}", badRecord, e.getMessage());
            }
        }

        private byte[] toJson(final BadRecord badRecord, final Instant timestamp) {
            final Map<String, Object> map = convertToMap(badRecord, jobName, moduleName, timestamp);
            final JsonObject jsonObject = ElementToJsonConverter.convert(fields, map);
            return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
        }

        private byte[] toAvro(final BadRecord badRecord, final Instant timestamp) throws IOException {
            final GenericRecord element = convertToAvro(badRecordAvroSchema, badRecord, jobName, moduleName, timestamp);
            return AvroSchemaUtil.encode(element);
        }
    }

    private static PubsubIO.Write<PubsubMessage> createWrite(Parameters parameters) {
        PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(parameters.topic);
        if (parameters.idAttribute != null) {
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
        return write;
    }
}
