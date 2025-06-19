package com.mercari.solution.module.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Source.Module(name="kafka")
public class KafkaSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    private static class Parameters implements Serializable {

        private String topic;
        private List<String> topics;
        private String topicPattern;
        private String bootstrapServers;
        private Integer maxNumRecords;
        private Duration maxReadTime;
        private Boolean withProcessingTime;
        private Boolean withReadCommitted;
        private Boolean withLogAppendTime;
        private Boolean withoutMetadata;

        private String idAttribute;

        private Serialize.Format format;
        private Boolean outputOriginal;
        private String charset;

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;


        private void validate(final PBegin begin, final Schema schema) {

            if(!OptionUtil.isStreaming(begin)) {
                throw new IllegalModuleException("Kafka source module only support streaming mode.");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(topic == null && (topics == null || topics.isEmpty())) {
                errorMessages.add("parameters.topic or subscription is required");
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
                format = Serialize.Format.unknown;
            }
            if(outputOriginal == null) {
                outputOriginal = false;
            }
            if(charset == null) {
                charset = StandardCharsets.UTF_8.name();
            }
        }

    }

    private enum Format {
        json,
        avro,
        protobuf,
        message
    }


    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(begin, getSchema());
        parameters.setDefaults();

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};
        final TupleTag<MElement> originalTag = new TupleTag<>() {};

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.ELEMENT);

        final Schema inputSchema = null;//createDeserializedInputSchema(parameters, getSchema());
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
                .apply("ReadKafka", createRead(parameters, errorHandler))
                .apply("Format", ParDo
                        .of(new OutputDoFn(getJobName(), getName(),
                                inputSchema, outputSchema, parameters,
                                outputType, getLoggings(), selectFunctions,
                                getFailFast(), failuresTag, originalTag))
                        .withOutputTags(outputTag, TupleTagList.of(outputTags)));

        errorHandler.addError(outputs.get(failuresTag));

        final MCollectionTuple outputTuple = MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);

        if (parameters.outputOriginal) {
            final Schema originalSchema = null;//createMessageSchema().withType(DataType.MESSAGE);
            return outputTuple
                    .and("original", outputs.get(originalTag), originalSchema);
        } else {
            return outputTuple;
        }
    }

    private static KafkaIO.Read<byte[], byte[]> createRead(
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        KafkaIO.Read<byte[], byte[]> read = KafkaIO.readBytes();
        if(parameters.bootstrapServers != null) {
            read = read.withBootstrapServers(parameters.bootstrapServers);
        }
        if(parameters.topic != null) {
            read = read.withTopic(parameters.topic);
        } else if(parameters.topics != null && !parameters.topics.isEmpty()) {
            read = read.withTopics(parameters.topics);
        } else if(parameters.topicPattern != null) {
            read = read.withTopicPattern(parameters.topicPattern);
        } else {
            throw new IllegalArgumentException("");
        }
        if(parameters.maxNumRecords != null) {
            read = read.withMaxNumRecords(parameters.maxNumRecords);
        }
        if(parameters.maxReadTime != null) {
            read = read.withMaxReadTime(parameters.maxReadTime);
        }

        if(parameters.withProcessingTime != null) {
            read.withProcessingTime();
        }
        if(parameters.withReadCommitted != null) {
            read.withReadCommitted();
        }
        if(parameters.withLogAppendTime != null) {
            read.withLogAppendTime();
        }
        if(parameters.withoutMetadata != null) {
            read.withoutMetadata();
        }

        errorHandler.apply(read);

        return read;
    }

    private static class OutputDoFn extends DoFn<KafkaRecord<byte[], byte[]>, MElement> {

        private final String jobName;
        private final String moduleName;

        private final Serialize.Format format;

        private final Map<String, Logging> loggings;
        private final DataType outputType;

        private final Serialize serialize;
        private final Filter filter;
        private final Select select;
        private final Unnest unnest;

        private final String charset;

        private final boolean failFast;
        private final boolean outputOriginal;
        private final TupleTag<BadRecord> failuresTag;
        private final TupleTag<MElement> originalTag;

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
            this.outputType = outputType;
            this.loggings = Logging.map(loggings);

            this.serialize = Serialize.of(format, inputSchema);
            this.filter = Filter.of(parameters.filter);
            this.select = Select.of(selectFunctions);
            this.unnest = Unnest.of(parameters.flattenField);

            this.charset = parameters.charset;

            this.failFast = failFast;
            this.outputOriginal = parameters.outputOriginal;
            this.failuresTag = failuresTag;
            this.originalTag = originalTag;

            this.outputSchema = outputSchema;
        }

        @Setup
        public void setup() {
            this.outputSchema.setup(outputType);
            this.serialize.setupDeserialize();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final KafkaRecord<byte[], byte[]> message = c.element();
            if (message == null) {
                return;
            }

            try {
                //Logging.log(LOG, loggings, "input", MessageSchemaUtil.toJsonString(message));
                if (outputOriginal) {
                    //final MElement element = MElement.of(message, c.timestamp());
                    //c.output(originalTag, element);
                }
                //var attributes = message.getHeaders();
                //if(!filter.filter(attributes)) {
                //    return;
                //}
                final MElement output = serialize.deserialize(message.getKV().getValue(), c.timestamp());

                Logging.log(LOG, loggings, "output", output);
                c.output(output);
            } catch (final Throwable e) {
                //ERROR_COUNTER.inc();
                String errorMessage = MFailure.convertThrowableMessage(e);
                LOG.error("kafka source parse error: {}, {} for message: {}", e, errorMessage, message);
                if (failFast) {
                    throw new IllegalStateException(errorMessage, e);
                }
                //final MFailure failureElement = createFailureElement(c, message, e);
                //c.output(failuresTag, failureElement.toElement(c.timestamp()));
            }
        }
    }

}