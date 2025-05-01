package com.mercari.solution.module.sink;

import com.google.auth.oauth2.AccessToken;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.gcp.IAMUtil;
import com.mercari.solution.util.gcp.vertexai.MatchingEngineUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.http.HttpClient;
import java.util.*;
import java.util.stream.Collectors;

public class MatchingEngineSink {

    private static final Logger LOG = LoggerFactory.getLogger(MatchingEngineSink.class);

    private class MatchingEngineSinkParameters implements Serializable {

        private String projectId;
        private String region;
        private String indexId;
        private String idField;
        private String vectorField;
        private String crowdingTagField;
        private List<Restrict> restricts;
        private Method method;

        private Long bufferSize;
        private Long maxBufferFlushIntervalSeconds;


        public String getProjectId() {
            return projectId;
        }

        public String getRegion() {
            return region;
        }

        public String getIndexId() {
            return indexId;
        }

        public String getIdField() {
            return idField;
        }

        public String getVectorField() {
            return vectorField;
        }

        public String getCrowdingTagField() {
            return crowdingTagField;
        }

        public List<Restrict> getRestricts() {
            return restricts;
        }

        public Method getMethod() {
            return method;
        }

        public Long getBufferSize() {
            return bufferSize;
        }

        public Long getMaxBufferFlushIntervalSeconds() {
            return maxBufferFlushIntervalSeconds;
        }


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();

            if(this.region == null) {
                errorMessages.add("MatchingEngine sink module requires region parameter");
            }
            if(this.indexId == null) {
                errorMessages.add("MatchingEngine sink module requires indexId parameter");
            }
            if(this.method == null || Method.upsert.equals(this.method)) {
                if(this.vectorField == null) {
                    errorMessages.add("MatchingEngine sink module requires vectorField parameter when method is upsert");
                }
            } else {
                if(this.idField == null) {
                    errorMessages.add("MatchingEngine sink module requires idField parameter when method is remove");
                }
            }

            if(this.restricts != null) {
                for(int i=0; i<this.restricts.size(); i++) {
                    errorMessages.addAll(this.restricts.get(i).validate(i));
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults(final String defaultProjectId) {
            if(this.projectId == null) {
                this.projectId = defaultProjectId;
            }
            if(this.bufferSize == null) {
                this.bufferSize = 100L;
            }
            if(this.maxBufferFlushIntervalSeconds == null) {
                this.maxBufferFlushIntervalSeconds = 10L;
            }
            if(this.restricts == null) {
                this.restricts = new ArrayList<>();
            }
            if(this.method == null) {
                this.method = Method.upsert;
            }
        }
    }

    public class Restrict implements Serializable {

        private String namespace;
        private String allowField;
        private String denyField;

        public String getNamespace() {
            return namespace;
        }

        public String getAllowField() {
            return allowField;
        }

        public String getDenyField() {
            return denyField;
        }

        public List<String> validate(int i) {
            final List<String> errorMessages = new ArrayList<>();

            if(this.namespace == null) {
                errorMessages.add("MatchingEngine restricts[" + i + "] requires namespace parameter");
            }
            if(this.allowField == null && this.denyField == null) {
                errorMessages.add("MatchingEngine restricts[" + i + "] requires allowField or denyField parameter");
            }

            return errorMessages;
        }

    }

    private enum Method {
        upsert,
        remove
    }

    public String getName() {
        return "matchingEngine";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("matchingEngine sink module requires input parameter");
        }
        final FCollection<?> input = inputs.get(0);

        final MatchingEngineSinkParameters parameters = new Gson().fromJson(config.getParameters(), MatchingEngineSinkParameters.class);
        parameters.validate();
        final String defaultProject = OptionUtil.getDefaultProject();
        parameters.setDefaults(defaultProject);

        final FCollection output;
        final Schema outputSchema = createFailedRecordSchema();
        final DataType inputType = input.getDataType();
        switch (inputType) {
            case ROW: {
                final IndexWrite<Row> write = new IndexWrite<>(
                        config.getName(), parameters,
                        RowSchemaUtil::getAsString,
                        RowSchemaUtil::getAsFloatList,
                        RowSchemaUtil::getValue);
                final PCollection<Row> inputCollection = (PCollection<Row>) input.getCollection();
                final PCollection<Row> outputCollection = inputCollection
                        .apply(config.getName(), write)
                        .setCoder(RowCoder.of(outputSchema));
                output = FCollection.of(config.getName(), outputCollection, DataType.ROW, outputSchema);
                break;
            }
            case AVRO: {
                final IndexWrite<GenericRecord> write = new IndexWrite<>(
                        config.getName(), parameters,
                        AvroSchemaUtil::getAsString,
                        AvroSchemaUtil::getAsFloatList,
                        AvroSchemaUtil::getValue);
                final PCollection<GenericRecord> inputCollection = (PCollection<GenericRecord>) input.getCollection();
                final PCollection<Row> outputCollection = inputCollection
                        .apply(config.getName(), write)
                        .setCoder(RowCoder.of(outputSchema));
                output = FCollection.of(config.getName(), outputCollection, DataType.ROW, outputSchema);
                break;
            }
            case STRUCT: {
                final IndexWrite<Struct> write = new IndexWrite<>(
                        config.getName(), parameters,
                        StructSchemaUtil::getAsString,
                        StructSchemaUtil::getAsFloatList,
                        StructSchemaUtil::getValue);
                final PCollection<Struct> inputCollection = (PCollection<Struct>) input.getCollection();
                final PCollection<Row> outputCollection = inputCollection
                        .apply(config.getName(), write)
                        .setCoder(RowCoder.of(outputSchema));
                output = FCollection.of(config.getName(), outputCollection, DataType.ROW, outputSchema);
                break;
            }
            case DOCUMENT: {
                final IndexWrite<Document> write = new IndexWrite<>(
                        config.getName(), parameters,
                        DocumentSchemaUtil::getAsString,
                        DocumentSchemaUtil::getAsFloatList,
                        DocumentSchemaUtil::getValue);
                final PCollection<Document> inputCollection = (PCollection<Document>) input.getCollection();
                final PCollection<Row> outputCollection = inputCollection
                        .apply(config.getName(), write)
                        .setCoder(RowCoder.of(outputSchema));
                output = FCollection.of(config.getName(), outputCollection, DataType.ROW, outputSchema);
                break;
            }
            case ENTITY: {
                final IndexWrite<Entity> write = new IndexWrite<>(
                        config.getName(), parameters,
                        EntitySchemaUtil::getAsString,
                        EntitySchemaUtil::getAsFloatList,
                        EntitySchemaUtil::getValue);
                final PCollection<Entity> inputCollection = (PCollection<Entity>) input.getCollection();
                final PCollection<Row> outputCollection = inputCollection
                        .apply(config.getName(), write)
                        .setCoder(RowCoder.of(outputSchema));
                output = FCollection.of(config.getName(), outputCollection, DataType.ROW, outputSchema);
                break;
            }
            default:
                throw new IllegalStateException("Not supported inputType: " + inputType);
        }

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        outputs.put(config.getName(), output);
        outputs.put(config.getName() + ".failures", output);
        return outputs;
    }

    private static class IndexWrite<T> extends PTransform<PCollection<T>, PCollection<Row>> {

        private final String name;
        private final MatchingEngineSinkParameters parameters;
        private final SchemaUtil.StringGetter<T> stringGetter;
        private final SchemaUtil.FloatListGetter<T> embeddingGetter;
        private final SchemaUtil.ValueGetter<T> valueGetter;

        IndexWrite(final String name,
                   final MatchingEngineSinkParameters parameters,
                   final SchemaUtil.StringGetter<T> stringGetter,
                   final SchemaUtil.FloatListGetter<T> embeddingGetter,
                   final SchemaUtil.ValueGetter<T> valueGetter) {

            this.name = name;
            this.parameters = parameters;
            this.stringGetter = stringGetter;
            this.embeddingGetter = embeddingGetter;
            this.valueGetter = valueGetter;
        }

        @Override
        public PCollection<Row> expand(PCollection<T> input) {
            final String name = Method.upsert.equals(parameters.method) ? "UpsertIndex" : "RemoveIndex";
            final SerializableFunction<T, String> groupKeysFunction = SchemaUtil.createGroupKeysFunction(stringGetter, new ArrayList<>());
            return input
                    .apply("WithKey", WithKeys.of(groupKeysFunction))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()))
                    .apply(name, ParDo.of(new IndexDoFn<>(name, parameters, stringGetter, embeddingGetter, valueGetter)));
        }

        private static class IndexDoFn<T> extends DoFn<KV<String, T>, Row> {

            private static final String STATE_ID_BUFFER = "buffer";
            private static final String STATE_ID_BUFFER_SIZE = "bufferSize";
            private static final String TIMER_ID = "onFlushBuffer";

            private final String name;
            private final String project;
            private final String region;
            private final String indexId;

            private final String idField;
            private final String vectorField;
            private final String crowdingTagField;
            private final List<Restrict> restricts;
            private final Method method;

            private final Long bufferSize;
            private final Long bufferIntervalSeconds;

            private final SchemaUtil.StringGetter<T> stringGetter;
            private final SchemaUtil.FloatListGetter<T> embeddingGetter;
            private final SchemaUtil.ValueGetter<T> valueGetter;

            @StateId(STATE_ID_BUFFER)
            private final StateSpec<BagState<MatchingEngineUtil.DataPoint>> bufferSpec;
            @StateId(STATE_ID_BUFFER_SIZE)
            private final StateSpec<CombiningState<Long, long[], Long>> bufferSizeSpec;


            @TimerId(TIMER_ID)
            private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            private final Counter counter;
            private final Counter failedCounter;
            private final Schema failedRecordSchema = createFailedRecordSchema();

            private transient HttpClient client;
            private transient AccessToken accessToken;


            IndexDoFn(final String name,
                      final MatchingEngineSinkParameters parameters,
                      final SchemaUtil.StringGetter<T> stringGetter,
                      final SchemaUtil.FloatListGetter<T> embeddingGetter,
                      final SchemaUtil.ValueGetter<T> valueGetter) {

                this.name = name;
                this.project = parameters.getProjectId();
                this.region = parameters.getRegion();
                this.indexId = parameters.getIndexId();
                this.idField = parameters.getIdField();
                this.vectorField = parameters.getVectorField();
                this.crowdingTagField = parameters.getCrowdingTagField();
                this.restricts = parameters.getRestricts();
                this.method = parameters.getMethod();
                this.bufferSize = parameters.getBufferSize();
                this.bufferIntervalSeconds = parameters.getMaxBufferFlushIntervalSeconds();

                this.stringGetter = stringGetter;
                this.embeddingGetter = embeddingGetter;
                this.valueGetter = valueGetter;

                this.bufferSpec = StateSpecs.bag(SerializableCoder.of(MatchingEngineUtil.DataPoint.class));
                this.bufferSizeSpec = StateSpecs.combining(new Combine.BinaryCombineLongFn() {
                    @Override
                    public long apply(long left, long right) {
                        return left + right;
                    }

                    @Override
                    public long identity() {
                        return 0;
                    }
                });

                this.counter = Metrics.counter(name, (Method.upsert.equals(method) ? "upsert" : "remove") + "_count");
                this.failedCounter = Metrics.counter(name, "failed_" + (Method.upsert.equals(method) ? "upsert" : "remove") + "_count");
            }

            @Setup
            public void setup() throws IOException {
                this.client = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(java.time.Duration.ofSeconds(60))
                        .build();

                accessToken = IAMUtil.getAccessToken();
                LOG.info("Setup matching engine worker. ThreadID: " + Thread.currentThread().getId());
            }

            @ProcessElement
            public void processElement(
                    final OutputReceiver<Row> receiver,
                    final @Timestamp Instant timestamp,
                    final @StateId(STATE_ID_BUFFER) BagState<MatchingEngineUtil.DataPoint> bufferState,
                    final @StateId(STATE_ID_BUFFER_SIZE) CombiningState<Long, long[], Long> bufferSizeState,
                    final @TimerId(TIMER_ID) Timer bufferTimer,
                    final ProcessContext c) throws IOException {

                bufferSizeState.readLater();

                final T input = c.element().getValue();

                final MatchingEngineUtil.DataPoint dataPoint;
                switch (method) {
                    case upsert:
                        dataPoint = createUpsertDataPoint(input);
                        break;
                    case remove:
                        dataPoint = createRemoveDataPoint(input);
                        break;
                    default:
                        throw new IllegalStateException("Not supported matching engine method: " + method);
                }

                bufferState.add(dataPoint);
                bufferSizeState.add(1L);

                final long count = bufferSizeState.read();
                if(count >= bufferSize) {
                    flush(receiver, bufferState, bufferSizeState);
                    bufferTimer.clear();
                } else {
                    bufferTimer.offset(Duration.standardSeconds(bufferIntervalSeconds)).setRelative();
                }
            }


            @OnTimer(TIMER_ID)
            public void onTimer(
                    final OnTimerContext c,
                    final OutputReceiver<Row> receiver,
                    final @StateId(STATE_ID_BUFFER) BagState<MatchingEngineUtil.DataPoint> bufferState,
                    final @StateId(STATE_ID_BUFFER_SIZE) CombiningState<Long, long[], Long> bufferSizeState) {

                LOG.info("onTimer: " + c.timestamp());

                flush(receiver, bufferState, bufferSizeState);
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                    final OutputReceiver<Row> receiver,
                    final @StateId(STATE_ID_BUFFER) BagState<MatchingEngineUtil.DataPoint> bufferState,
                    final @StateId(STATE_ID_BUFFER_SIZE) CombiningState<Long, long[], Long> bufferSizeState) {

                LOG.info("onWindowExpiration");

                flush(receiver, bufferState, bufferSizeState);
            }

            private void flush(final OutputReceiver<Row> receiver,
                              final BagState<MatchingEngineUtil.DataPoint> bufferState,
                              final CombiningState<Long, long[], Long> bufferSizeState) {

                final Iterable<MatchingEngineUtil.DataPoint> buffer = bufferState.read();
                try {
                    final Integer count;
                    switch (method) {
                        case upsert:
                            count = upsert(buffer);
                            break;
                        case remove:
                            count = remove(buffer);
                            break;
                        default:
                            throw new IllegalStateException("Not supported matching engine method: " + method);
                    }
                    counter.inc(count);
                } catch (final Throwable e) {
                    final List<Row> failedRecords = createFailedRecords(failedRecordSchema, buffer, e);
                    failedCounter.inc(failedRecords.size());
                    for(final Row failedRecord : failedRecords) {
                        receiver.output(failedRecord);
                    }
                }

                bufferState.clear();
                bufferSizeState.clear();
            }

            private MatchingEngineUtil.DataPoint createUpsertDataPoint(T input) {
                final String id = stringGetter.getAsString(input, idField);
                final List<Float> embedding = embeddingGetter.getAsFloats(input, vectorField);

                final Map<String, List<String>> restrictAllowValuesMap = new HashMap<>();
                final Map<String, List<String>> restrictDenyValuesMap = new HashMap<>();
                for(final Restrict restrict : restricts) {
                    final Object allowValue = valueGetter.getValue(input, restrict.getAllowField());
                    if(allowValue != null) {
                        final List<String> restrictAllowValues;
                        if(allowValue instanceof List) {
                            restrictAllowValues = ((List<?>) allowValue).stream()
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                        } else {
                            restrictAllowValues = new ArrayList<>();
                            restrictAllowValues.add(allowValue.toString());
                        }
                        restrictAllowValuesMap.put(restrict.getNamespace(), restrictAllowValues);
                    }

                    final Object denyValue = valueGetter.getValue(input, restrict.getDenyField());
                    if(denyValue != null) {
                        final List<String> restrictDenyValues;
                        if(denyValue instanceof List) {
                            restrictDenyValues = ((List<?>) denyValue).stream()
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                        } else {
                            restrictDenyValues = new ArrayList<>();
                            restrictDenyValues.add(denyValue.toString());
                        }
                        restrictDenyValuesMap.put(restrict.getNamespace(), restrictDenyValues);
                    }
                }

                final String crowdingTag = crowdingTagField == null ? null : stringGetter.getAsString(input, crowdingTagField);

                return MatchingEngineUtil.DataPoint.of(id, embedding, crowdingTag, restrictAllowValuesMap, restrictDenyValuesMap);
            }

            private MatchingEngineUtil.DataPoint createRemoveDataPoint(T input) {
                final String id = stringGetter.getAsString(input, idField);
                return MatchingEngineUtil.DataPoint.of(id);
            }

            private Integer upsert(final Iterable<MatchingEngineUtil.DataPoint> dataPoints) throws IOException {
                if(this.accessToken.getExpirationTime().before(new Date())) {
                    LOG.info("Refreshed access token with expirationTime: " + accessToken.getExpirationTime());
                    this.accessToken = IAMUtil.getAccessToken();
                }
                final String token = accessToken.getTokenValue();

                return MatchingEngineUtil.upsertDatapoints(client, token, project, region, indexId, dataPoints);
            }

            private Integer remove(final Iterable<MatchingEngineUtil.DataPoint> dataPoints) throws IOException {
                if(this.accessToken.getExpirationTime().before(new Date())) {
                    LOG.info("Refreshed access token with expirationTime: " + accessToken.getExpirationTime());
                    this.accessToken = IAMUtil.getAccessToken();
                }
                final String token = accessToken.getTokenValue();

                final List<String> dataPointIds = new ArrayList<>();
                for(final MatchingEngineUtil.DataPoint dataPoint : dataPoints) {
                    dataPointIds.add(dataPoint.getId());
                }

                return MatchingEngineUtil.removeDatapoints(client, token, project, region, indexId, dataPointIds);
            }

            private static List<Row> createFailedRecords(
                    final Schema schema,
                    final Iterable<MatchingEngineUtil.DataPoint> dataPoints,
                    final Throwable e) {

                final List<Row> failedRecords = new ArrayList<>();
                for(final MatchingEngineUtil.DataPoint dataPoint : dataPoints) {
                    final Row failedRecord = Row.withSchema(schema)
                            .withFieldValue("id", dataPoint.getId())
                            .withFieldValue("vector", dataPoint.getEmbedding())
                            .withFieldValue("errorMessage", e.getMessage())
                            .build();
                    failedRecords.add(failedRecord);
                }
                return failedRecords;
            }

        }

    }

    private static Schema createFailedRecordSchema() {
        return Schema.builder()
                .addField("id", Schema.FieldType.STRING.withNullable(true))
                .addField("vector", Schema.FieldType.array(Schema.FieldType.FLOAT).withNullable(true))
                .addField("errorMessage", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

}