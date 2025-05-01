package com.mercari.solution.module.transform;

import com.google.auth.oauth2.AccessToken;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.NullValue;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.gcp.IAMUtil;
import com.mercari.solution.util.gcp.VertexAIUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class AutoMLTransform {

    private static final Logger LOG = LoggerFactory.getLogger(AutoMLTransform.class);

    private static final String OUTPUT_SUFFIX_FAILURES = ".failures";

    private class AutoMLTransformParameters implements Serializable {

        private String endpoint;

        private Type type;
        private Objective objective;
        private EndpointType endpointType;

        private String prefix;
        private Integer batchSize;
        private Long maxPeriodSecond;
        private Long connectTimeoutSecond;
        private Integer parallelNum;
        private Boolean failFast;

        private Boolean deployModel;
        private Boolean undeployModel;

        private DeployModelParameter model;

        private List<String> groupFields;

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public Objective getObjective() {
            return objective;
        }

        public void setObjective(Objective objective) {
            this.objective = objective;
        }

        public EndpointType getEndpointType() {
            return endpointType;
        }

        public void setEndpointType(EndpointType endpointType) {
            this.endpointType = endpointType;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Long getMaxPeriodSecond() {
            return maxPeriodSecond;
        }

        public void setMaxPeriodSecond(Long maxPeriodSecond) {
            this.maxPeriodSecond = maxPeriodSecond;
        }

        public Long getConnectTimeoutSecond() {
            return connectTimeoutSecond;
        }

        public void setConnectTimeoutSecond(Long connectTimeoutSecond) {
            this.connectTimeoutSecond = connectTimeoutSecond;
        }

        public Integer getParallelNum() {
            return parallelNum;
        }

        public void setParallelNum(Integer parallelNum) {
            this.parallelNum = parallelNum;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public void setGroupFields(List<String> groupFields) {
            this.groupFields = groupFields;
        }

        public Boolean getDeployModel() {
            return deployModel;
        }

        public void setDeployModel(Boolean deployModel) {
            this.deployModel = deployModel;
        }

        public Boolean getUndeployModel() {
            return undeployModel;
        }

        public void setUndeployModel(Boolean undeployModel) {
            this.undeployModel = undeployModel;
        }

        public DeployModelParameter getModel() {
            return model;
        }

        public void setModel(DeployModelParameter model) {
            this.model = model;
        }

        private class DeployModelParameter implements Serializable {

            private String model;
            private String machineType;
            private Integer minReplicaCount;
            private Integer maxReplicaCount;

            public String getModel() {
                return model;
            }

            public void setModel(String model) {
                this.model = model;
            }

            public String getMachineType() {
                return machineType;
            }

            public void setMachineType(String machineType) {
                this.machineType = machineType;
            }

            public Integer getMinReplicaCount() {
                return minReplicaCount;
            }

            public void setMinReplicaCount(Integer minReplicaCount) {
                this.minReplicaCount = minReplicaCount;
            }

            public Integer getMaxReplicaCount() {
                return maxReplicaCount;
            }

            public void setMaxReplicaCount(Integer maxReplicaCount) {
                this.maxReplicaCount = maxReplicaCount;
            }

        }

    }

    private enum Type implements Serializable {
        table,
        vision,
        language,
        video
    }

    private enum Objective implements Serializable {
        classification,
        regression,
        forecasting
    }

    private enum EndpointType implements Serializable {
        managed,
        cloudrun,
        exported
    }



    public String getName() {
        return "automl";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {

        final AutoMLTransformParameters parameters = new Gson().fromJson(config.getParameters(), AutoMLTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final String failuresName = name + OUTPUT_SUFFIX_FAILURES;
            switch (input.getDataType()) {
                case ROW: {
                    final Schema outputSchema;
                    final Setter<Schema, Row> setter;
                    switch (parameters.getType()) {
                        case table: {
                            setter = (Schema s, Row r, Map<String, Object> values) -> {
                                var builder = RowSchemaUtil.toBuilder(s, r);
                                for(var value : values.entrySet()) {
                                    builder.withFieldValue(value.getKey(), value.getValue());
                                }
                                return builder.build();
                            };
                            switch (parameters.getObjective()) {
                                case classification: {
                                    outputSchema = RowSchemaUtil
                                            .toBuilder(input.getSchema())
                                            .addField(parameters.getPrefix() + "scores", Schema.FieldType.array(Schema.FieldType.DOUBLE).withNullable(true))
                                            .addField(parameters.getPrefix() + "classes", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
                                            .addField(parameters.getPrefix() + "deployedModelId", Schema.FieldType.STRING.withNullable(true))
                                            .build();
                                    break;
                                }
                                case regression: {
                                    outputSchema = RowSchemaUtil
                                            .toBuilder(input.getSchema())
                                            .addField(parameters.getPrefix() + "value", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "lower_bound", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "upper_bound", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "deployedModelId", Schema.FieldType.STRING.withNullable(true))
                                            .build();
                                    break;
                                }
                                case forecasting:
                                default: {
                                    throw new IllegalArgumentException("");
                                }
                            }
                            break;
                        }
                        case vision:
                        case language:
                        case video:
                        default: {
                            throw new IllegalArgumentException("");
                        }
                    }

                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row,Schema,Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            s -> s,
                            RowToJsonConverter::convertObject,
                            setter,
                            RowSchemaUtil::getAsString,
                            RowCoder.of(input.getSchema()));
                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> succeeded = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag);

                    results.put(name, FCollection.of(name, succeeded, DataType.ROW, outputSchema));
                    results.put(failuresName, FCollection.of(failuresName, failures, DataType.ROW, input.getSchema()));
                    break;
                }
                case AVRO: {
                    final org.apache.avro.Schema outputSchema;
                    final Setter<org.apache.avro.Schema, GenericRecord> setter;
                    switch (parameters.getType()) {
                        case table: {
                            setter = (org.apache.avro.Schema s, GenericRecord r, Map<String, Object> values) -> {
                                final GenericRecordBuilder builder = AvroSchemaUtil.toBuilder(s, r);
                                for(var value : values.entrySet()) {
                                    builder.set(value.getKey(), value.getValue());
                                }
                                return builder.build();
                            };
                            switch (parameters.getObjective()) {
                                case classification: {
                                    outputSchema = AvroSchemaUtil.toBuilder(input.getAvroSchema())
                                            .name(parameters.getPrefix() + "scores").type(AvroSchemaUtil.NULLABLE_ARRAY_DOUBLE_TYPE).withDefault(null)
                                            .name(parameters.getPrefix() + "classes").type(AvroSchemaUtil.NULLABLE_ARRAY_STRING_TYPE).withDefault(null)
                                            .name(parameters.getPrefix() + "deployedModelId").type(AvroSchemaUtil.NULLABLE_STRING).withDefault(null)
                                            .endRecord();
                                    break;
                                }
                                case regression: {
                                    outputSchema = AvroSchemaUtil.toBuilder(input.getAvroSchema())
                                            .name(parameters.getPrefix() + "value").type(AvroSchemaUtil.NULLABLE_DOUBLE).withDefault(null)
                                            .name(parameters.getPrefix() + "lower_bound").type(AvroSchemaUtil.NULLABLE_DOUBLE).withDefault(null)
                                            .name(parameters.getPrefix() + "upper_bound").type(AvroSchemaUtil.NULLABLE_DOUBLE).withDefault(null)
                                            .name(parameters.getPrefix() + "deployedModelId").type(AvroSchemaUtil.NULLABLE_STRING).withDefault(null)
                                            .endRecord();
                                    break;
                                }
                                case forecasting:
                                default: {
                                    throw new IllegalArgumentException("");
                                }
                            }
                            break;
                        }
                        case vision:
                        case language:
                        case video:
                        default: {
                            throw new IllegalArgumentException("");

                        }
                    }

                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroToJsonConverter::convertObject,
                            setter,
                            AvroSchemaUtil::getAsString,
                            AvroCoder.of(input.getAvroSchema()));

                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> succeeded = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag);

                    results.put(name, FCollection.of(name, succeeded, DataType.AVRO, outputSchema));
                    results.put(failuresName, FCollection.of(failuresName, failures, DataType.AVRO, input.getAvroSchema()));
                    break;
                }
                case STRUCT: {
                    final com.google.cloud.spanner.Type outputType;
                    final Setter<com.google.cloud.spanner.Type, Struct> setter;
                    switch (parameters.getType()) {
                        case table: {
                            setter = (com.google.cloud.spanner.Type s, Struct r, Map<String, Object> values) -> {
                                var builder = StructSchemaUtil.toBuilder(s, r);
                                for (var value : values.entrySet()) {
                                    if (value.getKey().contains("scores")) {
                                        builder.set(value.getKey()).toFloat64Array((List<Double>) value.getValue());
                                    } else if (value.getKey().contains("classes")) {
                                        builder.set(value.getKey()).toStringArray((List<String>) value.getValue());
                                    } else {
                                        builder.set(value.getKey()).to((Double) value.getValue());
                                    }
                                }
                                return builder.build();
                            };
                            final List<com.google.cloud.spanner.Type.StructField> structFields = input.getSpannerType().getStructFields();
                            switch (parameters.getObjective()) {
                                case classification: {
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "scores", com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.float64())));
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "classes", com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.string())));
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "deployedModelId", com.google.cloud.spanner.Type.string()));
                                    outputType = com.google.cloud.spanner.Type.struct(structFields);
                                    break;
                                }
                                case regression: {
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "value", com.google.cloud.spanner.Type.float64()));
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "lower_bound", com.google.cloud.spanner.Type.float64()));
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "upper_bound", com.google.cloud.spanner.Type.float64()));
                                    structFields.add(com.google.cloud.spanner.Type.StructField.of(parameters.getPrefix() + "deployedModelId", com.google.cloud.spanner.Type.string()));
                                    outputType = com.google.cloud.spanner.Type.struct(structFields);
                                    break;
                                }
                                case forecasting:
                                default: {
                                    throw new IllegalArgumentException("");
                                }
                            }
                            break;
                        }
                        case vision:
                        case language:
                        case video:
                        default: {
                            throw new IllegalArgumentException("");
                        }
                    }

                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct, com.google.cloud.spanner.Type, com.google.cloud.spanner.Type> transform = new Transform<>(
                            parameters,
                            outputType,
                            s -> s,
                            StructToJsonConverter::convertObject,
                            setter,
                            StructSchemaUtil::getAsString,
                            SerializableCoder.of(Struct.class));

                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> succeeded = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag);

                    results.put(name, FCollection.of(name, succeeded, DataType.STRUCT, outputType));
                    results.put(failuresName, FCollection.of(failuresName, failures, DataType.STRUCT, input.getSpannerType()));
                    break;
                }
                case ENTITY: {
                    final Schema outputSchema;
                    final Setter<Schema, Entity> setter;
                    switch (parameters.getType()) {
                        case table: {
                            setter = (Schema s, Entity r, Map<String, Object> values) -> {
                                var builder = EntitySchemaUtil.toBuilder(s, r);
                                for (var value : values.entrySet()) {
                                    if (value.getValue() == null) {
                                        builder.putProperties(value.getKey(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
                                        continue;
                                    }
                                    if (value.getKey().endsWith("scores")) {
                                        builder.putProperties(value.getKey(), Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                                .addAllValues(((List<Double>)value.getValue()).stream()
                                                        .map(d -> Value.newBuilder().setDoubleValue(d).build())
                                                        .collect(Collectors.toList()))
                                                .build()).build());
                                    } else if (value.getKey().endsWith("classes")) {
                                        builder.putProperties(value.getKey(), Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                                .addAllValues(((List<String>)value.getValue()).stream()
                                                        .map(d -> Value.newBuilder().setStringValue(d).build())
                                                        .collect(Collectors.toList()))
                                                .build()).build());
                                    } else if (value.getKey().endsWith("deployedModelId")) {
                                        builder.putProperties(value.getKey(), Value.newBuilder().setStringValue((String)value.getValue()).build());
                                    } else {
                                        builder.putProperties(value.getKey(), Value.newBuilder().setDoubleValue((Double)value.getValue()).build());
                                    }
                                }
                                return builder.build();
                            };

                            switch (parameters.getObjective()) {
                                case classification: {
                                    outputSchema = RowSchemaUtil
                                            .toBuilder(input.getSchema())
                                            .addField(parameters.getPrefix() + "scores", Schema.FieldType.array(Schema.FieldType.DOUBLE).withNullable(true))
                                            .addField(parameters.getPrefix() + "classes", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
                                            .addField(parameters.getPrefix() + "deployedModelId", Schema.FieldType.STRING.withNullable(true))
                                            .build();
                                    break;
                                }
                                case regression: {
                                    outputSchema = RowSchemaUtil
                                            .toBuilder(input.getSchema())
                                            .addField(parameters.getPrefix() + "value", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "lower_bound", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "upper_bound", Schema.FieldType.DOUBLE.withNullable(true))
                                            .addField(parameters.getPrefix() + "deployedModelId", Schema.FieldType.STRING.withNullable(true))
                                            .build();
                                    break;
                                }
                                case forecasting:
                                default: {
                                    throw new IllegalArgumentException("");
                                }
                            }
                            break;
                        }
                        case vision:
                        case language:
                        case video:
                        default: {
                            throw new IllegalArgumentException("");
                        }
                    }

                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity, Schema, Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            s -> s,
                            EntityToJsonConverter::convertObject,
                            setter,
                            EntitySchemaUtil::getAsString,
                            SerializableCoder.of(Entity.class));

                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> succeeded = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag);

                    results.put(name, FCollection.of(name, succeeded, DataType.ENTITY, outputSchema));
                    results.put(failuresName, FCollection.of(failuresName, failures, DataType.ENTITY, input.getSchema()));
                    break;
                }
                default: {
                    throw new IllegalArgumentException("");
                }
            }
        }

        return results;
    }

    private static void validateParameters(final AutoMLTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("AutoMLTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if (parameters.getType() == null) {
            errorMessages.add("AutoMLTransform config parameters must contain `type` parameter.");
        }
        if (parameters.getObjective() == null) {
            errorMessages.add("AutoMLTransform config parameters must contain `objective` parameter.");
        }
        if (parameters.getEndpoint() == null) {
            errorMessages.add("AutoMLTransform config parameters must contain `endpoint` parameter.");
        }
        if (parameters.getBatchSize() != null && parameters.getBatchSize() < 0) {
            errorMessages.add("AutoMLTransform config parameters `batchSize` must be over zero.");
        }
        if (parameters.getMaxPeriodSecond() != null && parameters.getMaxPeriodSecond() < 0) {
            errorMessages.add("AutoMLTransform config parameters `maxPeriodSecond` must be over zero.");
        }
        if (parameters.getParallelNum() != null && parameters.getParallelNum() < 0) {
            errorMessages.add("AutoMLTransform config parameters `parallelNum` must be over zero.");
        }
        if (parameters.getDeployModel() != null && parameters.getDeployModel()) {
            if (parameters.getModel() == null) {
                errorMessages.add("AutoMLTransform config parameter `model` is required if `deployModel` is true.");
            } else {
                if (parameters.getModel().getModel() == null) {
                    errorMessages.add("AutoMLTransform config parameter `model.model` is required if `deployModel` is true.");
                }
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(AutoMLTransformParameters parameters) {
        if (parameters.getEndpointType() == null) {
            parameters.setEndpointType(EndpointType.managed);
        }
        if (parameters.getPrefix() == null) {
            parameters.setPrefix("");
        }
        if (parameters.getBatchSize() == null) {
            parameters.setBatchSize(32);
        }
        if (parameters.getMaxPeriodSecond() == null) {
            parameters.setMaxPeriodSecond(10L);
        }
        if (parameters.getConnectTimeoutSecond() == null) {
            parameters.setConnectTimeoutSecond(60L);
        }
        if (parameters.getGroupFields() == null) {
            parameters.setGroupFields(new ArrayList<>());
        }
        if (parameters.getFailFast() == null) {
            parameters.setFailFast(true);
        }
        if (parameters.getParallelNum() == null) {
            parameters.setParallelNum(0);
        }
        if (parameters.getDeployModel() == null) {
            parameters.setDeployModel(false);
        }
        if (parameters.getUndeployModel() == null) {
            parameters.setUndeployModel(false);
        }
        if (parameters.getModel() != null) {
            if (parameters.getModel().getMachineType() == null) {
                parameters.getModel().setMachineType("n1-standard-2");
            }
            if (parameters.getModel().getMinReplicaCount() == null) {
                parameters.getModel().setMinReplicaCount(1);
            }
            if (parameters.getModel().getMaxReplicaCount() == null) {
                parameters.getModel().setMaxReplicaCount(parameters.getModel().getMinReplicaCount());
            }
        }
    }

    public static class Transform<T,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final TupleTag<T> outputTag;
        private final TupleTag<T> failuresTag;

        private final AutoMLTransformParameters parameters;
        private final InputSchemaT outputSchema;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final JsonConverter<T> jsonConverter;
        private final Setter<RuntimeSchemaT, T> setter;
        private final StringGetter<T> stringGetter;
        private final Coder<T> coder;

        private Transform(final AutoMLTransformParameters parameters,
                          final InputSchemaT outputSchema,
                          final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                          final JsonConverter<T> jsonConverter,
                          final Setter<RuntimeSchemaT, T> setter,
                          final StringGetter<T> stringGetter,
                          final Coder<T> coder) {

            this.parameters = parameters;
            this.outputSchema = outputSchema;
            this.schemaConverter = schemaConverter;
            this.jsonConverter = jsonConverter;
            this.setter = setter;
            this.stringGetter = stringGetter;
            this.coder = coder;

            this.outputTag = new TupleTag<>("output"){};
            this.failuresTag = new TupleTag<>("failures"){};
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {

            final PCollection<KV<String, T>> withKey;
            if (parameters.getGroupFields().size() > 0) {
                final List<String> groupFields = parameters.getGroupFields();
                withKey = input.apply("WithKey", WithKeys.of((T t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for (String field : groupFields) {
                        final String key = stringGetter.getAsString(t, field);
                        sb.append(key == null ? "" : key);
                        sb.append("#");
                    }
                    if (sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                }).withKeyType(TypeDescriptors.strings()));
            } else if(parameters.getParallelNum() > 1) {
                withKey = input
                        .apply("AssignRandomKey", ParDo.of(new AssignPartitionDoFn(parameters.getParallelNum())))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            } else {
                withKey = input
                        .apply("WithFixedKey", WithKeys.of(""))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            }

            final PCollection<KV<String, T>> instances;
            if (parameters.getDeployModel()) {
                final PCollection<String> wait = input.getPipeline()
                        .apply("SeedDeploy", Create.of(""))
                        .apply("DeployModel", ParDo.of(new DeployEndpointDoFn(
                                parameters.getEndpoint(),
                                parameters.getModel().getModel(),
                                parameters.getModel().getMachineType(),
                                parameters.getModel().getMinReplicaCount(),
                                parameters.getModel().getMaxReplicaCount())));
                instances = withKey
                        .apply("WaitOnDeployment", Wait.on(wait))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            } else {
                instances = withKey;
            }

            final Boolean isGCP = !OptionUtil.isDirectRunner(input);
            final PCollectionTuple predictions;
            if (parameters.getBatchSize() > 1) {
                predictions = instances.apply("Predict", ParDo
                        .of(new PredictBufferDoFn(
                                parameters.getEndpoint(), parameters.getType(),
                                parameters.getObjective(), parameters.getEndpointType(),
                                parameters.getPrefix(), parameters.getConnectTimeoutSecond(), parameters.getFailFast(),
                                schemaConverter, jsonConverter, setter, outputSchema,
                                parameters.getBatchSize(), parameters.getMaxPeriodSecond(), coder, isGCP))
                        .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            } else {
                predictions = instances.apply("Predict", ParDo
                        .of(new PredictSingleDoFn(
                                parameters.getEndpoint(), parameters.getType(),
                                parameters.getObjective(), parameters.getEndpointType(),
                                parameters.getPrefix(), parameters.getConnectTimeoutSecond(), parameters.getFailFast(),
                                schemaConverter, jsonConverter, setter, outputSchema, isGCP))
                        .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            }

            if (parameters.getUndeployModel() && !OptionUtil.isStreaming(input.getPipeline().getOptions())) {
                input.getPipeline()
                        .apply("SeedUndeploy", Create.of(""))
                        .apply("WaitOnPrediction", Wait.on(predictions.get(outputTag)))
                        .setCoder(StringUtf8Coder.of())
                        .apply("UndeployModel", ParDo.of(new DeployEndpointDoFn(parameters.getEndpoint())));
            }

            return predictions;
        }

        private abstract class PredictDoFn extends DoFn<KV<String, T>, T> {

            private final String endpoint;
            private final Type type;
            private final Objective objective;
            private final EndpointType endpointType;
            private final String prefix;
            private final Long connectTimeoutSecond;

            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final JsonConverter<T> jsonConverter;
            private final Setter<RuntimeSchemaT, T> setter;
            private final InputSchemaT inputSchema;

            private final Boolean isGCP;

            private transient HttpClient client;
            private transient RuntimeSchemaT outputSchema;
            private transient AccessToken accessToken;

            PredictDoFn(final String endpoint,
                        final Type type,
                        final Objective objective,
                        final EndpointType endpointType,
                        final String prefix,
                        final Long connectTimeoutSecond,
                        final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                        final JsonConverter<T> jsonConverter,
                        final Setter<RuntimeSchemaT, T> setter,
                        final InputSchemaT inputSchema,
                        final Boolean isGCP) {

                this.endpoint = endpoint;
                this.type = type;
                this.objective = objective;
                this.endpointType = endpointType;
                this.prefix = prefix;
                this.connectTimeoutSecond = connectTimeoutSecond;
                this.schemaConverter = schemaConverter;
                this.jsonConverter = jsonConverter;
                this.setter = setter;
                this.inputSchema = inputSchema;
                this.isGCP = isGCP;
            }

            void setup() throws IOException {
                this.client = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(connectTimeoutSecond))
                        .build();
                this.outputSchema = schemaConverter.convert(inputSchema);

                if(!isGCP || EndpointType.managed.equals(endpointType)) {
                    LOG.info("Create access token.");
                    accessToken = IAMUtil.getAccessToken();
                }
                LOG.info("Setup predictor worker. ThreadID: " + Thread.currentThread().getId());
            }

            void teardown() {
                LOG.info("Teardown predictor worker. ThreadID: " + Thread.currentThread().getId());
            }

            void startupBundle() {

            }

            List<T> send(final List<T> elements)
                    throws IOException, URISyntaxException, InterruptedException {

                final JsonArray instances = new JsonArray();
                for(final T element : elements) {
                    final JsonObject instance = jsonConverter.convertObject(element);
                    instances.add(instance);
                }

                final String token;
                if(!isGCP || EndpointType.managed.equals(endpointType)) {
                    if(accessToken.getExpirationTime().before(new Date())) {
                        LOG.info("Refreshed access token with expirationTime: " + accessToken.getExpirationTime());
                        accessToken = IAMUtil.getAccessToken();
                    }
                    token = accessToken.getTokenValue();
                } else {
                    token = IAMUtil.getIdToken(client, endpoint);
                }

                final JsonObject response = VertexAIUtil.predict(client, token, endpoint, instances);
                final JsonArray predictions = response.getAsJsonArray("predictions");
                final String deployedModelId = response.getAsJsonPrimitive("deployedModelId").getAsString();

                final List<T> results = new ArrayList<>();
                for(int i=0; i<elements.size(); i++) {
                    final T element = elements.get(i);
                    final JsonObject prediction = predictions.get(i).getAsJsonObject();
                    final T result = merge(element, prediction, deployedModelId);
                    results.add(result);
                }

                return results;
            }

            private T merge(final T element, final JsonObject prediction, final String deployedModelId) {
                final Map<String, Object> data = new HashMap<>();
                data.put(prefix + "deployedModelId", deployedModelId);
                switch (type) {
                    case table: {
                        switch (objective) {
                            case classification: {
                                final List<Double> scores = new ArrayList<>();
                                final JsonArray jsonScores = prediction.getAsJsonArray("scores");
                                for(final JsonElement score : jsonScores) {
                                    scores.add(score.getAsDouble());
                                }
                                data.put(prefix + "scores", scores);
                                final List<String> classes = new ArrayList<>();
                                final JsonArray jsonClasses = prediction.getAsJsonArray("classes");
                                for(final JsonElement cls : jsonClasses) {
                                    classes.add(cls.getAsString());
                                }
                                data.put(prefix + "classes", classes);
                                break;
                            }
                            case regression: {
                                data.put(prefix + "value", prediction.getAsJsonPrimitive("value").getAsDouble());
                                data.put(prefix + "lower_bound", prediction.getAsJsonPrimitive("lower_bound").getAsDouble());
                                data.put(prefix + "upper_bound", prediction.getAsJsonPrimitive("upper_bound").getAsDouble());
                                break;
                            }
                            case forecasting:
                            default: {
                                throw new IllegalArgumentException("Not supported objective: " + objective + " for type: " + type);
                            }
                        }
                        break;
                    }
                    case vision:
                    case language:
                    case video:
                    default: {
                        throw new IllegalArgumentException("Not supported type: " + type);
                    }
                }

                return setter.setValue(outputSchema, element, data);
            }

        }

        private class PredictSingleDoFn extends PredictDoFn {

            private final Boolean failFast;

            PredictSingleDoFn(final String endpoint,
                              final Type type,
                              final Objective objective,
                              final EndpointType endpointType,
                              final String prefix,
                              final Long connectTimeoutSecond,
                              final Boolean failFast,
                              final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                              final JsonConverter<T> jsonConverter,
                              final Setter<RuntimeSchemaT, T> setter,
                              final InputSchemaT inputSchema,
                              final Boolean isGCP) {

                super(endpoint, type, objective, endpointType, prefix, connectTimeoutSecond, schemaConverter, jsonConverter, setter, inputSchema, isGCP);
                this.failFast = failFast;
            }

            @Setup
            public void setup() throws IOException {
                super.setup();
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

            @StartBundle
            public void startBundle(StartBundleContext c) {
                super.startupBundle();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws Exception {
                final List<T> elements = new ArrayList<>();
                elements.add(c.element().getValue());
                try {
                    final List<T> results = send(elements);
                    for (final T result : results) {
                        c.output(result);
                    }
                } catch (IllegalStateException e) {
                    LOG.error("Failed to send request: " + e.getMessage());
                    if(failFast) {
                        throw  e;
                    }
                    for (final T element : elements) {
                        c.output(failuresTag, element);
                    }
                }
            }

        }

        private class PredictBufferDoFn extends PredictDoFn {

            private static final String STATEID_VALUES = "bufferedInstances";
            private static final String TIMERID_VALUES = "bufferedInstanceTimer";

            private final Boolean failFast;
            private final Integer batchSize;
            private final Long maxPeriodSecond;

            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<List<T>>> values;

            @TimerId(TIMERID_VALUES)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            PredictBufferDoFn(final String endpoint,
                              final Type type,
                              final Objective objective,
                              final EndpointType endpointType,
                              final String prefix,
                              final Long connectTimeoutSecond,
                              final Boolean failFast,
                              final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                              final JsonConverter<T> jsonConverter,
                              final Setter<RuntimeSchemaT, T> setter,
                              final InputSchemaT inputSchema,
                              final Integer batchSize,
                              final Long maxPeriodSecond,
                              final Coder<T> coder,
                              final Boolean isGCP) {

                super(endpoint, type, objective, endpointType, prefix, connectTimeoutSecond, schemaConverter, jsonConverter, setter, inputSchema, isGCP);
                this.batchSize = batchSize;
                this.maxPeriodSecond = maxPeriodSecond;
                this.failFast = failFast;
                this.values = StateSpecs.value(ListCoder.of(coder));
            }

            @Setup
            public void setup() throws IOException  {
                super.setup();
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

            @StartBundle
            public void startBundle(StartBundleContext c) {
                super.startupBundle();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_VALUES) ValueState<List<T>> valuesState,
                                       final @TimerId(TIMERID_VALUES) Timer timer) throws Exception {

                final List<T> buffer = Optional.ofNullable(valuesState.read()).orElse(new ArrayList<>());
                buffer.add(c.element().getValue());

                if(buffer.size() >= batchSize) {
                    try {
                        final List<T> results = send(buffer);
                        for (final T result : results) {
                            c.output(result);
                        }
                        buffer.clear();
                        valuesState.write(buffer);
                        timer.set(c.timestamp().plus(org.joda.time.Duration.standardSeconds(maxPeriodSecond)));
                    } catch (IllegalStateException e) {
                        LOG.error("Failed to send request: " + e.getMessage());
                        if(failFast) {
                            buffer.clear();
                            valuesState.write(buffer);
                            throw  e;
                        }
                        for (final T element : buffer) {
                            c.output(failuresTag, element);
                        }
                        buffer.clear();
                        valuesState.write(buffer);
                    }
                } else {
                    valuesState.write(buffer);
                }
            }

            @OnTimer(TIMERID_VALUES)
            public void onTimer(final OnTimerContext c,
                                final @StateId(STATEID_VALUES) ValueState<List<T>> valuesState) {

                final List<T> buffer = Optional.ofNullable(valuesState.read()).orElse(new ArrayList<>());
                if(buffer.size() > 0) {
                    try {
                        final List<T> results = send(buffer);
                        for (final T result : results) {
                            c.output(result);
                        }
                    } catch (IllegalStateException e) {
                        if(failFast) {
                            throw  e;
                        }
                        for (final T element : buffer) {
                            c.output(failuresTag, element);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

        }

        private class AssignPartitionDoFn extends DoFn<T, KV<String,T>> {

            private final Integer num;

            private transient Random random;

            AssignPartitionDoFn(final Integer num) {
                this.num = num;
            }

            @Setup
            public void setup() {
                random = new Random();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws Exception {
                final String key = Integer.toString(random.nextInt(num));
                c.output(KV.of(key, c.element()));
            }

        }

        private class DeployEndpointDoFn extends DoFn<String, String> {

            private final Boolean on;
            private final String endpoint;
            private final String model;

            // for deploy only
            private final String machineType;
            private final Integer minReplicaCount;
            private final Integer maxReplicaCount;

            private final String endpointRegion;

            private transient HttpClient client;
            private transient String accessToken;

            DeployEndpointDoFn(final String endpoint, final String model,
                               final String machineType, final Integer minReplicaCount, final Integer maxReplicaCount) {
                this.on = true;
                this.endpoint = endpoint;
                this.model = model;
                this.machineType = machineType;
                this.minReplicaCount = minReplicaCount;
                this.maxReplicaCount = maxReplicaCount;
                this.endpointRegion = VertexAIUtil.getEndpointRegion(endpoint);
            }

            DeployEndpointDoFn(final String endpoint) {
                this.on = false;
                this.endpoint = endpoint;
                this.model = null;
                this.machineType = null;
                this.minReplicaCount = null;
                this.maxReplicaCount = null;
                this.endpointRegion = null;
            }

            @Setup
            public void setup() throws IOException {
                this.client = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(60))
                        .build();
                this.accessToken = IAMUtil.getAccessToken().getTokenValue();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws Exception {
                if(on) {
                    final String operationName = VertexAIUtil.deploy(
                            client, accessToken, endpoint, model, machineType, minReplicaCount, maxReplicaCount);
                    long second = 0;
                    while(!VertexAIUtil.isOperationDone(client, accessToken, endpointRegion, operationName) && second < 1800) {
                        Thread.sleep(10000L);
                        second += 10;
                        LOG.info("Waiting for " + second + " seconds to finish deploy operation: " + operationName);
                    }
                    LOG.info("Finished to deploy model: " + model + " for endpoint: " + endpoint + " (" + second + " seconds)");
                } else {
                    final List<String> deployedModelIds = VertexAIUtil.getDeployedModelIds(client, accessToken, endpoint);
                    for(final String deployedModelId : deployedModelIds) {
                        VertexAIUtil.undeploy(client, accessToken, endpoint, deployedModelId);
                        LOG.info("Undeploy model: " + deployedModelId + " for endpoint: " + endpoint);
                    }
                    LOG.info("Finished to undeploy model: " + model + " for endpoint: " + endpoint);
                }

                c.output("ok");
            }

        }

    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(final T value, final String field);
    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface JsonConverter<T> extends Serializable {
        JsonObject convertObject(T element);
    }

    private interface Setter<SchemaT,T> extends Serializable {
        T setValue(final SchemaT schema, final T element, final Map<String, Object> children);
    }

}
