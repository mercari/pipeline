package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.ElementToOnnxConverter;
import com.mercari.solution.util.schema.converter.OnnxToElementConverter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.*;


@Transform.Module(name="onnx")
public class ONNXTransform extends Transform {

    private static class Parameters implements Serializable {

        private ModelParameter model;
        private List<InferenceParameter> inferences;

        private Integer bufferSize;
        private Integer bufferIntervalSeconds;

        private List<String> groupFields;

        public void validate(final Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(model == null) {
                errorMessages.add("parameters.model must not be null");
            } else {
                errorMessages.addAll(model.validate(inputSchemas));
            }
            if(inferences != null) {
                for(int index=0; index<inferences.size(); index++) {
                    errorMessages.addAll(inferences.get(index).validate(index, inputSchemas));
                }
            }

            if(bufferSize != null && bufferSize < 1) {
                errorMessages.add("parameters.bufferSize must be over than zero");
            }
            if(bufferIntervalSeconds != null && bufferIntervalSeconds < 1) {
                errorMessages.add("parameters.bufferIntervalSeconds must be over than zero");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            model.setDefaults();
            if(inferences == null) {
                inferences = new ArrayList<>();
            } else {
                for(InferenceParameter inference : inferences) {
                    inference.setDefaults();
                }
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }

            if(bufferSize == null) {
                bufferSize = 1;
            }
            if(bufferIntervalSeconds == null) {
                bufferIntervalSeconds = 1;
            }

        }

    }

    private static class ModelParameter implements Serializable {

        private String path;
        private OrtSession.SessionOptions.OptLevel optLevel;
        private OrtSession.SessionOptions.ExecutionMode executionMode;
        private JsonElement outputSchemaFields;

        public List<String> validate(Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.path == null) {
                errorMessages.add("parameters.model.path parameter must not be null");
            } else if(!path.startsWith("gs://")) {
                errorMessages.add("parameters.model.path must start with gs://");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(optLevel == null) {
                optLevel = OrtSession.SessionOptions.OptLevel.BASIC_OPT;
            }
            if(executionMode == null) {
                executionMode = OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL;
            }
        }

        public ModelSetting toSettings() {
            final ModelSetting setting = new ModelSetting();
            setting.path = this.path;
            setting.optLevel = this.optLevel;
            setting.executionMode = this.executionMode;
            if(outputSchemaFields != null && outputSchemaFields.isJsonArray()) {
                setting.outputSchemaFieldsJson = outputSchemaFields.toString();
            }
            return setting;
        }

    }

    private static class InferenceParameter implements Serializable {

        private String input;
        private List<MappingParameter> mappings;
        private JsonArray select;

        public Schema mergeSchema(final Schema inputSchema, final Schema onnxOutputSchema) {
            final Schema schema = mergeOnnxSchema(inputSchema, onnxOutputSchema);
            if(select == null || select.isJsonNull() || select.isEmpty()) {
                return schema;
            }
            return SelectFunction.createSchema(select, schema.getFields());
        }

        public Schema mergeOnnxSchema(final Schema inputSchema, final Schema onnxOutputSchema) {
            final List<Schema.Field> outputFields = new ArrayList<>();
            for(final Schema.Field field : inputSchema.getFields()) {
                outputFields.add(Schema.Field.of(field.getName(), field.getFieldType()));
            }
            if(!mappings.isEmpty()) {
                for(final MappingParameter mapping : mappings) {
                    for(final Map.Entry<String, String> output : mapping.outputs.entrySet()) {
                        final String onnxOutputFieldName = output.getKey();
                        final String outputFieldName = output.getValue();
                        if(!onnxOutputSchema.hasField(onnxOutputFieldName)) {
                            throw new IllegalStateException("outputs.key: " + onnxOutputFieldName + " is missing in onnx output schema: " + onnxOutputSchema);
                        }
                        final Schema.Field field = onnxOutputSchema.getField(onnxOutputFieldName);
                        outputFields.add(Schema.Field
                                .of(outputFieldName, field.getFieldType().withNullable(field.getFieldType().getNullable()))
                                .withOptions(field.getOptions()));
                    }
                }
            } else {
                outputFields.addAll(onnxOutputSchema.getFields());
            }
            return Schema.builder()
                    .withFields(outputFields)
                    .build();
        }

        public InferenceSetting toSetting(final List<Schema.Field> inputFields) {
            final InferenceSetting inferenceSetting = new InferenceSetting();
            inferenceSetting.input = this.input;
            inferenceSetting.mappings = this.mappings;

            if(this.select != null) {
                inferenceSetting.selectFunctions = SelectFunction.of(this.select, inputFields);
            } else {
                inferenceSetting.selectFunctions = new ArrayList<>();
            }

            return inferenceSetting;
        }

        public List<String> validate(int index, Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("parameters.inferences[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(mappings == null) {
                mappings = new ArrayList<>();
            }
        }

        public static InferenceParameter defaultInference(final String input) {
            final InferenceParameter inferenceParameter = new InferenceParameter();
            inferenceParameter.input = input;
            inferenceParameter.setDefaults();
            return inferenceParameter;
        }

    }

    private static class MappingParameter implements Serializable {

        private Map<String, String> inputs;
        private Map<String, String> outputs;

        public List<String> validate(int index, Map<String, KV<DataType, Schema>> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.inputs == null) {
                errorMessages.add("parameters.inferences[" + index + "].inputs parameter must not be null.");
            }
            if(this.outputs == null) {
                errorMessages.add("parameters.inferences[" + index + "].outputs parameter must not be null.");
            }
            return errorMessages;
        }

    }

    private static class ModelSetting implements Serializable {

        private String path;
        private OrtSession.SessionOptions.OptLevel optLevel;
        private OrtSession.SessionOptions.ExecutionMode executionMode;
        private String outputSchemaFieldsJson;

        private transient List<Schema.Field> outputSchemaFields;

        public void setup() {
            this.outputSchemaFields = new ArrayList<>();
            if(outputSchemaFieldsJson != null) {
                final JsonElement element = new Gson().fromJson(outputSchemaFieldsJson, JsonElement.class);
                if(!element.isJsonArray()) {
                    throw new IllegalArgumentException();
                }
                final JsonArray array = element.getAsJsonArray();
                for(final JsonElement fieldElement : array) {
                    final Schema.Field field = Schema.Field.parse(fieldElement.getAsJsonObject());
                    this.outputSchemaFields.add(field);
                }
            }
        }

        public Schema getModelOutputSchema() {
            if (outputSchemaFields != null && !outputSchemaFields.isEmpty()) {
                return Schema.of(outputSchemaFields);
            } else {
                final KV<Map<String, NodeInfo>,Map<String,NodeInfo>> nodesInfo = ONNXRuntimeUtil.getNodesInfo(path);
                return OnnxToElementConverter.convertOutputSchema(nodesInfo.getValue(), null);
            }
        }

    }

    private static class InferenceSetting implements Serializable {

        private String input;
        private List<MappingParameter> mappings;
        private List<SelectFunction> selectFunctions;

        public void setup() {
            if(selectFunctions != null) {
                for(final SelectFunction selectFunction : selectFunctions) {
                    selectFunction.setup();
                }
            }
        }

    }

    @NonNull
    @Override
    public MCollectionTuple expand(
            @NonNull MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(new HashMap<>()); // TODO
        parameters.setDefaults();

        final ModelSetting model = parameters.model.toSettings();
        model.setup();
        final Schema outputOnnxSchema = model.getModelOutputSchema();

        final List<String> inputNames = new ArrayList<>();
        final List<TupleTag<MElement>> outputTags = new ArrayList<>();
        final List<Schema> outputSchemas = new ArrayList<>();
        final List<InferenceSetting> inferenceSettings = new ArrayList<>();
        int index = 0;
        for (final String inputName : inputs.getAllInputs()){
            final TupleTag<MElement> outputTag = new TupleTag<>() {};
            final Schema inputSchema = inputs.getSchema(inputName);
            final InferenceParameter inference = parameters.inferences.get(index);
            final Schema outputMediumSchema = inference.mergeOnnxSchema(inputSchema, outputOnnxSchema);
            final Schema outputSchema = inference.mergeSchema(inputSchema, outputOnnxSchema);

            inputNames.add(inputName);
            outputTags.add(outputTag);
            outputSchemas.add(outputSchema);
            LOG.info("outputSchema::: " + outputSchema.getAvroSchema());
            inferenceSettings.add(inference.toSetting(outputMediumSchema.getFields()));
        }

        final TupleTag<MElement> dummyTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final TupleTagList tupleTagList = TupleTagList.of(new ArrayList<>(outputTags));

        final PCollectionTuple outputs = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("Inference", ParDo.of(new InferenceDoFn(
                                getJobName(), getName(), model, inferenceSettings, parameters.bufferSize,
                                inputs.getAllInputs(), outputSchemas,
                                outputTags, failureTag, getFailFast()))
                        .withOutputTags(dummyTag, tupleTagList));

        MCollectionTuple outputTuple = MCollectionTuple.empty(inputs.getPipeline());
        for(int i=0; i<inputNames.size(); i++) {
            outputTuple = outputTuple
                    .and(inputNames.get(i), outputs.get(outputTags.get(i)), outputSchemas.get(i));
        }

        errorHandler.addError(outputs.get(failureTag));

        return outputTuple;
    }

    private static class InferenceDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final ModelSetting model;
        private final List<InferenceSetting> inferences;
        private final Integer bufferSize;

        private final List<String> inputNames;
        private final List<Schema> outputSchemas;
        private final Schema outputFailureSchema = MFailure.schema();

        private final int maxLevel;
        private final List<List<MappingParameter>> mappingsList;

        private final List<TupleTag<MElement>> outputTags;
        private final TupleTag<BadRecord> failureTag;
        private final boolean failFast;

        private transient OrtEnvironment environment;
        private static final Map<String, OrtSession> sessions = new HashMap<>();// Collections.synchronizedMap(new HashMap<>());

        private transient List<MElement> buffer;


        public InferenceDoFn(
                final String jobName,
                final String name,
                final ModelSetting model,
                final List<InferenceSetting> inferences,
                final Integer bufferSize,
                final List<String> inputNames,
                final List<Schema> outputSchemas,
                final List<TupleTag<MElement>> outputTags,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.model = model;
            this.inferences = inferences;
            this.bufferSize = bufferSize;

            this.inputNames = inputNames;
            this.outputSchemas = outputSchemas;

            this.outputTags = outputTags;
            this.failureTag = failureTag;
            this.failFast = failFast;

            this.maxLevel = inferences.stream()
                    .map(i -> i.mappings.size())
                    .max(Integer::compareTo)
                    .orElse(1);

            this.mappingsList = new ArrayList<>();
            for(int level=0; level<maxLevel; level++) {
                final List<MappingParameter> mappings = new ArrayList<>();
                for (int inputIndex = 0; inputIndex < inferences.size(); inputIndex++) {
                    final InferenceSetting inference = inferences.get(inputIndex);
                    final MappingParameter mapping;
                    if (inference.mappings.size() > level) {
                        mapping = inference.mappings.get(level);
                    } else {
                        mapping = null;
                    }
                    mappings.add(mapping);
                }
                this.mappingsList.add(mappings);
            }
        }

        @Setup
        public void setup() throws OrtException {
            LOG.info("setup onnx module: " + name + ", inference thread: " + Thread.currentThread().getId());
            for(final Schema schema : outputSchemas) {
                schema.setup();
            }

            this.environment = OrtEnvironment.getEnvironment();
            createSession(name, model, environment);

            this.buffer = new ArrayList<>();

            for(final InferenceSetting inference : inferences) {
                inference.setup();
            }
        }

        @Teardown
        public void teardown() {
            LOG.info("teardown onnx module: " + name + ", inference thread: " + Thread.currentThread().getId());
            closeSession(name);
        }

        /*
        @StartBundle
        public void startBundle(final StartBundleContext c) {
            this.buffer.clear();
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) throws OrtException {
            if(buffer.size() > 0) {
                inference(buffer, receiver);
                buffer.clear();
            }
        }
         */

        @ProcessElement
        public void processElement(final ProcessContext c) {

            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                /*
                buffer.add(element);
                if(buffer.size() >= bufferSize) {
                    inference(buffer, receiver);
                    buffer.clear();
                }
                 */
                inference(c, List.of(input));
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to inference onnx", input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private void inference(
                final ProcessContext c,
                final List<MElement> elements)
                throws OrtException {

            final List<Map<String,Object>> updatesList = elements.stream()
                    .map(MElement::asPrimitiveMap)
                    .toList();
            for(int level=0; level<maxLevel; level++) {
                final List<MappingParameter> mappings = mappingsList.get(level);

                final Map<Integer,Integer> unionValueIndexMap = new HashMap<>();
                final List<MElement> targetUnionValues = new ArrayList<>();
                final List<Map<String,String>> renameFieldsList = new ArrayList<>();
                final Set<String> requestedOutputs = new HashSet<>();
                for(int index=0; index<elements.size(); index++) {
                    final MElement unionValue = elements.get(index);
                    final MappingParameter mapping = mappings.get(unionValue.getIndex());
                    if(mapping != null) {
                        final int targetUnionValueIndex = targetUnionValues.size();
                        unionValueIndexMap.put(targetUnionValueIndex, index);
                        targetUnionValues.add(unionValue);
                        renameFieldsList.add(mapping.inputs);
                        requestedOutputs.addAll(mapping.outputs.keySet());
                    }
                }

                final OrtSession session = getOrCreateSession(name, model, environment);
                final Map<String, OnnxTensor> inputs = ElementToOnnxConverter.convert(environment, session.getInputInfo(), targetUnionValues, renameFieldsList);
                try(final OrtSession.Result result = session.run(inputs, requestedOutputs)) {
                    final List<Map<String,Object>> updates = OnnxToElementConverter.convert(result);
                    for(int targetUnionValueIndex=0; targetUnionValueIndex<targetUnionValues.size(); targetUnionValueIndex++) {
                        final Map<String, Object> values = updates.get(targetUnionValueIndex);
                        final Integer unionValueIndex = unionValueIndexMap.get(targetUnionValueIndex);
                        final int inputIndex = elements.get(unionValueIndex).getIndex();
                        final MappingParameter mapping = mappings.get(inputIndex);
                        if(mapping == null || mapping.outputs == null) {
                            updatesList.get(unionValueIndex).putAll(values);
                        } else {
                            final Map<String, Object> renamedValues = new HashMap<>();
                            for(Map.Entry<String, Object> entry : values.entrySet()) {
                                final String field = mapping.outputs.getOrDefault(entry.getKey(), entry.getKey());
                                renamedValues.put(field, entry.getValue());
                            }
                            updatesList.get(unionValueIndex).putAll(renamedValues);
                        }
                    }
                }
            }

            for(int elementIndex=0; elementIndex<elements.size(); elementIndex++) {
                final MElement element = elements.get(elementIndex);
                final TupleTag<MElement> outputTag = outputTags.get(element.getIndex());
                final InferenceSetting inference = inferences.get(element.getIndex());
                final Schema outputSchema = outputSchemas.get(element.getIndex());
                Map<String, Object> values = updatesList.get(elementIndex);
                if(inference.selectFunctions != null && !inference.selectFunctions.isEmpty()) {
                    values = SelectFunction.apply(inference.selectFunctions, values, c.timestamp());
                }
                final MElement output = MElement.of(outputSchema, values, c.timestamp());

                c.output(outputTag, output);
            }
        }

        synchronized static private OrtSession getOrCreateSession(
                final String name,
                final ModelSetting model,
                final OrtEnvironment environment) throws OrtException {

            if(sessions.containsKey(name)) {
                final OrtSession session = sessions.get(name);
                if(session != null) {
                    return session;
                } else {
                    sessions.remove(name);
                }
            }
            createSession(name, model, environment);
            return sessions.get(name);
        }

        synchronized static private void createSession(
                final String name,
                final ModelSetting model,
                final OrtEnvironment environment) throws OrtException {

            if(sessions.containsKey(name) && sessions.get(name) == null) {
                sessions.remove(name);
            }

            if(!sessions.containsKey(name)) {
                try (final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {

                    sessionOptions.setOptimizationLevel(model.optLevel);
                    sessionOptions.setExecutionMode(model.executionMode);
                    try {
                        sessionOptions.registerCustomOpLibrary(ONNXRuntimeUtil.getExtensionsLibraryPath());
                    } catch (Throwable e) {
                        LOG.warn("setup onnx module: " + name + " is creating session. skipped to load extensions library");
                    }

                    final byte[] bytes = StorageUtil.readBytes(model.path);
                    final OrtSession session = environment.createSession(bytes, sessionOptions);
                    sessions.put(name, session);
                    LOG.info("setup onnx module: " + name + " created session");
                    LOG.info("onnx model: " + ONNXRuntimeUtil.getModelDescription(session));
                }
            } else {
                LOG.info("setup onnx module: " + name + " skipped creating session");
            }
        }

        synchronized static private void closeSession(final String name) {
            if(sessions.containsKey(name)) {
                try(final OrtSession session = sessions.remove(name);) {
                    LOG.info("teardown onnx module: " + name + " closed session");
                } catch (final OrtException e) {
                    LOG.error("teardown onnx module: " + name + " failed to close session cause: " + e.getMessage());
                }
            }
        }

    }

}
