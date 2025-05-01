package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.schema.converter.RowToRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ProcessingTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTransform.class);

    private static final String FIELD_SOURCE = "___sourceName___";

    public static class ProcessingTransformParameters {

        private List<StageParameters> stages;
        private List<String> outputs;
        private OutputType outputType;


        public List<StageParameters> getStages() {
            return stages;
        }

        public List<String> getOutputs() {
            return outputs;
        }

        public OutputType getOutputType() {
            return outputType;
        }


        public static ProcessingTransformParameters of(final JsonElement jsonElement, final String name) {
            final ProcessingTransformParameters parameters = new Gson().fromJson(jsonElement, ProcessingTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("ProcessingTransform config parameters must not be empty!");
            }

            parameters.validate(name);
            parameters.setDefaults();

            return parameters;
        }


        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(stages == null || stages.size() == 0) {
                errorMessages.add("processing module: " + name + ".stages parameter must not be empty");
            } else {
                for(final StageParameters stage : stages) {
                    errorMessages.addAll(stage.validate());
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            for(final StageParameters stage : stages) {
                stage.setDefaults();
            }
            if(this.outputType == null) {
                this.outputType = OutputType.avro;
            }
        }

    }

    public static class StageParameters {

        private String name;
        private List<String> inputs;
        private List<String> triggerInputs;
        private List<String> groupFields;
        private List<String> remainFields;
        private Type type;
        private JsonArray steps;
        private JsonElement filter;
        private List<String> outputFields;
        private Map<String, String> outputRenameFields;

        public String getName() {
            return name;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public List<String> getTriggerInputs() {
            return triggerInputs;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public List<String> getRemainFields() {
            return remainFields;
        }

        public Type getType() {
            return type;
        }

        public JsonArray getSteps() {
            return steps;
        }

        public JsonElement getFilter() {
            return filter;
        }

        public List<String> getOutputFields() {
            return outputFields;
        }

        public Map<String, String> getOutputRenameFields() {
            return outputRenameFields;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.inputs == null || this.inputs.size() == 0) {
                errorMessages.add("inputs must not be null and size zero.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.triggerInputs == null || this.triggerInputs.size() == 0) {
                this.triggerInputs = new ArrayList<>(inputs);
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            if(this.remainFields == null) {
                this.remainFields = new ArrayList<>();
            }
            this.remainFields.addAll(this.groupFields);

            if(this.outputFields == null) {
                this.outputFields = new ArrayList<>();
            }
            if(this.outputRenameFields == null) {
                this.outputRenameFields = new HashMap<>();
            }
        }

        public Stage toStage() {
            final List<Processor> processors = new ArrayList<>();
            for(final JsonElement element : this.steps) {
                final Processor processor = Processor.of(element);
                processors.add(processor);
            }
            final String filterString = filter == null ? null : filter.toString();
            return Stage.of(name, inputs, triggerInputs, groupFields, remainFields, outputFields, outputRenameFields, type, processors, filterString);
        }
    }

    public static class Stage implements Serializable {
        private String name;
        private List<String> inputs;
        private List<String> triggerInputs;
        private List<String> groupFields;
        private List<String> remainFields;
        private List<String> outputFields;
        private Map<String, String> outputRenameFields;
        private Type type;
        private List<Processor> processors;
        private String filterString;


        public Stage() {

        }

        public static Stage of(final String name,
                               final List<String> inputs,
                               final List<String> triggerInputs,
                               final List<String> groupFields,
                               final List<String> remainFields,
                               final List<String> outputFields,
                               final Map<String, String> outputRenameFields,
                               final Type type,
                               final List<Processor> processors,
                               final String filterString) {

            final Stage stage = new Stage();
            stage.name = name;
            stage.inputs = inputs;
            stage.triggerInputs = triggerInputs;
            stage.groupFields = groupFields;
            stage.remainFields = remainFields;
            stage.outputFields = outputFields;
            stage.outputRenameFields = outputRenameFields;
            stage.type = type;
            stage.processors = processors;
            stage.filterString = filterString;
            return stage;
        }

        public String getName() {
            return name;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public List<String> getRemainFields() {
            return remainFields;
        }

        public List<String> getOutputFields() {
            return outputFields;
        }

        public Map<String, String> getOutputRenameFields() {
            return outputRenameFields;
        }

        public Type getType() {
            return type;
        }

        public List<Processor> getProcessors() {
            return processors;
        }

        public List<String> getTriggerInputs() {
            return triggerInputs;
        }

        public String getFilterString() {
            return filterString;
        }
    }


    private enum Type implements Serializable {
        timeseries,
        normal
    }

    private enum OutputType {
        row,
        avro,
        struct,
        document,
        entity;

    }

    public String getName() {
        return "processing";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final ProcessingTransformParameters parameters = ProcessingTransformParameters.of(config.getParameters(), config.getName());

        final String lastStageName = parameters.getStages().get(parameters.getStages().size() - 1).getName();

        final Map<TupleTag<?>, String> inputNames = new HashMap<>();
        final Map<TupleTag<?>, DataType> inputTypes = new HashMap<>();
        final Map<TupleTag<?>, Schema> inputSchemas = new HashMap<>();

        final Pipeline pipeline = inputs.get(0).getCollection().getPipeline();
        pipeline.getCoderRegistry().registerCoderForClass(ProcessingBuffer.class, ProcessingBuffer.coder());
        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            inputNames.put(tag, input.getName());
            inputTypes.put(tag, input.getDataType());
            inputSchemas.put(tag, input.getSchema());
            tuple = tuple.and(tag, input.getCollection());
        }

        //
        switch (parameters.outputType) {
            case row -> {
                final Transform<Schema, Row> transform = new Transform<>(
                        parameters,
                        s -> s,
                        RowSchemaUtil::convertPrimitive,
                        RowSchemaUtil::create,
                        inputNames,
                        inputTypes,
                        inputSchemas);
                final PCollectionTuple outputTuple = tuple.apply(config.getName(), transform);
                final Map<TupleTag<Row>, String> outputNames = transform.getOutputNames();
                final Map<TupleTag<Row>, Schema> outputSchemas = transform.getOutputSchemas();

                final Map<String, FCollection<?>> outputs = new HashMap<>();
                for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : outputTuple.getAll().entrySet()) {
                    final TupleTag<Row> outputTag = (TupleTag<Row>) entry.getKey();
                    final PCollection<Row> output = (PCollection<Row>) entry.getValue();
                    final Schema outputSchema = outputSchemas.get(outputTag);
                    final String stageName = outputNames.get(outputTag);
                    final String name = config.getName() + "." + stageName;
                    final FCollection<?> collection = FCollection.of(name, output.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
                    outputs.put(name, collection);
                    if (lastStageName.equals(stageName)) {
                        final FCollection<?> lastCollection = FCollection.of(config.getName(), output.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
                        outputs.put(config.getName(), lastCollection);
                    }
                }
                return outputs;
            }
            case avro -> {
                final Transform<org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        parameters,
                        RowToRecordConverter::convertSchema,
                        AvroSchemaUtil::convertPrimitive,
                        AvroSchemaUtil::create,
                        inputNames,
                        inputTypes,
                        inputSchemas);
                final PCollectionTuple outputTuple = tuple.apply(config.getName(), transform);
                final Map<TupleTag<GenericRecord>, String> outputNames = transform.getOutputNames();
                final Map<TupleTag<GenericRecord>, Schema> outputSchemas = transform.getOutputSchemas();

                final Map<String, FCollection<?>> outputs = new HashMap<>();
                for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : outputTuple.getAll().entrySet()) {
                    final TupleTag<GenericRecord> outputTag = (TupleTag<GenericRecord>) entry.getKey();
                    final PCollection<GenericRecord> output = (PCollection<GenericRecord>) entry.getValue();
                    final Schema outputSchema = outputSchemas.get(outputTag);
                    final String stageName = outputNames.get(outputTag);
                    final String name = config.getName() + "." + stageName;
                    final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);
                    final FCollection<?> collection = FCollection.of(name, output.setCoder(AvroCoder.of(outputAvroSchema)), DataType.AVRO, outputAvroSchema);
                    outputs.put(name, collection);
                    if (lastStageName.equals(stageName)) {
                        final FCollection<?> lastCollection = FCollection.of(config.getName(), output.setCoder(AvroCoder.of(outputAvroSchema)), DataType.AVRO, outputAvroSchema);
                        outputs.put(config.getName(), lastCollection);
                    }
                }
                return outputs;
            }
            default -> throw new IllegalArgumentException("Not supported input type: " + parameters.getOutputType());
        }
    }

    public static class Transform<RuntimeSchemaT,T> extends PTransform<PCollectionTuple, PCollectionTuple> {
        private final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.PrimitiveValueConverter valueConverter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

        private final Map<TupleTag<?>, String> inputNames;
        private final Map<TupleTag<?>, DataType> inputTypes;
        private final Map<TupleTag<?>, Schema> inputSchemas;

        private final List<Stage> stages;
        private final List<String> outputs;

        private final Map<TupleTag<T>, String> outputNames;
        private final Map<TupleTag<T>, Schema> outputSchemas;

        public Map<TupleTag<T>, String> getOutputNames() {
            return this.outputNames;
        }

        public Map<TupleTag<T>, Schema> getOutputSchemas() {
            return this.outputSchemas;
        }


        Transform(final ProcessingTransformParameters parameters,
                  final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                  final Map<TupleTag<?>, String> inputNames,
                  final Map<TupleTag<?>, DataType> inputTypes,
                  final Map<TupleTag<?>, Schema> inputSchemas) {

            this.schemaConverter = schemaConverter;
            this.valueConverter = valueConverter;
            this.valueCreator = valueCreator;

            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.inputSchemas = inputSchemas;

            this.stages = parameters.getStages().stream()
                    .map(StageParameters::toStage)
                    .collect(Collectors.toList());
            this.outputs = Optional.ofNullable(parameters.getOutputs()).orElseGet(() -> {
                final List<String> list = new ArrayList<>();
                list.add(this.stages.get(this.stages.size() - 1).getName());
                return list;
            });

            this.outputNames = new HashMap<>();
            this.outputSchemas = new HashMap<>();
        }


        @Override
        public PCollectionTuple expand(PCollectionTuple inputTuple) {

            final Set<String> allStageInputFieldNames = new HashSet<>();
            for(final Stage stage : stages) {
                for(final Processor processor : stage.getProcessors()) {
                    allStageInputFieldNames.addAll(processor.getBufferSizes().keySet());
                }
                allStageInputFieldNames.addAll(stage.getRemainFields());
            }

            final Map<String, TupleTag<Map<String, Object>>> stageOutputTags = new HashMap<>();
            final Map<TupleTag<Map<String, Object>>, String> stageOutputNames = new HashMap<>();
            final Map<TupleTag<Map<String, Object>>, Schema> stageOutputSchemas = new HashMap<>();
            final Map<String, Schema.FieldType> stageInputFieldTypes = new HashMap<>();

            // ready inputs
            final Coder<Map<String,Object>> mapCoder = createMapCoder();
            PCollectionTuple stageOutputTuple = PCollectionTuple.empty(inputTuple.getPipeline());
            for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : inputTuple.getAll().entrySet()) {
                final TupleTag<Map<String, Object>> inputTag = (TupleTag<Map<String,Object>>) entry.getKey();
                final String inputName = inputNames.get(inputTag);
                final Schema inputSchema = inputSchemas.get(inputTag);
                final List<Schema.Field> inputFields = getInputFields(inputSchema, allStageInputFieldNames);
                final PCollection<Map<String, Object>> map = inputTuple.get(inputTag)
                        .apply("ToMap" + inputName, ParDo
                                .of(new MapDoFn(inputName, inputFields, inputTypes.get(inputTag))))
                        .setCoder(mapCoder);
                final TupleTag<Map<String, Object>> tag = new TupleTag<>(){};
                stageOutputTuple = stageOutputTuple.and(tag, map);
                stageOutputNames.put(tag, inputName);
                stageOutputSchemas.put(tag, inputSchema);
                stageOutputTags.put(inputName, tag);

                for(final Schema.Field field : inputFields) {
                    stageInputFieldTypes.put(field.getName(), field.getType());
                }
            }

            // process stages
            for(final Stage stage : stages) {
                final Map<TupleTag<Map<String, Object>>, String> stageInputNames = new HashMap<>();
                final Map<TupleTag<Map<String, Object>>, Schema> stageInputSchemas = new HashMap<>();
                PCollectionTuple stageInputTuple = PCollectionTuple.empty(inputTuple.getPipeline());
                for(String input : stage.getInputs()) {
                    final TupleTag<Map<String, Object>> tag = stageOutputTags.get(input);
                    if(!stageOutputTuple.has(tag)) {
                        throw new IllegalStateException();
                    }
                    stageInputTuple = stageInputTuple.and(tag, stageOutputTuple.get(tag));
                    stageInputNames.put(tag, input);
                    stageInputSchemas.put(tag, stageOutputSchemas.get(tag));
                }

                final StageTransform stageTransform = new StageTransform(stage, stageInputNames, stageInputSchemas, stageInputFieldTypes);
                final PCollection<Map<String, Object>> stageOutput = stageInputTuple
                        .apply("Stage" + stage.getName(), stageTransform)
                        .setCoder(mapCoder);

                final TupleTag<Map<String,Object>> stageOutputTag = new TupleTag<>(){};
                stageOutputTags.put(stage.getName(), stageOutputTag);
                stageOutputTuple = stageOutputTuple.and(stageOutputTag, stageOutput);
                stageOutputNames.put(stageOutputTag, stage.getName());
                stageOutputSchemas.put(stageOutputTag, stageTransform.getOutputSchema());
            }

            // format outputs
            PCollectionTuple outputTuple = PCollectionTuple.empty(inputTuple.getPipeline());
            for(final String outputName : outputs) {
                final TupleTag<Map<String, Object>> stageOutputTag = stageOutputTags.get(outputName);
                final TupleTag<T> outputTag = new TupleTag<>(){};
                final Schema outputSchema = stageOutputSchemas.get(stageOutputTag);
                final PCollection<T> output = stageOutputTuple.get(stageOutputTag)
                        .apply("Format" + outputName, ParDo
                                .of(new OutputDoFn<>(outputSchema, schemaConverter, valueConverter, valueCreator)));
                outputTuple = outputTuple.and(outputTag, output);

                outputNames.put(outputTag, outputName);
                outputSchemas.put(outputTag, outputSchema);
            }

            return outputTuple;
        }

        protected static class MapDoFn extends DoFn<Object, Map<String, Object>> {

            private final String source;
            private final List<Schema.Field> fields;
            private final SchemaUtil.PrimitiveValueGetter valueGetter;

            MapDoFn(final String source, final List<Schema.Field> fields, final DataType dataType) {
                this.source = source;
                this.fields = fields;
                this.valueGetter = switch (dataType) {
                    case ROW -> RowSchemaUtil::getAsPrimitive;
                    case AVRO -> AvroSchemaUtil::getAsPrimitive;
                    case STRUCT -> StructSchemaUtil::getAsPrimitive;
                    case DOCUMENT -> DocumentSchemaUtil::getAsPrimitive;
                    case ENTITY -> EntitySchemaUtil::getAsPrimitive;
                    default -> throw new IllegalStateException("Not supported dataType: " + dataType);
                };
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Object input = c.element();
                final Map<String, Object> values = new HashMap<>();
                for(final Schema.Field field : this.fields) {
                    final Object value = valueGetter.getValue(input, field.getType(), field.getName());
                    values.put(field.getName(), value);
                }
                values.put(FIELD_SOURCE, source);

                c.output(values);
            }

        }

        protected static class OutputDoFn<RuntimeSchemaT,T> extends DoFn<Map<String,Object>, T> {

            private final Schema outputSchema;
            private final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.PrimitiveValueConverter valueConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

            private transient RuntimeSchemaT runtimeOutputSchema;


            OutputDoFn(final Schema outputSchema,
                       final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                       final SchemaUtil.PrimitiveValueConverter valueConverter,
                       final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator) {

                this.outputSchema = outputSchema;
                this.schemaConverter = schemaConverter;
                this.valueConverter = valueConverter;
                this.valueCreator = valueCreator;
            }

            @Setup
            public void setup() {
                this.runtimeOutputSchema = schemaConverter.convert(outputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Map<String, Object> primitiveMap = c.element();
                if(primitiveMap == null) {
                    return;
                }
                final Map<String, Object> values = new HashMap<>();
                for(final Schema.Field field : outputSchema.getFields()) {
                    final Object primitiveValue = primitiveMap.get(field.getName());
                    final Object outputValue = valueConverter.convertPrimitive(field.getType(), primitiveValue);
                    values.put(field.getName(), outputValue);
                }
                final T output = valueCreator.create(runtimeOutputSchema, values);
                c.output(output);
            }

        }

        private static List<Schema.Field> getInputFields(final Schema schema, final Set<String> targetFields) {
            return schema.getFields().stream()
                    .filter(f -> targetFields.contains(f.getName()))
                    .filter(f -> {
                        switch (f.getType().getTypeName()) {
                            case ROW:
                            case MAP:
                            case ARRAY:
                            case ITERABLE:
                            case INT16:
                            case BYTE:
                                return false;
                            default:
                                return true;
                        }
                    })
                    .collect(Collectors.toList());
        }

    }

    public static class StageTransform extends PTransform<PCollectionTuple, PCollection<Map<String,Object>>> {

        private final String name;
        private final Type type;
        private final List<String> triggerInputs;
        private final List<String> groupFields;
        private final List<Schema.Field> remainFields;
        private final List<String> outputFields;
        private final Map<String, String> outputRenameFields;

        private final Map<TupleTag<Map<String, Object>>, String> inputNames;
        private final Map<TupleTag<Map<String, Object>>, Schema> inputSchemas;
        private final Map<String, Schema.FieldType> bufferInputTypes;
        private final List<Processor> processors;
        private final String condition;


        private transient Schema outputSchema;

        public Schema getOutputSchema() {
            return outputSchema;
        }

        StageTransform(final Stage stage,
                       final Map<TupleTag<Map<String, Object>>, String> inputNames,
                       final Map<TupleTag<Map<String, Object>>, Schema> inputSchemas,
                       final Map<String, Schema.FieldType> bufferInputTypes) {

            this.name = stage.name;
            this.type = stage.type;
            this.groupFields = stage.groupFields;
            this.processors = stage.processors;
            this.triggerInputs = new ArrayList<>(stage.getTriggerInputs());

            this.inputNames = inputNames;
            this.inputSchemas = inputSchemas;

            this.remainFields = new ArrayList<>();
            for(final String remainField : stage.getRemainFields()) {
                final Schema schema = inputSchemas.entrySet().stream().findAny().get().getValue();
                final Schema.Field field = schema.getField(remainField);
                this.remainFields.add(field.withNullable(true));
            }
            this.outputFields = stage.getOutputFields();
            this.outputRenameFields = stage.outputRenameFields;

            this.bufferInputTypes = bufferInputTypes;
            this.condition = stage.getFilterString();
        }


        @Override
        public PCollection<Map<String, Object>> expand(PCollectionTuple inputs) {

            PCollectionList<KV<String, Map<String, Object>>> list = PCollectionList.empty(inputs.getPipeline());
            for(final Map.Entry<TupleTag<?>,PCollection<?>> entry : inputs.getAll().entrySet()) {
                final TupleTag<Map<String,Object>> tag = (TupleTag<Map<String,Object>>) entry.getKey();
                final PCollection<KV<String, Map<String, Object>>> mapWithKey = inputs.get(tag)
                        .apply("WithKey" + inputNames.get(tag), ParDo
                                .of(new WithKeyDoFn(groupFields)))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), inputs.get(tag).getCoder()));
                list = list.and(mapWithKey);
            }

            this.outputSchema = createOutputSchema();

            final PCollection<KV<String, Map<String, Object>>> mapsWithKeyWindow = list
                    .apply("Flatten", Flatten.pCollections());

            final PCollection<Map<String,Object>> output;
            switch (type) {
                case normal -> {
                    if (!groupFields.isEmpty()) {
                        output = mapsWithKeyWindow
                                .apply("GroupByKey", GroupByKey.create())
                                .apply("StatelessGroupProcess", ParDo.of(new AggregateDoFn(name, remainFields, processors)));
                    } else {
                        output = mapsWithKeyWindow
                                .apply("StatelessSingleProcess", ParDo.of(new StatelessDoFn(name, remainFields, processors)));
                    }
                }
                case timeseries -> {
                    final Map<String, Integer> bufferSizes = Processor.getBufferSizes(processors);
                    final Map<String, Schema.FieldType> bufferTypes = Processor.getBufferTypes(processors, bufferInputTypes);
                    final Map<String, Processor.SizeUnit> bufferUnits = Processor.getBufferUnits(processors);

                    final Window<KV<String, Map<String, Object>>> window = createStatefulWindow();
                    if (OptionUtil.isStreaming(inputs)) {
                        output = mapsWithKeyWindow
                                .apply("WithWindow", window)
                                .apply("StatefulStreamingProcess", ParDo.of(new StatefulStreamingDoFn(name, remainFields, processors, bufferSizes, bufferTypes, bufferUnits, triggerInputs)));
                    } else {
                        output = mapsWithKeyWindow
                                .apply("WithWindow", window)
                                .apply("StatefulBatchProcess", ParDo.of(new StatefulBatchDoFn(name, remainFields, processors, bufferSizes, bufferTypes, bufferUnits, triggerInputs)));
                    }
                }
                default -> throw new IllegalStateException("Not supported processing type: " + type);
            }

            if(outputFields.isEmpty() && outputRenameFields.isEmpty() && condition == null) {
                return output;
            } else {
                final Coder<Map<String,Object>> mapCoder = createMapCoder();
                return output
                        .setCoder(mapCoder)
                        .apply("SelectFilter", ParDo.of(new SelectDoFn(outputFields, outputRenameFields, condition)));
            }

        }

        private Schema createOutputSchema() {

            final Map<String, Schema.FieldType> inputTypes = new HashMap<>();
            for(final Map.Entry<TupleTag<Map<String, Object>>, Schema> inputSchema : inputSchemas.entrySet()) {
                for(final Schema.Field field : inputSchema.getValue().getFields()) {
                    inputTypes.put(field.getName(), field.getType());
                }
            }

            final List<Schema.Field> outputSchemaFields = new ArrayList<>();
            for(final Schema.Field targetField : remainFields) {
                outputSchemaFields.add(Schema.Field.of(targetField.getName(), targetField.getType().withNullable(true)));
            }
            for(final Processor processor : processors) {
                if(processor.ignore()) {
                    continue;
                }
                final List<Schema.Field> fields = processor.getOutputFields(inputTypes);
                outputSchemaFields.addAll(fields);

                for(final Schema.Field field : fields) {
                    inputTypes.put(field.getName(), field.getType());
                }
            }

            final Schema.Builder builder = Schema.builder();
            if(this.outputFields.isEmpty() && this.outputRenameFields.isEmpty()) {
                return builder.addFields(outputSchemaFields).build();
            } else if(this.outputFields.isEmpty()) {
                for(final Schema.Field outputSchemaField : outputSchemaFields) {
                    final String renamedField = outputRenameFields.getOrDefault(outputSchemaField.getName(), outputSchemaField.getName());
                    builder.addField(Schema.Field.of(renamedField, outputSchemaField.getType()));
                }
            } else if(this.outputRenameFields.isEmpty()) {
                for(final Schema.Field outputSchemaField : outputSchemaFields) {
                    if(outputFields.contains(outputSchemaField.getName())) {
                        builder.addField(outputSchemaField);
                    }
                }
            } else {
                for(final Schema.Field outputSchemaField : outputSchemaFields) {
                    if(outputFields.contains(outputSchemaField.getName())) {
                        if(outputRenameFields.containsKey(outputSchemaField.getName())) {
                            final String renamedField = outputRenameFields.getOrDefault(outputSchemaField.getName(), outputSchemaField.getName());
                            builder.addField(Schema.Field.of(renamedField, outputSchemaField.getType()));
                        } else {
                            builder.addField(outputSchemaField);
                        }
                    } else if(outputRenameFields.containsKey(outputSchemaField.getName())) {
                        final String renamedField = outputRenameFields.getOrDefault(outputSchemaField.getName(), outputSchemaField.getName());
                        builder.addField(Schema.Field.of(renamedField, outputSchemaField.getType()));
                    }
                }
            }

            return builder.build();
        }

        private Window<KV<String, Map<String, Object>>> createStatefulWindow() {
            return Window.<KV<String,Map<String,Object>>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO);
        }

        private static class WithKeyDoFn extends DoFn<Map<String, Object>, KV<String, Map<String, Object>>> {

            private final List<String> groupFieldNames;

            WithKeyDoFn(final List<String> groupFieldNames) {
                this.groupFieldNames = groupFieldNames;
            }

            @ProcessElement
            public void  processElement(ProcessContext c) {
                final Map<String, Object> values = c.element();
                if(values == null) {
                    return;
                }
                final StringBuilder sb = new StringBuilder();
                for(final String field : this.groupFieldNames) {
                    final Object value = values.get(field);
                    if(value != null) {
                        sb.append(value);
                    }
                    sb.append("#");
                }
                final String key = sb.toString();
                c.output(KV.of(key, values));
            }

        }

        private static class AggregateDoFn extends DoFn<KV<String, Iterable<Map<String, Object>>>, Map<String, Object>> {

            private final String name;
            private final List<Schema.Field> fields;
            private final List<Processor> processors;


            AggregateDoFn(final String name, final List<Schema.Field> fields, final List<Processor> processors) {
                this.name = name;
                this.fields = fields;
                this.processors = processors;
            }

            @Setup
            public void setup() {
                for(final Processor processor : processors) {
                    processor.setup();
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final List<Map<String, Object>> inputs = Lists.newArrayList(c.element().getValue());
                final List<Map<String, Object>> outputs = Processor.process(processors, inputs, c.timestamp());
                for(int i=0; i<Math.min(inputs.size(), outputs.size()); i++) {
                    final Map<String, Object> input = inputs.get(i);
                    final Map<String, Object> output = outputs.get(i);
                    c.output(format(input, output));
                }
            }

            private Map<String, Object> format(Map<String, Object> input, Map<String, Object> output) {
                for(final Schema.Field field : fields) {
                    output.put(field.getName(), input.get(field.getName()));
                }
                output.put(FIELD_SOURCE, name);
                return output;
            }

        }

        private static class StatelessDoFn extends DoFn<KV<String, Map<String,Object>>, Map<String,Object>> {

            private final String name;
            private final List<Schema.Field> fields;
            private final List<Processor> processors;

            StatelessDoFn(final String name, final List<Schema.Field> fields, final List<Processor> processors) {
                this.name = name;
                this.fields = fields;
                this.processors = processors;
            }

            @Setup
            public void setup() {
                for(final Processor processor : processors) {
                    processor.setup();
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final Map<String, Object> input = c.element().getValue();
                final Map<String, Object> output = Processor.process(processors, input, c.timestamp());

                for(final Schema.Field field : fields) {
                    output.put(field.getName(), input.get(field.getName()));
                }
                output.put(FIELD_SOURCE, name);

                c.output(output);
            }

        }

        protected static class StatefulDoFn extends DoFn<KV<String, Map<String,Object>>, Map<String,Object>> {

            protected static final String STATEID_BUFFER = "processingTransformBuffer";
            protected static final String STATEID_STATE = "processingTransformState";
            protected static final String STATEID_BUFFER_UPDATE_INTERVAL_COUNTER = "processingTransformBufferUpdateIntervalCounter";


            private final String name;
            private final List<Schema.Field> fields;
            private final List<Processor> processors;
            private final List<String> triggerInputs;

            private final Map<String, Integer> bufferSizes;
            private final Map<String, Schema.FieldType> bufferTypes;
            private final Map<String, Integer> bufferUnits;

            private final Integer bufferUpdateInterval = 100;

            StatefulDoFn(final String name,
                         final List<Schema.Field> fields,
                         final List<Processor> processors,
                         final Map<String, Integer> bufferSizes,
                         final Map<String, Schema.FieldType> bufferTypes,
                         final Map<String, Processor.SizeUnit> bufferSizeUnits,
                         final List<String> triggerInputs) {

                this.name = name;
                this.fields = fields;
                this.processors = processors;
                this.triggerInputs = triggerInputs;

                this.bufferSizes = bufferSizes;
                this.bufferTypes = bufferTypes;
                this.bufferUnits = bufferSizeUnits.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().id));
            }

            protected void setup() {
                for(final Processor processor : processors) {
                    processor.setup();
                }
            }

            protected void processElement(
                    final ProcessContext c,
                    final ValueState<ProcessingBuffer> bufferValueState,
                    final ValueState<ProcessingState> stateValueState,
                    final ValueState<Integer> bufferUpdateIntervalCounterValueState,
                    final boolean isLast) {

                final ProcessingBuffer buffer = Optional
                        .ofNullable(bufferValueState.read())
                        .orElseGet(() -> ProcessingBuffer.of(bufferTypes, bufferSizes, bufferUnits));
                final ProcessingState state = Optional
                        .ofNullable(stateValueState.read())
                        .orElseGet(ProcessingState::new);

                if(!isLast) {
                    // deep-copy state
                    final ProcessingBuffer initBuffer = buffer.copy();
                    bufferValueState.write(initBuffer);
                }

                process(c, buffer, state);

                if(isLast) {
                    final int bufferUpdateIntervalCounter = Optional.ofNullable(bufferUpdateIntervalCounterValueState.read()).orElse(1);
                    if(bufferUpdateIntervalCounter % bufferUpdateInterval == 0) {
                        buffer.update(c.timestamp());
                        bufferUpdateIntervalCounterValueState.write(1);
                    } else {
                        bufferUpdateIntervalCounterValueState.write(bufferUpdateIntervalCounter + 1);
                    }

                    bufferValueState.write(buffer);
                    stateValueState.write(state);
                }
            }

            private void process(
                    final ProcessContext c,
                    final ProcessingBuffer buffer,
                    final ProcessingState state) {

                final Map<String, Object> input = c.element().getValue();
                for(final Map.Entry<String, Integer> entry : buffer.getSizes().entrySet()) {
                    if(input.containsKey(entry.getKey())) {
                        final Object value = input.get(entry.getKey());
                        buffer.add(entry.getKey(), value, c.timestamp());
                    }
                }

                final Map<String, Object> output = Processor.process(processors, buffer, state, c.timestamp());

                output.put(FIELD_SOURCE, name);

                for(final Schema.Field field : fields) {
                    output.put(field.getName(), input.get(field.getName()));
                }

                final String source = (String) input.get(FIELD_SOURCE);
                if(triggerInputs.contains(source)) {
                    c.output(output);
                }

            }

        }

        protected static class StatefulBatchDoFn extends StatefulDoFn {

            @StateId(STATEID_BUFFER)
            private final StateSpec<ValueState<ProcessingBuffer>> bufferSpec;
            @StateId(STATEID_STATE)
            private final StateSpec<ValueState<ProcessingState>> stateSpec;
            @StateId(STATEID_BUFFER_UPDATE_INTERVAL_COUNTER)
            private final StateSpec<ValueState<Integer>> bufferUpdateIntervalCounterSpec;

            StatefulBatchDoFn(final String name,
                              final List<Schema.Field> fields,
                              final List<Processor> processors,
                              final Map<String, Integer> bufferSizes,
                              final Map<String, Schema.FieldType> bufferTypes,
                              final Map<String, Processor.SizeUnit> bufferSizeUnits,
                              final List<String> targetSources) {

                super(name, fields, processors, bufferSizes, bufferTypes, bufferSizeUnits, targetSources);

                this.bufferSpec = StateSpecs.value(ProcessingBuffer.coder());
                this.stateSpec = StateSpecs.value(SerializableCoder.of(ProcessingState.class));
                this.bufferUpdateIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<ProcessingBuffer> bufferValueState,
                    final @AlwaysFetched @StateId(STATEID_STATE) ValueState<ProcessingState> stateValueState,
                    final @AlwaysFetched @StateId(STATEID_BUFFER_UPDATE_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

                super.processElement(c, bufferValueState, stateValueState, bufferUpdateIntervalValueState, true);
            }

        }

        protected static class StatefulStreamingDoFn extends StatefulDoFn {

            @StateId(STATEID_BUFFER)
            private final StateSpec<ValueState<ProcessingBuffer>> bufferSpec;
            @StateId(STATEID_STATE)
            private final StateSpec<ValueState<ProcessingState>> stateSpec;
            @StateId(STATEID_BUFFER_UPDATE_INTERVAL_COUNTER)
            private final StateSpec<ValueState<Integer>> bufferUpdateIntervalCounterSpec;

            StatefulStreamingDoFn(final String name,
                                  final List<Schema.Field> fields,
                                  final List<Processor> processors,
                                  final Map<String, Integer> bufferSizes,
                                  final Map<String, Schema.FieldType> bufferTypes,
                                  final Map<String, Processor.SizeUnit> bufferSizeUnits,
                                  final List<String> targetSources) {

                super(name, fields, processors, bufferSizes, bufferTypes, bufferSizeUnits, targetSources);

                this.bufferSpec = StateSpecs.value(ProcessingBuffer.coder());
                this.stateSpec = StateSpecs.value(SerializableCoder.of(ProcessingState.class));
                this.bufferUpdateIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<ProcessingBuffer> bufferValueState,
                    final @AlwaysFetched @StateId(STATEID_STATE) ValueState<ProcessingState> stateValueState,
                    final @AlwaysFetched @StateId(STATEID_BUFFER_UPDATE_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

                super.processElement(c, bufferValueState, stateValueState, bufferUpdateIntervalValueState,true);
            }

        }

        private static class SelectDoFn extends DoFn<Map<String, Object>, Map<String, Object>> {

            private final List<String> fields;
            private final Map<String, String> renameFields;
            private final String condition;

            private transient Filter.ConditionNode conditionNode;


            SelectDoFn(final List<String> fields,
                       final Map<String, String> renameFields,
                       final String condition) {

                this.fields = fields;
                this.renameFields = renameFields;
                this.condition = condition;
            }

            @Setup
            public void setup() {
                if(condition != null) {
                    this.conditionNode = Filter.parse(new Gson().fromJson(condition, JsonElement.class));
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Map<String, Object> input = c.element();
                if(input == null) {
                    return;
                }

                if(condition != null) {
                    if(!Filter.filter(conditionNode, input)) {
                        return;
                    }
                }

                if(fields.isEmpty() && renameFields.isEmpty()) {
                    c.output(input);
                    return;
                }

                final Map<String, Object> output = new HashMap<>();
                if(fields.isEmpty()) {
                    for(final Map.Entry<String, Object> entry : input.entrySet()) {
                        final String renamedField = renameFields.getOrDefault(entry.getKey(), entry.getKey());
                        output.put(renamedField, entry.getValue());
                    }
                } else {
                    if(renameFields.isEmpty()) {
                        for(final String field : fields) {
                            output.put(field, input.get(field));
                        }
                    } else {
                        for(final String field : fields) {
                            final String renamedField = renameFields.getOrDefault(field, field);
                            output.put(renamedField, input.get(field));
                        }
                    }
                }

                c.output(output);
            }
        }

    }

    private static Coder<Map<String,Object>> createMapCoder() {
        final org.apache.avro.Schema multiValueSchema = org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)
        );

        final org.apache.avro.Schema mapSchema = org.apache.avro.Schema.createMap(multiValueSchema);
        final Coder coder = AvroCoder.of(Map.class, mapSchema, false);
        return (Coder<Map<String,Object>>) coder;
    }

}
