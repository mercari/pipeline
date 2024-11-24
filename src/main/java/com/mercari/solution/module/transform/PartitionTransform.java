package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Transform.Module(name="partition")
public class PartitionTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionTransform.class);

    private static class PartitionTransformParameters implements Serializable {

        private Boolean exclusive;
        private Boolean union;
        private List<PartitionParameter> partitions;

        private static class PartitionParameter implements Serializable {

            private String name;
            private JsonElement filter;
            private JsonArray select;

            private List<String> validate(int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.name == null) {
                    errorMessages.add("parameters.partitions[" + index + "].output must not be null");
                }
                return errorMessages;
            }

            private void setDefaults() {

            }

        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.partitions == null || this.partitions.isEmpty()) {
                errorMessages.add("parameters.partitions must not be null");
            } else {
                for(int index=0; index<this.partitions.size(); index++) {
                    errorMessages.addAll(partitions.get(index).validate(index));
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            for(final PartitionParameter partitionParameter : partitions) {
                partitionParameter.setDefaults();
            }
            if(this.exclusive == null) {
                this.exclusive = true;
            }
            if(this.union == null) {
                this.union = false;
            }
        }

    }

    private static class PartitionSetting implements Serializable {

        private String name;
        private Schema schema;
        private String conditionJson;
        private List<SelectFunction> selectFunctions;
        private TupleTag<MElement> tag;

        private transient Filter.ConditionNode conditionNode;

        PartitionSetting(
                String name, Schema schema, JsonElement conditionJson, List<SelectFunction> selectFunctions) {
            this.name = name;
            this.schema = schema;
            this.conditionJson = Optional.ofNullable(conditionJson).map(JsonElement::toString).orElse(null);
            this.selectFunctions = selectFunctions;
            this.tag = new TupleTag<>() {};
        }

        public void setup() {
            //this.schema.setup();
            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }
            if(conditionJson != null) {
                this.conditionNode = Filter.parse(conditionJson);
            }
        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {

        final PartitionTransformParameters parameters = getParameters(PartitionTransformParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final List<TupleTag<?>> outputTags = new ArrayList<>();
        final List<PartitionSetting> settings = new ArrayList<>();
        final List<Schema> outputSchemas = new ArrayList<>();
        for(int i=0; i<parameters.partitions.size(); i++) {
            final var p = parameters.partitions.get(i);
            final Schema outputSchema;
            final List<SelectFunction> selectFunctions;
            if(p.select != null && p.select.isJsonArray()) {
                selectFunctions = SelectFunction.of(p.select, inputSchema.getFields());
                outputSchema = SelectFunction.createSchema(selectFunctions);
            } else {
                selectFunctions = new ArrayList<>();
                outputSchema = inputSchema;
            }
            final PartitionSetting setting = new PartitionSetting(p.name, outputSchema, p.filter, selectFunctions);
            if(!parameters.union) {
                outputTags.add(setting.tag);
            }
            settings.add(setting);
            outputSchemas.add(outputSchema);
        }

        final TupleTag<MElement> defaultOutputTag = new TupleTag<>() {};
        final TupleTag<MElement> failureTag = new TupleTag<>() {};

        outputTags.add(failureTag);

        MCollectionTuple tuple;
        final PCollectionTuple outputs;
        if(!getOutputFailure() && parameters.union) {
            final PCollection<MElement> output = input
                    .apply("Partition", ParDo
                            .of(new PartitionDoFn(getJobName(), getName(), parameters, settings, inputSchema, failureTag, getFailFast())));
            if(parameters.union) {
                outputs = null;
            } else {
                outputs = PCollectionTuple.of(defaultOutputTag, output);
            }
            tuple = MCollectionTuple
                    .of(output, outputSchemas.getFirst());
        } else {
            outputs = input
                    .apply("Partition", ParDo
                            .of(new PartitionDoFn(getJobName(), getName(), parameters, settings, inputSchema, failureTag, getFailFast()))
                            .withOutputTags(defaultOutputTag, TupleTagList.of(outputTags)));
            final PCollection<MElement> defaultOutput = outputs.get(defaultOutputTag);
            tuple = MCollectionTuple
                    .of(defaultOutput, inputSchema)
                    .failure(outputs.get(failureTag));
        }

        if (!parameters.union) {
            for (final PartitionSetting setting : settings) {
                final PCollection<MElement> output = outputs.get(setting.tag).setCoder(ElementCoder.of(setting.schema));
                tuple = tuple.and(setting.name, output, setting.schema);
            }
        }
        return tuple;
    }

    private static class PartitionDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final Boolean exclusive;
        private final Boolean union;
        private final List<PartitionSetting> settings;
        private final Schema inputSchema;
        private final TupleTag<MElement> failureTag;
        private final boolean failFast;

        private final Counter errorCounter;

        PartitionDoFn(
                final String jobName,
                final String name,
                final PartitionTransformParameters parameters,
                final List<PartitionSetting> settings,
                final Schema inputSchema,
                final TupleTag<MElement> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.exclusive = parameters.exclusive;
            this.union = parameters.union;
            this.settings = settings;
            this.inputSchema = inputSchema;
            this.failureTag = failureTag;
            this.failFast = failFast;

            this.errorCounter = Metrics.counter(name, "partition_error");
        }

        @Setup
        public void setup() {
            //inputSchema.setup();
            for(final PartitionSetting setting : settings) {
                setting.setup();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            boolean outputed = false;
            try {
                for (final PartitionSetting setting : settings) {
                    if (setting.conditionNode == null) {
                        continue;
                    }
                    if (Filter.filter(setting.conditionNode, inputSchema, element)) {
                        final MElement result;
                        if(setting.selectFunctions.isEmpty()) {
                            result = MElement.of(setting.schema, element.asPrimitiveMap(), c.timestamp());
                        } else {
                            final Map<String, Object> values = SelectFunction.apply(setting.selectFunctions, element, c.timestamp());
                            result = MElement.of(setting.schema, values, c.timestamp());
                        }
                        if(union) {
                            c.output(result);
                        } else {
                            c.output(setting.tag, result);
                        }
                        outputed = true;
                        if (exclusive) {
                            return;
                        }
                    }
                }
                if(!outputed) {
                    final MElement output = MElement.of(inputSchema, element.asPrimitiveMap(), c.timestamp());
                    if(!union) {
                        c.output(output);
                    }
                }
            } catch (final Throwable e) {
                errorCounter.inc();
                final MFailure failure = MFailure.of(jobName, name, element.toString(), e, c.timestamp());
                if(failFast) {
                    throw new RuntimeException("partition module " + name + " failed to execute partition for element: " + element, e);
                }

                c.output(failureTag, failure.toElement(c.timestamp()));
                LOG.error("Failed to execute partition for element: {}, cause: {}", element, failure.getError());
            }
        }
    }

}
