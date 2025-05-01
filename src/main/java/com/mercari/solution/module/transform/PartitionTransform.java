package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Transform.Module(name="partition")
public class PartitionTransform extends Transform {

    private static class Parameters implements Serializable {

        private Boolean exclusive;
        private Boolean union;
        private List<PartitionParameter> partitions;

        private static class PartitionParameter implements Serializable {

            private String name;
            private JsonElement filter;
            private JsonArray select;

            private List<String> validate(int index, Schema schema) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.name == null) {
                    errorMessages.add("parameters.partitions[" + index + "].output must not be null");
                }
                if(filter != null && !filter.isJsonNull() && !filter.isJsonPrimitive()) {
                    final Filter.ConditionNode node = Filter.parse(filter);
                    for(final String m : node.validate(schema.getFields())) {
                        errorMessages.add("parameters.partitions[" + index + "].filter is illegal: " + m);
                    }
                }
                return errorMessages;
            }

            private void setDefaults() {

            }

        }

        private void validate(final Schema schema) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.partitions == null || this.partitions.isEmpty()) {
                errorMessages.add("parameters.partitions must not be null");
            } else {
                for(int index=0; index<this.partitions.size(); index++) {
                    errorMessages.addAll(partitions.get(index).validate(index, schema));
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


    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        final Schema inputSchema = Union.createUnionSchema(inputs);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

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

        final Schema outputSchema = Union.createUnionSchema(outputSchemas);

        final TupleTag<MElement> defaultOutputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final TupleTag<MElement> excludedTag = new TupleTag<>() {};

        outputTags.add(failureTag);
        outputTags.add(excludedTag);

        final PCollectionTuple outputs = input
                .apply("Partition", ParDo
                        .of(new PartitionDoFn(getJobName(), getName(), parameters, settings, inputSchema, failureTag, excludedTag, getFailFast()))
                        .withOutputTags(defaultOutputTag, TupleTagList.of(outputTags)));

        if(errorHandler != null) {
            errorHandler.addError(outputs.get(failureTag));
        }

        final PCollection<MElement> defaultOutput = outputs.get(defaultOutputTag);
        final PCollection<MElement> excludedOutput = outputs.get(excludedTag);
        MCollectionTuple tuple = MCollectionTuple
                .of(defaultOutput, outputSchema)
                .and("excluded", excludedOutput, inputSchema);

        if (!parameters.union) {
            for (final PartitionSetting setting : settings) {
                final PCollection<MElement> output = outputs
                        .get(setting.tag)
                        .setCoder(ElementCoder.of(setting.schema));
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
        private final TupleTag<BadRecord> failureTag;
        private final TupleTag<MElement> excludedTag;
        private final boolean failFast;

        PartitionDoFn(
                final String jobName,
                final String name,
                final Parameters parameters,
                final List<PartitionSetting> settings,
                final Schema inputSchema,
                final TupleTag<BadRecord> failureTag,
                final TupleTag<MElement> excludedTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.exclusive = parameters.exclusive;
            this.union = parameters.union;
            this.settings = settings;
            this.inputSchema = inputSchema;
            this.failureTag = failureTag;
            this.excludedTag = excludedTag;
            this.failFast = failFast;

            //this.errorCounter = Metrics.counter(name, "partition_error");
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
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                boolean outputed = false;
                for (final PartitionSetting setting : settings) {
                    if (setting.conditionNode == null) {
                        continue;
                    }
                    if (Filter.filter(setting.conditionNode, inputSchema, input)) {
                        final MElement result;
                        if(setting.selectFunctions.isEmpty()) {
                            result = MElement.of(setting.schema, input.asPrimitiveMap(), c.timestamp());
                        } else {
                            final Map<String, Object> values = SelectFunction.apply(setting.selectFunctions, input, c.timestamp());
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
                    c.output(excludedTag, input);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process partition", input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }
    }

}
