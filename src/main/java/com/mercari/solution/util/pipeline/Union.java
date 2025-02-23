package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


public class Union {

    public static class Parameters implements Serializable {

        public String baseInput;
        public List<MappingParameter> mappings;
        public Boolean each;

        public static class MappingParameter implements Serializable {
            private String outputField;
            private List<MappingInputParameter> inputs;
        }

        public static class MappingInputParameter implements Serializable {
            private String input;
            private String field;
        }

        public void validate(final MCollectionTuple inputs) {

            final List<String> errorMessages = new ArrayList<>();
            if(this.mappings != null) {
                int index = 0;
                for(final MappingParameter mapping : this.mappings) {
                    if(mapping.outputField == null) {
                        errorMessages.add("parameters.mappings[" + index + "].outputField must not be null.");
                    }
                    if(mapping.inputs == null) {
                        errorMessages.add("parameters.mappings[" + index + "].inputs must not be null.");
                    } else {
                        int indexInput = 0;
                        for(final MappingInputParameter mappingInput : mapping.inputs) {
                            if(mappingInput.input == null) {
                                errorMessages.add("parameters.mappings[" + index + "].inputs[" + indexInput +"].input must not be null.");
                            } else if(!inputs.has(mappingInput.input)){
                                errorMessages.add("parameters.mappings[" + index + "].inputs[" + indexInput +"].input does not exists in inputs: " + inputs);
                            }
                            if(mappingInput.field == null) {
                                errorMessages.add("parameters mappings[" + index + "].inputs[" + indexInput +"].field must not be null.");
                            }
                            indexInput++;
                        }
                    }
                    index++;
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            if(each == null) {
                this.each = false;
            }
            if(this.mappings == null) {
                this.mappings = new ArrayList<>();
            }
        }

        public static Parameters createDefaultParameter() {
            final Parameters parameters = new Parameters();
            parameters.setDefaults();
            return parameters;
        }

    }

    public static Schema createUnionSchema(final MCollectionTuple inputs) {
        return createUnionSchema(inputs, null);
    }

    public static Schema createUnionSchema(final MCollectionTuple inputs, final Parameters parameters) {
        if(inputs.size() == 1) {
            return inputs.getSingleSchema();
        }
        if(parameters != null && parameters.baseInput != null) {
            return inputs.getSchema(parameters.baseInput);
        } else if(parameters != null && !parameters.mappings.isEmpty()) {
            final Schema.Builder builder = Schema.builder();
            for(final Parameters.MappingParameter mapping : parameters.mappings) {
                final Schema.Field field = inputs.getSchema(mapping.inputs.get(0).input).getField(mapping.inputs.get(0).field);
                builder.withField(mapping.outputField, field.getFieldType().withNullable(true));
            }
            return builder.build();
        } else {
            return createUnionSchema(inputs.getAllSchema());
        }
    }

    public static Schema createUnionSchema(final Iterable<Schema> schemas) {
        final Set<String> fieldNames = new HashSet<>();
        final Schema.Builder builder = Schema.builder();
        for (final Schema schema : schemas) {
            for (final Schema.Field field : schema.getFields()) {
                if (!fieldNames.contains(field.getName())) {
                    builder.withField(field);
                    fieldNames.add(field.getName());
                }
            }
        }
        return builder.build();
    }

    public static UnionFlatten flatten() {
        return new UnionFlatten();
    }

    public static UnionWithKey withKeys(final List<String> keyFields) {
        return new UnionWithKey(keyFields);
    }

    public static class UnionFlatten extends PTransform<MCollectionTuple, PCollection<MElement>> {

        private static final Logger LOG = LoggerFactory.getLogger(UnionFlatten.class);

        private List<PCollection<?>> waits;
        private Strategy strategy;

        public UnionFlatten withWaits(List<PCollection<?>> waits) {
            if(this.waits == null) {
                this.waits = new ArrayList<>();
            }
            if(waits != null) {
                this.waits.addAll(waits);
            }
            return this;
        }

        public UnionFlatten withStrategy(final Strategy strategy) {
            this.strategy = strategy;
            return this;
        }


        @Override
        public PCollection<MElement> expand(MCollectionTuple inputs) {

            PCollection<MElement> outputs;
            if(strategy == null || strategy.isDefault()) {
                try {
                    outputs = merge(inputs, null);
                } catch (final IllegalStateException e) {
                    LOG.info("set strategy for inputs");
                    outputs = merge(inputs, strategy);
                }
            } else {
                outputs = merge(inputs, strategy);
            }

            if(waits == null || waits.isEmpty()) {
                return outputs;
            }

            return outputs
                    .apply("Wait", Wait.on(waits))
                    .setCoder(outputs.getCoder());
        }

        private static PCollection<MElement> merge(
                final MCollectionTuple inputs,
                final Strategy strategy) {

            if(inputs.size() == 1) {
                final PCollection<MElement> input = inputs.getSinglePCollection();
                if(strategy == null) {
                    return input;
                }
                return input
                        .apply("WithWindow", strategy.createWindow())
                        .setCoder(input.getCoder());
            }

            final Coder<MElement> unionCoder = ElementCoder.of(inputs.getAllSchema());

            PCollectionList<MElement> list = PCollectionList.empty(inputs.getPipeline());
            int index = 0;
            for(Map.Entry<String, PCollection<MElement>> entry : inputs.getAll().entrySet()) {
                PCollection<MElement> element = entry.getValue();
                if(strategy != null) {
                    element = element.apply("WithWindow_" + entry.getKey(), strategy.createWindow());
                }
                final PCollection<MElement> unified = element
                        .apply("Union_" + entry.getKey(), ParDo.of(new UnionDoFn(index)))
                        .setCoder(unionCoder);
                list = list.and(unified);
                index++;
            }
            return list.apply("Flatten", Flatten.pCollections());
        }

        private static class UnionDoFn extends DoFn<MElement, MElement> {

            private final int index;

            UnionDoFn(final int index) {
                this.index = index;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                final MElement output = MElement.of(index, input.getType(), input.getValue(), input.getEpochMillis());
                c.output(output);
            }

        }

    }

    public static class UnionWithKey extends PTransform<MCollectionTuple, PCollection<KV<String, MElement>>> {

        private static final Logger LOG = LoggerFactory.getLogger(UnionWithKey.class);

        private final List<String> commonFields;

        private List<PCollection<?>> waits;
        private Strategy strategy;

        public UnionWithKey(final List<String> commonFields) {
            this.commonFields = commonFields;
        }

        public UnionWithKey withWaits(List<PCollection<?>> waits) {
            if(this.waits == null) {
                this.waits = new ArrayList<>();
            }
            if(waits != null) {
                this.waits.addAll(waits);
            }
            return this;
        }

        public UnionWithKey withStrategy(final Strategy strategy) {
            this.strategy = strategy;
            return this;
        }

        @Override
        public PCollection<KV<String, MElement>> expand(MCollectionTuple inputs) {

            final SerializableFunction<MElement, String> groupKeysFunction = SchemaUtil.createGroupKeysFunction(MElement::getAsString, commonFields);
            PCollection<KV<String, MElement>> outputs;
            if(inputs.size() == 1) {
                outputs = inputs.getSinglePCollection()
                        .apply("WithKey", ParDo.of(new UnionDoFn(0, groupKeysFunction)))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(inputs.getSingleSchema())));
            } else if(strategy == null || strategy.isDefault()) {
                try {
                    outputs = merge(inputs, groupKeysFunction, null);
                } catch (final IllegalStateException e) {
                    outputs = merge(inputs, groupKeysFunction, strategy);
                }
            } else {
                outputs = merge(inputs, groupKeysFunction, strategy);
            }

            if(waits == null || waits.isEmpty()) {
                return outputs;
            }

            return outputs
                    .apply("Wait", Wait.on(waits))
                    .setCoder(outputs.getCoder());
        }

        private static PCollection<KV<String, MElement>> merge(
                final MCollectionTuple inputs,
                final SerializableFunction<MElement, String> groupKeysFunction,
                final Strategy strategy) {

            final Coder<MElement> unionCoder = ElementCoder.of(inputs.getAllSchema());
            final Coder<KV<String, MElement>> outputCoder = KvCoder.of(StringUtf8Coder.of(), unionCoder);

            PCollectionList<KV<String, MElement>> list = PCollectionList.empty(inputs.getPipeline());
            int index = 0;
            for(Map.Entry<String, PCollection<MElement>> entry : inputs.getAll().entrySet()) {
                PCollection<MElement> element = entry.getValue();
                if(strategy != null) {
                    element = element.apply("WithWindow_" + entry.getKey(), strategy.createWindow());
                }
                final PCollection<KV<String, MElement>> unified = element
                        .apply("Union_" + entry.getKey(), ParDo.of(new UnionDoFn(index, groupKeysFunction)))
                        .setCoder(outputCoder);
                list = list.and(unified);
                index++;
            }

            return list
                    .apply("Flatten", Flatten.pCollections())
                    .setCoder(outputCoder);
        }

        private static class UnionDoFn extends DoFn<MElement, KV<String, MElement>> {

            private final int index;
            private final SerializableFunction<MElement, String> groupKeysFunction;

            UnionDoFn(final int index, final SerializableFunction<MElement, String> groupKeysFunction) {
                this.index = index;
                this.groupKeysFunction = groupKeysFunction;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                final MElement output = MElement.of(index, input.getType(), input.getValue(), input.getEpochMillis());
                final String key = groupKeysFunction.apply(input);
                c.output(KV.of(key, output));
            }

        }

    }

}
