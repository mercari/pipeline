package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.*;

@Transform.Module(name="compare")
public class CompareTransform extends Transform {

    private static class Parameters implements Serializable {

        private List<String> outputs;
        private List<String> primaryKeyFields;

        public void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(primaryKeyFields == null || primaryKeyFields.isEmpty()) {
                errorMessages.add("compare transform module[" + name + "] primaryKeyFields parameter is required");
            }
            if (!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            if (this.outputs == null) {
                this.outputs = new ArrayList<>();
            }
            if (this.primaryKeyFields == null) {
                this.primaryKeyFields = new ArrayList<>();
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final PCollection<MElement> results = inputs
                .apply("Union", Union.withKeys(parameters.primaryKeyFields)
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("GroupByKeys", GroupByKey.create())
                .apply("Compare", ParDo.of(new CompareDoFn(getName(), inputSchema, inputs.getAllInputs())))
                .setCoder(ElementCoder.of(createComparingRowResultSchema()));


        return MCollectionTuple
                .of(results, createComparingRowResultSchema());
    }

    private static class CompareDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {

        private final String table;
        private final List<String> inputNames;
        private final Schema inputSchema;
        private final Schema outputSchema = createComparingRowResultSchema();

        CompareDoFn(final String table, final Schema inputSchema, final List<String> inputNames) {
            this.table = table;
            this.inputSchema = inputSchema;
            this.inputNames = inputNames;
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {

            final String key = c.element().getKey();
            final List<MElement> unionValues = Lists.newArrayList(c.element().getValue());
            final List<Integer> indexes = unionValues.stream().map(MElement::getIndex).toList();

            final List<String> missingInputs = new ArrayList<>();
            final List<String> duplicatedInputs = new ArrayList<>();

            final long indexUniqueSize = indexes.stream().distinct().count();
            if(inputNames.size() != indexUniqueSize) {
                final Set<String> n = new HashSet<>(inputNames);
                for(final Integer index : indexes) {
                    n.remove(inputNames.get(index));
                }
                missingInputs.addAll(n);
            }
            if(indexes.size() > indexUniqueSize) {
                duplicatedInputs.add("");
            }
            final List<Map<String,Object>> valuesList = unionValues.stream().map(MElement::asPrimitiveMap).toList();

            final List<MElement> differences = new ArrayList<>();

            for(final Schema.Field field : inputSchema.getFields()) {
                final long count = valuesList.stream()
                        .map(v -> v.get(field.getName()))
                        .map(v -> v instanceof byte[] ? Base64.getEncoder().encodeToString((byte[])v) : v)
                        .distinct()
                        .count();
                if(count != 1) {
                    final Map<String, String> differenceValues = new HashMap<>();
                    for(final MElement unionValue : unionValues) {
                        final String value = unionValue.getAsString(field.getName());
                        final String inputName = inputNames.get(unionValue.getIndex());
                        differenceValues.put(inputName, value);
                    }

                    final MElement difference = MElement.builder()
                            .withPrimitiveValue("field", field.getName())
                            .withPrimitiveValue("values", differenceValues)
                            .build();
                    differences.add(difference);
                }
            }

            if(!missingInputs.isEmpty() || !duplicatedInputs.isEmpty() || !differences.isEmpty()) {
                final MElement result = MElement.builder()
                        .withPrimitiveValue("table", table)
                        .withPrimitiveValue("keys", key)
                        .withPrimitiveValue("missingInputs", missingInputs)
                        .withPrimitiveValue("duplicatedInputs", duplicatedInputs)
                        .withPrimitiveValue("differences", differences)
                        .build();

                c.output(result);
                LOG.error("NG table: {}, for key: {}", table, key);
            }
        }

    }

    private static Schema createComparingRowResultSchema() {
        return Schema.builder()
                .withField("table", Schema.FieldType.STRING)
                .withField("keys", Schema.FieldType.STRING)
                .withField("missingInputs", Schema.FieldType.STRING)
                .withField("duplicatedInputs", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("differences", Schema.FieldType.array(
                        Schema.FieldType.element(Schema.builder()
                                .withField("field", Schema.FieldType.STRING)
                                .withField("values", Schema.FieldType.map(Schema.FieldType.STRING))
                                .build())))
                .build();
    }
}