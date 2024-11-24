package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;
import com.mercari.solution.util.coder.AccumulatorCoder;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.aggregation.Aggregator;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Aggregation {

    private static final Logger LOG = LoggerFactory.getLogger(Aggregation.class);

    public static Aggregate aggregate(
            final List<String> inputNames,
            final List<String> groupFields,
            final Strategy strategy,
            final List<Aggregators> aggregatorsList,
            final Integer fanout) {

        return new Aggregate(inputNames, groupFields, strategy, aggregatorsList, fanout);
    }

    public static Schema createOutputSchema(
            final Map<String, Schema> inputSchemas,
            final List<String> groupFields,
            final List<Aggregators> aggregatorsList) {

        final List<Schema.Field> aggregationOutputFields = new ArrayList<>();
        for(final String groupField : groupFields) {
            final Schema.Field field = inputSchemas.values().stream()
                    .findAny()
                    .map(s -> s.getField(groupField))
                    .orElseThrow();
            aggregationOutputFields.add(field);
        }

        for(final Aggregators aggregators : aggregatorsList) {
            for(final Aggregator aggregation : aggregators.getAggregators()) {
                if(aggregation.getIgnore()) {
                    continue;
                }
                final List<Schema.Field> outputFields = aggregation.getOutputFields();
                for(final Schema.Field outputField : outputFields) {
                    aggregationOutputFields.add(Schema.Field.of(outputField.getName(), outputField.getFieldType()));
                }
            }
        }

        return Schema.of(aggregationOutputFields);
    }

    public static class Aggregate extends PTransform<MCollectionTuple, PCollection<KV<String, Map<String,Object>>>> {

        private final List<String> inputNames;
        private final List<String> groupFields;
        private final Strategy strategy;
        private final List<Aggregators> aggregatorsList;
        private final Integer fanout;

        Aggregate(
                final List<String> inputNames,
                final List<String> groupFields,
                final Strategy strategy,
                final List<Aggregators> aggregatorsList,
                final Integer fanout) {

            this.inputNames = inputNames;
            this.groupFields = groupFields;
            this.strategy = strategy;
            this.aggregatorsList = aggregatorsList;
            this.fanout = fanout;
        }

        @Override
        public PCollection<KV<String, Map<String,Object>>> expand(MCollectionTuple inputs) {

            final PCollection<KV<String, MElement>> withKey = inputs
                    .apply("WithKeys", Union
                            .withKeys(groupFields)
                            .withStrategy(strategy));

            final PCollection<KV<String,Map<String, Object>>> aggregated;
            if(fanout != null) {
                aggregated = withKey
                        .apply("CombineWithFanOut", Combine
                                .<String,MElement,Map<String,Object>>perKey(
                                        AggregationCombineFn.combine(inputNames, groupFields, aggregatorsList))
                                .withHotKeyFanout(fanout));
            } else {
                aggregated = withKey
                        .apply("Combine", Combine
                                .perKey(AggregationCombineFn.combine(inputNames, groupFields, aggregatorsList)));
            }

            return aggregated
                    .setCoder(KvCoder.of(
                            StringUtf8Coder.of(),
                            MapCoder.of(
                                    StringUtf8Coder.of(),
                                    UnionMapCoder.unionValueCoder())));
        }

    }

    public static class AggregationCombineFn extends Combine.CombineFn<MElement, Accumulator, Map<String, Object>> {

        private final List<String> inputNames;
        private final List<String> groupFields;
        private final List<Aggregators> aggregatorsList;

        private transient Map<String, Aggregators> aggregatorsMap;

        public static AggregationCombineFn combine(
                final List<String> inputNames,
                final List<String> groupFields,
                final List<Aggregators> aggregatorsList) {

            return new AggregationCombineFn(inputNames, groupFields, aggregatorsList);
        }

        AggregationCombineFn(
                final List<String> inputNames,
                final List<String> groupFields,
                final List<Aggregators> aggregatorsList) {

            this.inputNames = inputNames;
            this.groupFields = groupFields;
            this.aggregatorsList = aggregatorsList;
        }

        private void init() {
            if(this.aggregatorsMap == null) {
                this.aggregatorsMap = aggregatorsList
                        .stream()
                        .peek(Aggregators::setup)
                        .collect(Collectors.toMap(
                                Aggregators::getInput,
                                a -> a));
            }
        }

        @Override
        public Accumulator createAccumulator() {
            init();
            return Accumulator.of();
        }

        @Override
        public Accumulator addInput(Accumulator accum, final MElement input) {
            init();
            final String inputName = inputNames.get(input.getIndex());
            return aggregatorsMap.get(inputName)
                    .addInput(accum, input);
        }

        @Override
        public Accumulator mergeAccumulators(final Iterable<Accumulator> accums) {
            init();
            Accumulator accumulator = createAccumulator();
            for(final Aggregators aggregators : aggregatorsMap.values()) {
                accumulator = aggregators.mergeAccumulators(accumulator, accums);
            }
            return accumulator;
        }

        @Override
        public Map<String, Object> extractOutput(final Accumulator accumulator) {

            if(accumulator == null) {
                LOG.info("Skip null aggregation result");
                return new HashMap<>();
            }
            if(accumulator.isEmpty()) {
                LOG.info("Skip empty aggregation result");
                return new HashMap<>();
            }

            Map<String, Object> values = new HashMap<>();
            // set group fields values
            for(final String groupField : groupFields) {
                final Object primitiveValue = accumulator.get(groupField);
                values.put(groupField, primitiveValue);
            }

            // set aggregation results
            init();
            for(final Aggregators aggregators : aggregatorsMap.values()) {
                values = aggregators.extractOutput(accumulator, values);
            }

            return values;
        }

        @Override
        public Coder<Accumulator> getAccumulatorCoder(CoderRegistry registry, Coder<MElement> input) {
            return AccumulatorCoder.of();
        }

    }

}
