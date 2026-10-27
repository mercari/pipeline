package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;
import com.mercari.solution.util.coder.AccumulatorCoder;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.aggregation.AggregateFunction;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Aggregation {

    private static final Logger LOG = LoggerFactory.getLogger(Aggregation.class);

    // '$' is not valid in Avro or BigQuery field names, so the key does not collide with group or aggregation fields.
    public static final String INPUT_EPOCH_MILLIS_KEY = "$inputEpochMillis";

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
            for(final AggregateFunction aggregation : aggregators.getAggregators()) {
                if(aggregation.ignore()) {
                    continue;
                }
                aggregationOutputFields.add(Schema.Field.of(aggregation.getName(), aggregation.getOutputFieldType()));
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

            final boolean earliest = TimestampCombiner.EARLIEST.equals(strategy.getTimestampCombiner());
            final PCollection<KV<String,Map<String, Object>>> aggregated;
            if(fanout != null) {
                aggregated = withKey
                        .apply("CombineWithFanOut", Combine
                                .<String,MElement,Map<String,Object>>perKey(
                                        AggregationCombineFn.combine(inputNames, groupFields, aggregatorsList, earliest))
                                .withHotKeyFanout(fanout));
            } else {
                aggregated = withKey
                        .apply("Combine", Combine
                                .perKey(AggregationCombineFn.combine(inputNames, groupFields, aggregatorsList, earliest)));
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
        private final boolean earliest;

        private transient Map<String, Aggregators> aggregatorsMap;

        public static AggregationCombineFn combine(
                final List<String> inputNames,
                final List<String> groupFields,
                final List<Aggregators> aggregatorsList,
                final boolean earliest) {

            return new AggregationCombineFn(inputNames, groupFields, aggregatorsList, earliest);
        }

        AggregationCombineFn(
                final List<String> inputNames,
                final List<String> groupFields,
                final List<Aggregators> aggregatorsList,
                final boolean earliest) {

            this.inputNames = inputNames;
            this.groupFields = groupFields;
            this.aggregatorsList = aggregatorsList;
            this.earliest = earliest;
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
            accum.put(INPUT_EPOCH_MILLIS_KEY, selectEpochMillis(accum.get(INPUT_EPOCH_MILLIS_KEY), input.getEpochMillis()));
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
            Object epochMillis = null;
            for(final Accumulator accum : accums) {
                epochMillis = selectEpochMillis(epochMillis, accum.get(INPUT_EPOCH_MILLIS_KEY));
            }
            if(epochMillis != null) {
                accumulator.put(INPUT_EPOCH_MILLIS_KEY, epochMillis);
            }
            return accumulator;
        }

        @Override
        public Map<String, Object> extractOutput(final Accumulator accumulator) {

            if(accumulator == null) {
                LOG.info("Skip null aggregation result");
                return new HashMap<>();
            }
            final Object epochMillis = accumulator.get(INPUT_EPOCH_MILLIS_KEY);
            if(accumulator.isEmpty() || (epochMillis != null && accumulator.getMap().size() == 1 && accumulator.getSets().isEmpty())) {
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
            values.put(INPUT_EPOCH_MILLIS_KEY, epochMillis);

            return values;
        }

        private Object selectEpochMillis(final Object current, final Object candidate) {
            if(current == null) {
                return candidate;
            }
            if(candidate == null) {
                return current;
            }
            final long c = (Long) current;
            final long n = (Long) candidate;
            return earliest ? Math.min(c, n) : Math.max(c, n);
        }

        @Override
        public Coder<Accumulator> getAccumulatorCoder(CoderRegistry registry, Coder<MElement> input) {
            return AccumulatorCoder.of();
        }

    }

}
