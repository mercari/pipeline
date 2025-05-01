package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Aggregators implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Aggregators.class);

    private String input;
    private List<String> commonFields;

    private List<AggregateFunction> aggregateFunctions;

    public String getInput() {
        return input;
    }

    public List<AggregateFunction> getAggregators() {
        return aggregateFunctions;
    }

    public static Aggregators of(
            final String input,
            final List<String> commonFields,
            final Schema inputSchema,
            final JsonArray fields) {

        final Aggregators aggregators = new Aggregators();
        aggregators.input = input;
        aggregators.commonFields = commonFields;
        aggregators.aggregateFunctions = new ArrayList<>();
        for(final JsonElement element : fields) {
            final AggregateFunction aggregateFunction = AggregateFunction.of(element, inputSchema.getFields());
            aggregators.aggregateFunctions.add(aggregateFunction);
        }
        return aggregators;
    }

    public List<String> validate(int index) {
        final List<String> errorMessages = new ArrayList<>();

        if(this.input == null) {
            errorMessages.add("aggregation[" + index + "].input must not be null");
        }
        if(this.aggregateFunctions == null) {
            errorMessages.add("aggregation[" + index + "].fields must not be null");
        } else {
            for(int fieldIndex = 0; fieldIndex<this.aggregateFunctions.size(); fieldIndex++) {
                errorMessages.addAll(aggregateFunctions.get(fieldIndex).validate(index, fieldIndex));
            }
        }
        return errorMessages;
    }

    public Aggregators setup() {
        for(final AggregateFunction aggregateFunction : this.aggregateFunctions) {
            aggregateFunction.setup();
        }
        return this;
    }

    public Accumulator addInput(Accumulator accumulator, MElement input) {
        for(final String commonField : commonFields) {
            final Object primitiveValue = input.getPrimitiveValue(commonField);
            accumulator.put(commonField, primitiveValue);
        }
        for(final AggregateFunction aggregateFunction : this.aggregateFunctions) {
            if(aggregateFunction.ignore()) {
                continue;
            }
            if(!aggregateFunction.filter(input)) {
                continue;
            }
            accumulator = aggregateFunction.addInput(accumulator, input);
        }
        return accumulator;
    }

    public Accumulator mergeAccumulators(Accumulator base, final Iterable<Accumulator> accums) {
        boolean done = false;
        for (final Accumulator accum : accums) {
            if(accum.isEmpty()) {
                continue;
            }
            if(!done) {
                for(final String commonField : commonFields) {
                    final Object primitiveValue = accum.get(commonField);
                    base.put(commonField, primitiveValue);
                }
                done = true;
            }
            for(final AggregateFunction aggregateFunction : aggregateFunctions) {
                if(aggregateFunction.ignore()) {
                    continue;
                }
                base = aggregateFunction.mergeAccumulator(base, accum);
            }
        }

        return base;
    }

    public Map<String, Object> extractOutput(final Accumulator accumulator, Map<String, Object> values) {
        for(final AggregateFunction aggregateFunction : this.aggregateFunctions) {
            final Object output = aggregateFunction.extractOutput(accumulator, values);
            values.put(aggregateFunction.getName(), output);
        }
        return values;
    }

}