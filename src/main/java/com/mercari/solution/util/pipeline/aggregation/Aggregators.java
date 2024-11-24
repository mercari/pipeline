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

    private List<Aggregator> aggregators;

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    public void setAggregators(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public static Aggregators of(
            final String input,
            final List<String> commonFields,
            final Schema inputSchema,
            final JsonArray fields) {

        final Aggregators aggregators = new Aggregators();
        aggregators.input = input;
        aggregators.commonFields = commonFields;
        aggregators.aggregators = new ArrayList<>();
        for(final JsonElement element : fields) {
            final Aggregator aggregator = Aggregator.of(element, inputSchema);
            aggregators.aggregators.add(aggregator);
        }
        return aggregators;
    }

    public List<String> validate(int index) {
        final List<String> errorMessages = new ArrayList<>();

        if(this.input == null) {
            errorMessages.add("aggregation[" + index + "].input must not be null");
        }
        if(this.aggregators == null) {
            errorMessages.add("aggregation[" + index + "].fields must not be null");
        } else {
            for(int fieldIndex=0; fieldIndex<this.aggregators.size(); fieldIndex++) {
                errorMessages.addAll(aggregators.get(fieldIndex).validate(index, fieldIndex));
            }
        }
        return errorMessages;
    }

    public Aggregators setup() {
        for(final Aggregator aggregator : this.aggregators) {
            aggregator.setup();
        }
        return this;
    }

    public Accumulator addInput(Accumulator accumulator, MElement input) {
        for(final String commonField : commonFields) {
            final Object primitiveValue = input.getPrimitiveValue(commonField);
            accumulator.put(commonField, primitiveValue);
        }
        for(final Aggregator aggregator : this.aggregators) {
            if(aggregator.getIgnore()) {
                continue;
            }
            if(!aggregator.filter(input)) {
                continue;
            }
            accumulator = aggregator.addInput(accumulator, input);
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
            for(final Aggregator aggregator : aggregators) {
                if(aggregator.getIgnore()) {
                    continue;
                }
                base = aggregator.mergeAccumulator(base, accum);
            }
        }

        return base;
    }

    public Map<String, Object> extractOutput(final Accumulator accumulator, Map<String, Object> values) {
        for(final Aggregator aggregator : this.aggregators) {
            values = aggregator.extractOutput(accumulator, values);
        }
        return values;
    }

}