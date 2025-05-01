package com.mercari.solution.util.pipeline.aggregation;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import org.joda.time.Instant;

import java.util.*;

public class Count implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String condition;

    private Boolean ignore;

    private List<Range> ranges;

    private transient Filter.ConditionNode conditionNode;


    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean ignore() {
        return Optional.ofNullable(this.ignore).orElse(false);
    }

    @Override
    public Boolean filter(final MElement element) {
        return AggregateFunction.filter(conditionNode, element);
    }

    @Override
    public List<Range> getRanges() {
        return ranges;
    }

    public static Count of(
            final String name,
            final String condition,
            final List<Range> ranges,
            final Boolean ignore) {

        final Count count = new Count();
        count.name = name;
        count.condition = condition;
        count.ranges = ranges;
        count.ignore = ignore;

        count.inputFields = new ArrayList<>();
        count.outputFieldType = Schema.FieldType.INT64.withNullable(true);

        return count;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if (name == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].name must not be null");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if (this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return null;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Integer count, final Instant timestamp) {
        final Object countPrev = accumulator.get(name);
        final long countNext;
        if(countPrev == null) {
            countNext = 1L;
        } else {
            countNext = (Long) countPrev + 1L;
        }
        accumulator.put(name, countNext);
        return accumulator;
    }

    @Override
    public Accumulator addInput(final Accumulator accum, final MElement input) {
        return addInput(accum, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator accum) {

        final Long stateValue = (Long) base.get(name);
        final Long accumValue = (Long) accum.get(name);

        final Long count = Optional.ofNullable(stateValue).orElse(0L) + Optional.ofNullable(accumValue).orElse(0L);
        base.put(name, count);
        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        return Optional
                .ofNullable((Long)accumulator.get(name))
                .orElse(0L);
    }

}