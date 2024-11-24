package com.mercari.solution.util.pipeline.aggregation;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;

import java.util.*;

public class Count implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private String condition;

    private Boolean ignore;

    private transient Filter.ConditionNode conditionNode;


    @Override
    public Boolean getIgnore() {
        return this.ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
    }


    public static Count of(final String name, final String condition, final Boolean ignore) {

        final Count count = new Count();
        count.name = name;
        count.condition = condition;
        count.ignore = ignore;

        count.outputFields = new ArrayList<>();
        count.outputFields.add(Schema.Field.of(name, Schema.FieldType.INT64.withNullable(true)));

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
    public List<Schema.Field> getOutputFields() {
        return this.outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accum, final MElement input) {
        final Object countPrev = accum.get(name);
        final long countNext;
        if(countPrev == null) {
            countNext = 1L;
        } else {
            countNext = (Long) countPrev + 1L;
        }
        accum.put(name, countNext);
        return accum;
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
    public Map<String,Object> extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        final Long count = Optional
                .ofNullable((Long)accumulator.get(name))
                .orElse(0L);
        outputs.put(name, count);
        return outputs;
    }

}