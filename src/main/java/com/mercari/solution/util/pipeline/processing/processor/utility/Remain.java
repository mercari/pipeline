package com.mercari.solution.util.pipeline.processing.processor.utility;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Remain implements Processor {

    private final String name;
    private final String field;
    private final String condition;
    private final Boolean ignore;

    private final Map<String, Integer> bufferSizes;

    private transient Filter.ConditionNode conditionNode;


    public Remain(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(!params.has("field") || !params.get("field").isJsonPrimitive()) {
            this.field = name;
        } else {
            this.field = params.get("field").getAsString();
        }

        this.bufferSizes = new HashMap<>();
        this.bufferSizes.put(this.field, 1);
    }

    public static Remain of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Remain(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.remain;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public boolean filter(Map<String, Object> input) {
        if(condition == null) {
            return true;
        }
        return Filter.filter(this.conditionNode, input);
    }

    @Override
    public SizeUnit getSizeUnit() {
        return SizeUnit.count;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final List<Schema.Field> outputFields = new ArrayList<>();
        if(!inputTypes.containsKey(field)) {
            throw new IllegalArgumentException("Not found field: " + field + " in inputTypes: " + inputTypes);
        }
        outputFields.add(Schema.Field.of(name, inputTypes.get(field)));
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        for(final String input : bufferSizes.keySet()) {
            final Schema.FieldType inputType = inputTypes.get(input);
            if(inputType != null) {
                bufferTypes.put(input, inputType);
            } else {
                throw new IllegalArgumentException("remain processor: " + name + " has no inputType for input: " + input);
            }
        }
        return bufferTypes;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return this.bufferSizes;
    }

    @Override
    public void setup() {
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {
        final Map<String, Object> outputs = new HashMap<>();
        outputs.put(name, buffer.get(field, 0));
        return outputs;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> outputs = new HashMap<>();
        outputs.put(name, input.get(field));
        return outputs;
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        for(final Map<String, Object> input : inputs) {
            final Map<String, Object> output = new HashMap<>();
            //output.put(name, value);
            //outputs.add(output);
            //TODO
        }
        return outputs;
    }

}
