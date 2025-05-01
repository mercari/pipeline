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

public class CurrentTimestamp implements Processor {

    private final String name;
    private final String type;

    private final String condition;
    private final Boolean ignore;

    private final Map<String, Integer> bufferSizes;

    private transient Filter.ConditionNode conditionNode;


    public CurrentTimestamp(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(params.has("type") && params.get("type").isJsonPrimitive()) {
            this.type = params.get("type").getAsString();
            if(!this.type.equals("processingTime") && !this.type.equals("eventTime")) {
                throw new IllegalArgumentException("CurrentTimestamp step: " + name + " type value must be processingTime or eventTime");
            }
        } else {
            this.type = "processingTime";
        }

        this.bufferSizes = new HashMap<>();
    }

    public static CurrentTimestamp of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new CurrentTimestamp(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.current_timestamp;
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
        outputFields.add(Schema.Field.of(name, Schema.FieldType.DATETIME.withNullable(true)));
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        return new HashMap<>();
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
        return currentTimestamp(timestamp);
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        return currentTimestamp(timestamp);
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        for(final Map<String, Object> input : inputs) {
            final Map<String, Object> output = currentTimestamp(timestamp);
            outputs.add(output);
        }
        return outputs;
    }

    private Map<String, Object> currentTimestamp(Instant eventTime) {
        final Instant timestamp;
        if("processingTime".equals(type)) {
            timestamp = Instant.now();
        } else if("eventTime".equals(type)) {
            timestamp = eventTime;
        } else {
            throw new IllegalArgumentException();
        }
        final Map<String, Object> outputs = new HashMap<>();
        outputs.put(name, timestamp.getMillis() * 1000L);
        return outputs;
    }

}