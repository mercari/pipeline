package com.mercari.solution.util.pipeline.processing.processor.signal;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;

import java.util.*;

public class LinearFilter implements Processor {

    final String name;

    final boolean ignore;
    final String condition;

    protected final List<String> fields;

    private final Map<String, Integer> bufferSizes;

    final List<Double> b;
    final List<Double> a;

    final boolean isSingleField;

    private transient Filter.ConditionNode conditionNode;


    public static LinearFilter of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        if(!params.has("fields") || params.get("fields").isJsonNull()) {
            throw new IllegalArgumentException("LinearFilter step: " + name + ", requires input");
        }

        final List<String> fields;
        final boolean isSingleField;
        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "field", "fields");
            fields = fieldsAndIsSingle.getKey();
            isSingleField = fieldsAndIsSingle.getValue();
        } else {
            throw new IllegalArgumentException();
        }

        final List<Double> b = new ArrayList<>();
        if(params.has("b") && !params.get("b").isJsonNull() && params.get("b").isJsonArray()) {
            final JsonArray array = params.get("b").getAsJsonArray();
            for(JsonElement element : array) {
                b.add(element.getAsDouble());
            }
        }

        final List<Double> a = new ArrayList<>();
        if(params.has("a") && !params.get("a").isJsonNull() && params.get("a").isJsonArray()) {
            final JsonArray array = params.get("a").getAsJsonArray();
            for(JsonElement element : array) {
                a.add(element.getAsDouble());
            }
        }

        return new LinearFilter(name, fields, b, a, condition, ignore, isSingleField);
    }

    LinearFilter(final String name, final List<String> fields, final List<Double> b, final List<Double> a, final String condition, final boolean ignore, final boolean isSingleField) {
        this.name = name;
        this.fields = fields;
        this.b = b;
        this.a = a;
        this.condition = condition;
        this.ignore = ignore;
        this.isSingleField = isSingleField;

        this.bufferSizes = new HashMap<>();
        for(final String field : fields) {
            this.bufferSizes.put(field, b.size());
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Op getOp() {
        return Op.binning;
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
        outputFields.add(Schema.Field.of(name, Schema.FieldType.DOUBLE.withNullable(true)));
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        for(final String input : bufferSizes.keySet()) {
            final Schema.FieldType inputType = inputTypes.get(input);
            if(inputType != null) {
                bufferTypes.put(input, inputType);
            } else if(name.equals(input)) {
                bufferTypes.put(input, Schema.FieldType.DOUBLE.withNullable(true));
            } else {
                throw new IllegalArgumentException("LinearFilter step: " + name + " has no inputType for input: " + input);
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

        return null;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        throw new NotImplementedException("");
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        throw new NotImplementedException("");
    }

    /*
    public Map<String, Object> calculate(Map<String, List<Double>> state) {

        final List<Double> inputs  = Optional.ofNullable(state.get(input)).orElseGet(Lists::newArrayList);
        final List<Double> outputs = Optional.ofNullable(state.get(name)).orElseGet(() -> Lists.newArrayList(inputs));
        final Double output = filter(inputs, outputs, b, a);
        return Map.of(name, output);
    }

    public static Double filter(
            final List<Double> inputs,
            final List<Double> outputs,
            final List<Double> b,
            final List<Double> a) {

        double output = 0D;
        // FIR filter
        for(int i=0; i<b.size();i++) {
            if(i>=inputs.size()) {
                break;
            }
            output += (inputs.get(i) * b.get(i));
        }
        // IIR filter
        for(int i=0; i<a.size();i++) {
            if(i>=outputs.size()) {
                break;
            }
            output += (outputs.get(i) * a.get(i));
        }

        return output;
    }

     */

}