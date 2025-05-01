package com.mercari.solution.util.pipeline.processing.processor.utility;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;

import java.util.*;

public class Expression implements Processor {

    private final String name;

    private final String expression;

    private final String condition;
    private final Boolean ignore;

    private final Set<String> variables;

    private final Map<String, Integer> bufferSizes;

    private transient net.objecthunter.exp4j.Expression exp;
    private transient Filter.ConditionNode conditionNode;


    public Expression(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(!params.has("expression") || !params.get("expression").isJsonPrimitive()) {
            throw new IllegalArgumentException("Expression step: " + name + " requires expression");
        }

        this.expression = params.get("expression").getAsString();
        this.variables = ExpressionUtil.estimateVariables(expression);
        this.bufferSizes = ExpressionUtil.extractBufferSizes(variables, 0, DEFAULT_VARIABLE_NAME_SEPARATOR);
    }

    public static Expression of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Expression(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
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
                throw new IllegalArgumentException("Expression step: " + name + " has no inputType for input: " + input);
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
        this.exp = ExpressionUtil.createDefaultExpression(this.expression, this.variables);
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {

        final Map<String, Double> values = new HashMap<>();
        for(final String input : this.bufferSizes.keySet()) {
            if(variables.contains(input)) {
                final Double firstLagValue = buffer.getAsDouble(input, 0);
                values.put(input, firstLagValue);
            }
            final int size = this.bufferSizes.get(input) + 1;
            for(int index=0; index<size; index++) {
                final String variableName = Processor.createVariableName(input, DEFAULT_VARIABLE_NAME_SEPARATOR, index);
                if(variables.contains(variableName)) {
                    final Double variableValue = buffer.getAsDouble(input, index);
                    values.put(variableName, variableValue);
                }
            }
        }

        final Map<String, Object> outputs = new HashMap<>();
        try {
            final Double output = exp.setVariables(values).evaluate();
            if(Double.isNaN(output) || Double.isInfinite(output)) {
                outputs.put(name, null);
            } else {
                outputs.put(name, output);
            }
        } catch (final NullPointerException | ArithmeticException e) {
            outputs.put(name, null);
        }
        return outputs;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        throw new NotImplementedException("");
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        throw new NotImplementedException("");
    }

}