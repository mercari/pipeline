package com.mercari.solution.util.pipeline.select.stateful;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;

import java.util.*;

public class Lag implements StatefulFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String expression;
    private String condition;

    private Range range;

    private Boolean ignore;

    private transient Expression exp;
    private transient Map<Integer,Set<String>> reverseArrayIndexes;
    private transient Filter.ConditionNode conditionNode;

    public static Lag of(
            final String name,
            final List<Schema.Field> inputFields,
            final String expression,
            final String condition,
            final Boolean ignore) {

        final Lag lag = new Lag();
        lag.name = name;
        lag.expression = expression;
        lag.condition = condition;
        lag.ignore = ignore;

        final Map<String,Set<Integer>> arrayIndexes = ExpressionUtil.extractArrayIndexes(expression);
        lag.reverseArrayIndexes = ExpressionUtil.reverseArrayIndexes(arrayIndexes);

        lag.inputFields = new ArrayList<>();
        for(final String variable : ExpressionUtil.estimateVariables(expression)) {
            final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(variable, inputFields);
            lag.inputFields.add(Schema.Field.of(variable, inputFieldType));
        }
        lag.outputFieldType = Schema.FieldType.FLOAT64;

        lag.range = new Range();
        lag.range.type = Range.RangeType.count;

        lag.range.count = ExpressionUtil.maxArrayIndex(lag.reverseArrayIndexes) + 1;

        return lag;
    }

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
        return StatefulFunction.filter(conditionNode, element);
    }

    @Override
    public Range getRange() {
        return range;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if (name == null) {
            errorMessages.add("selectFunction[" + parent + "].fields[" + index + "].name must not be null");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        final Map<String,Set<Integer>> arrayIndexes = ExpressionUtil.extractArrayIndexes(expression);
        this.reverseArrayIndexes = ExpressionUtil.reverseArrayIndexes(arrayIndexes);

        final Set<String> variables = new HashSet<>();
        for(final Map.Entry<Integer,Set<String>> entry : reverseArrayIndexes.entrySet()) {
            for(final String variable : entry.getValue()) {
                if(entry.getKey() > 0) {
                    final String variableName = ExpressionUtil.replaceArrayFieldName(variable, entry.getKey());
                    variables.add(variableName);
                } else {
                    variables.add(variable);
                }
            }
        }

        final String expressionText = ExpressionUtil.replaceArrayExpression(expression);
        this.exp = ExpressionUtil.createDefaultExpression(expressionText, variables);
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
        final Map<String, Object> variables = accumulator.getMap(name);
        for(final String variable : reverseArrayIndexes.getOrDefault(count, Set.of())) {
            final Double variableValue = Optional.ofNullable(input.getAsDouble(variable)).orElse(Double.NaN);
            if(count > 0) {
                final String variableName = ExpressionUtil.replaceArrayFieldName(variable, count);
                variables.put(variableName, variableValue);
            } else {
                variables.put(variable, variableValue);
            }
        }

        accumulator.put(name, variables);
        return accumulator;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        final Map variables = accumulator.getMap(name);
        if(variables == null) {
            return null;
        }
        try {
            return exp.setVariables((Map<String, Double>) variables).evaluate();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

}
