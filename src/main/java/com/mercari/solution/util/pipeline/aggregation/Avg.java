package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import net.objecthunter.exp4j.Expression;

import java.util.*;

public class Avg implements Aggregator {

    private List<Schema.Field> outputFields;
    private String name;
    private String field;
    private String expression;
    private String weightField;
    private String weightExpression;
    private String condition;

    private Boolean ignore;

    private String weightKeyName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static Avg of(final String name,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore,
                         final JsonObject params) {

        final Avg avg = new Avg();
        avg.name = name;
        avg.field = field;
        avg.expression = expression;
        avg.condition = condition;
        avg.ignore = ignore;

        avg.outputFields = new ArrayList<>();
        avg.outputFields.add(Schema.Field.of(name, Schema.FieldType.FLOAT64.withNullable(true)));

        if(params.has("weightField")) {
            avg.weightField = params.get("weightField").getAsString();
        } else if(params.has("weightExpression")) {
            avg.weightExpression = params.get("weightExpression").getAsString();
        }

        avg.weightKeyName = name + ".weight";

        return avg;
    }

    @Override
    public Boolean getIgnore() {
        return ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.expression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.expression);
            this.variables = variables;
            this.exp = ExpressionUtil.createDefaultExpression(this.expression, variables);
        }
        if(this.weightExpression != null) {
            final Set<String> weightVariables = ExpressionUtil.estimateVariables(this.weightExpression);
            this.weightVariables = weightVariables;
            this.weightExp = ExpressionUtil.createDefaultExpression(this.weightExpression, weightVariables);
        }
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public List<Schema.Field> getOutputFields() {
        return outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        final Double prevAvg = (Double) accumulator.get(name);
        final Double prevWeight = Optional.ofNullable((Double)accumulator.get(weightKeyName)).orElse(0D);
        final Double inputValue;
        if(field != null) {
            inputValue = input.getAsDouble(field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }
        final Double inputWeight;
        if(weightField != null) {
            inputWeight = input.getAsDouble(weightField);
        } else if(weightExpression != null) {
            inputWeight = Aggregator.eval(this.weightExp, weightVariables, input);
        } else {
            inputWeight = 1D;
        }

        final Double avgNext = Aggregator.avg(prevAvg, prevWeight, inputValue, inputWeight);
        accumulator.put(name, avgNext);
        if(inputValue != null) {
            accumulator.put(weightKeyName, prevWeight + inputWeight);
        } else {
            accumulator.put(weightKeyName, prevWeight);
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseAvg = (Double) base.get(name);
        final Double baseWeight = (Double) base.get(weightKeyName);
        final Double inputAvg = (Double) input.get(name);
        final Double inputWeight = (Double) input.get(weightKeyName);
        final Double avg = Aggregator.avg(baseAvg, baseWeight, inputAvg, inputWeight);
        final Double weight = Optional.ofNullable(baseWeight).orElse(0D) + Optional.ofNullable(inputWeight).orElse(0D);
        base.put(name, avg);
        base.put(weightKeyName, weight);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values) {

        final Double avg = (Double) accumulator.get(name);
        values.put(name, avg);
        return values;
    }

}
