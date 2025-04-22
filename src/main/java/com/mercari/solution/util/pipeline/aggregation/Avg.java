package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;

import java.util.*;

public class Avg implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String field;
    private String expression;
    private String weightField;
    private String weightExpression;
    private String condition;

    private List<Range> ranges;

    private Boolean ignore;

    private String weightKeyName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static Avg of(
            final String name,
            final List<Schema.Field> inputFields,
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

        avg.inputFields = new ArrayList<>();
        if (field != null) {
            final Schema.Field inputField = Schema.getField(inputFields, field);
            avg.inputFields.add(Schema.Field.of(field, inputField.getFieldType()));
        } else {
            for(final String variable : ExpressionUtil.estimateVariables(expression)) {
                avg.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }

        if(params.has("weightField")) {
            avg.weightField = params.get("weightField").getAsString();
            final Schema.Field inputField = Schema.getField(inputFields, avg.weightField);
            avg.inputFields.add(Schema.Field.of(avg.weightField, inputField.getFieldType()));
        } else if(params.has("weightExpression")) {
            avg.weightExpression = params.get("weightExpression").getAsString();
            for(final String variable : ExpressionUtil.estimateVariables(avg.weightExpression)) {
                avg.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }

        avg.outputFieldType = Schema.FieldType.FLOAT64.withNullable(true);

        avg.weightKeyName = name + ".weight";

        return avg;
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
        return AggregateFunction.filter(conditionNode, element);
    }

    @Override
    public List<Range> getRanges() {
        return ranges;
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
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Instant timestamp, final Integer count) {
        final Double prevAvg = (Double) accumulator.get(name);
        final Double prevWeight = Optional.ofNullable((Double)accumulator.get(weightKeyName)).orElse(0D);
        final Double inputValue;
        if(field != null) {
            inputValue = input.getAsDouble(field);
        } else {
            inputValue = AggregateFunction.eval(this.exp, variables, input);
        }
        final Double inputWeight;
        if(weightField != null) {
            inputWeight = input.getAsDouble(weightField);
        } else if(weightExpression != null) {
            inputWeight = AggregateFunction.eval(this.weightExp, weightVariables, input);
        } else {
            inputWeight = 1D;
        }

        final Double avgNext = AggregateFunction.avg(prevAvg, prevWeight, inputValue, inputWeight);
        accumulator.put(name, avgNext);
        if(inputValue != null) {
            accumulator.put(weightKeyName, prevWeight + inputWeight);
        } else {
            accumulator.put(weightKeyName, prevWeight);
        }
        return accumulator;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseAvg = (Double) base.get(name);
        final Double baseWeight = (Double) base.get(weightKeyName);
        final Double inputAvg = (Double) input.get(name);
        final Double inputWeight = (Double) input.get(weightKeyName);
        final Double avg = AggregateFunction.avg(baseAvg, baseWeight, inputAvg, inputWeight);
        final Double weight = Optional.ofNullable(baseWeight).orElse(0D) + Optional.ofNullable(inputWeight).orElse(0D);
        base.put(name, avg);
        base.put(weightKeyName, weight);
        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values) {

        return accumulator.get(name);
    }

}
