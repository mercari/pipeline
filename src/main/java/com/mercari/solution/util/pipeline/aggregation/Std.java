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

public class Std implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String field;
    private String expression;
    private String weightField;
    private String weightExpression;
    private Integer ddof; // Delta Degree of Freedom
    private Boolean outputVar;
    private String condition;

    private List<Range> ranges;

    private Boolean ignore;

    private String accumKeyAvgName;
    private String accumKeyCountName;
    private String accumKeyWeightName;
    private String outputVarName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static Std of(
            final String name,
            final List<Schema.Field> inputFields,
            final String field,
            final String expression,
            final String condition,
            final Boolean ignore,
            final JsonObject params) {

        final Std std = new Std();
        std.name = name;
        std.field = field;
        std.expression = expression;
        std.condition = condition;
        std.ignore = ignore;

        if(params.has("ddof") && params.get("ddof").isJsonPrimitive()) {
            std.ddof = params.get("ddof").getAsInt();
        } else {
            std.ddof = 1;
        }

        if(params.has("outputVar") && params.get("outputVar").isJsonPrimitive()) {
            std.outputVar = params.get("outputVar").getAsBoolean();
        } else {
            std.outputVar = false;
        }

        std.inputFields = new ArrayList<>();

        if(field != null) {
            std.inputFields.add(Schema.Field.of(field, Schema.getField(inputFields, field).getFieldType()));
        } else {
            for(final String variable : ExpressionUtil.estimateVariables(expression)) {
                std.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }
        if(params.has("weightField")) {
            std.weightField = params.get("weightField").getAsString();
            std.inputFields.add(Schema.Field.of(std.weightField, Schema.getField(inputFields, std.weightField).getFieldType()));
        } else if(params.has("weightExpression")) {
            std.weightExpression = params.get("weightExpression").getAsString();
            for(final String variable : ExpressionUtil.estimateVariables(std.weightExpression)) {
                std.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }

        std.outputFieldType = Schema.FieldType.FLOAT64.withNullable(true);
        /*
        if(std.outputVar) {
            std.outputVarName = std.outputFieldName("var");
            std.outputFields.add(Schema.Field.of(std.outputVarName, Schema.FieldType.FLOAT64.withNullable(true)));
        }
         */

        std.accumKeyAvgName = name + ".avg";
        std.accumKeyCountName = name + ".count";
        std.accumKeyWeightName = name + ".weight";

        return std;
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
        final Double inputValue;
        if(field != null) {
            inputValue = input.getAsDouble(field);
        } else {
            inputValue = AggregateFunction.eval(this.exp, variables, input);
        }
        if(inputValue == null || Double.isNaN(inputValue)) {
            return accumulator;
        }
        Double inputWeight;
        if(weightField != null) {
            inputWeight = input.getAsDouble(weightField);
        } else if(weightExpression != null) {
            inputWeight = AggregateFunction.eval(this.weightExp, weightVariables, input);
        } else {
            inputWeight = 1D;
        }
        if(inputWeight != null && !Double.isNaN(inputWeight) && inputWeight < 0) {
            inputWeight = Math.abs(inputWeight);
        }

        return add(accumulator, inputValue, inputWeight);
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseCount = Optional.ofNullable(base.getAsDouble(accumKeyCountName)).orElse(0D);
        final Double inputCount = Optional.ofNullable(input.getAsDouble(accumKeyCountName)).orElse(0D);
        if(inputCount == 0) {
            return base;
        } else if(baseCount == 0) {
            base.put(name, input.getAsDouble(name));
            base.put(accumKeyCountName, inputCount);
            base.put(accumKeyAvgName, input.getAsDouble(accumKeyAvgName));
            base.put(accumKeyWeightName, input.getAsDouble(accumKeyWeightName));
            return base;
        } else if(baseCount == 1 || inputCount == 1) {
            final Double inputValue;
            final Double inputWeight;
            if(inputCount == 1) {
                inputValue = input.getAsDouble(accumKeyAvgName);
                inputWeight = input.getAsDouble(accumKeyWeightName);
                return add(base, inputValue, inputWeight);
            } else {
                inputValue = base.getAsDouble(accumKeyAvgName);
                inputWeight = base.getAsDouble(accumKeyWeightName);
                final Accumulator mergedInput = add(input, inputValue, inputWeight);
                base.put(name, mergedInput.getAsDouble(name));
                base.put(accumKeyCountName, mergedInput.getAsDouble(accumKeyCountName));
                base.put(accumKeyAvgName, mergedInput.getAsDouble(accumKeyAvgName));
                base.put(accumKeyWeightName, mergedInput.getAsDouble(accumKeyWeightName));
                return base;
            }
        }

        final Double baseAvg = base.getAsDouble(accumKeyAvgName);
        final Double baseWeight = base.getAsDouble(accumKeyWeightName);
        final Double inputAvg = input.getAsDouble(accumKeyAvgName);
        final Double inputWeight = input.getAsDouble(accumKeyWeightName);
        final Double avg = AggregateFunction.avg(baseAvg, baseWeight, inputAvg, inputWeight);
        final Double count = baseCount + inputCount;
        final Double weight = Optional.ofNullable(baseWeight).orElse(0D) + Optional.ofNullable(inputWeight).orElse(0D);
        base.put(accumKeyAvgName, avg);
        base.put(accumKeyCountName, count);
        base.put(accumKeyWeightName, weight);

        final Double baseVar = Optional.ofNullable(base.getAsDouble(name)).orElse(0D);
        final Double inputVar = Optional.ofNullable(input.getAsDouble(name)).orElse(0D);
        base.put(name, baseVar + inputVar);

        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values) {

        final Double var = accumulator.getAsDouble(name);
        final Double weight = Optional.ofNullable(accumulator.getAsDouble(accumKeyWeightName)).orElse(0D);
        if(var != null && weight != 0 && weight - ddof > 0) {
            return Math.sqrt(var / (weight - ddof));
            /*
            values.put(name, Math.sqrt(var / (weight - ddof)));
            if(outputVar) {
                values.put(outputVarName, var);
            }
             */
        } else {
            return null;
        }
    }

    private Accumulator add(final Accumulator accumulator, final Double inputValue, final Double inputWeight) {
        if(inputValue == null || Double.isNaN(inputValue)) {
            return accumulator;
        }

        final Double prevAvg = accumulator.getAsDouble(accumKeyAvgName);
        final Double prevWeight = Optional.ofNullable(accumulator.getAsDouble(accumKeyWeightName)).orElse(0D);
        final Double nextAvg = AggregateFunction.avg(prevAvg, prevWeight, inputValue, inputWeight);
        final Double nextWeight = prevWeight + Optional.ofNullable(inputWeight).orElse(0D);
        accumulator.put(accumKeyAvgName, nextAvg);
        accumulator.put(accumKeyCountName, Optional.ofNullable(accumulator.getAsDouble(accumKeyCountName)).orElse(0D) + 1D);
        accumulator.put(accumKeyWeightName, nextWeight);

        double deltaPrev = inputValue - Optional.ofNullable(prevAvg).orElse(0D);
        double deltaNext = inputValue - Optional.ofNullable(nextAvg).orElse(0D);
        final Double prevVar = Optional.ofNullable(accumulator.getAsDouble(name)).orElse(0D);
        final Double nextVar = prevVar + (deltaPrev * deltaNext);

        accumulator.put(name, nextVar);

        return accumulator;
    }

}
