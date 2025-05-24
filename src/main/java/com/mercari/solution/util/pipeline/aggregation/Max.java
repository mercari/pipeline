package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;

import java.util.*;

public class Max implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String field;
    private String expression;
    private String condition;

    private Boolean opposite;

    private Range range;

    private Boolean ignore;

    private transient Expression exp;
    private transient Set<String> variables;
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
        return StatefulFunction.filter(conditionNode, element);
    }

    @Override
    public Range getRange() {
        return range;
    }


    public static Max of(
            final String name,
            final List<Schema.Field> inputFields,
            final String field,
            final String expression,
            final String condition,
            final Range range,
            final Boolean ignore,
            final Boolean opposite) {

        final Max max = new Max();
        max.name = name;
        max.field = field;
        max.expression = expression;
        max.condition = condition;
        max.range = range;
        max.ignore = ignore;
        max.opposite = opposite;

        max.inputFields = new ArrayList<>();
        if (field != null) {
            final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
            max.inputFields.add(Schema.Field.of(field, inputFieldType));
            max.outputFieldType = inputFieldType;
        } else {
            for(final String variable : ExpressionUtil.estimateVariables(expression)) {
                final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(variable, inputFields);
                max.inputFields.add(Schema.Field.of(variable, inputFieldType));
            }
            max.outputFieldType = Schema.FieldType.FLOAT64.withNullable(true);
        }

        return max;
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
        if(this.expression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.expression);
            this.variables = variables;
            this.exp = ExpressionUtil.createDefaultExpression(this.expression, variables);
        }
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
        final Object inputValue;
        if(field != null) {
            inputValue = input.getPrimitiveValue(field);
        } else {
            inputValue = ExpressionUtil.eval(this.exp, variables, input);
        }

        final Object prevValue = accumulator.get(name);
        final Object maxNext = AggregateFunction.max(prevValue, inputValue, opposite);
        accumulator.put(name, maxNext);

        return accumulator;

    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Object stateValue = base.get(name);
        final Object accumValue = input.get(name);
        final Object max = AggregateFunction.max(stateValue, accumValue, opposite);
        base.put(name, max);
        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        return accumulator.get(name);
    }

}
