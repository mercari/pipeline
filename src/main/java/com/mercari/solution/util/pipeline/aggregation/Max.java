package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import net.objecthunter.exp4j.Expression;

import java.util.*;

public class Max implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private String field;
    private String expression;
    private String condition;

    private Boolean opposite;
    private Boolean ignore;

    private transient Expression exp;
    private transient Set<String> variables;
    private transient Filter.ConditionNode conditionNode;

    @Override
    public Boolean getIgnore() {
        return this.ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
    }


    public static Max of(final String name,
                         final Schema inputSchema,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore,
                         final Boolean opposite) {

        final Max max = new Max();
        max.name = name;
        max.field = field;
        max.expression = expression;
        max.condition = condition;
        max.ignore = ignore;
        max.opposite = opposite;

        max.outputFields = new ArrayList<>();
        if (field != null) {
            final Schema.Field inputField = inputSchema.getField(field);
            max.outputFields.add(Schema.Field.of(name, inputField.getFieldType()));
        } else {
            max.outputFields.add(Schema.Field.of(name, Schema.FieldType.FLOAT64));
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
    public List<Schema.Field> getOutputFields() {
        return this.outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        final Object prevValue = accumulator.get(name);
        final Object inputValue;
        if(field != null) {
            inputValue = input.getPrimitiveValue(field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }

        final Object maxNext = Aggregator.max(prevValue, inputValue, opposite);
        accumulator.put(name, maxNext);
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Object stateValue = base.get(name);
        final Object accumValue = input.get(name);
        final Object max = Aggregator.max(stateValue, accumValue, opposite);
        base.put(name, max);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        final Object maxValue = accumulator.get(name);
        outputs.put(name, maxValue);
        return outputs;
    }

}
