package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import net.objecthunter.exp4j.Expression;

import java.util.*;

public class Sum implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private String field;
    private String expression;
    private String condition;

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


    public static Sum of(final String name,
                         final Schema inputSchema,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore) {

        final Sum sum = new Sum();
        sum.name = name;
        sum.field = field;
        sum.expression = expression;
        sum.condition = condition;
        sum.ignore = ignore;

        sum.outputFields = new ArrayList<>();
        if (field != null) {
            final Schema.Field inputField = inputSchema.getField(field);
            sum.outputFields.add(Schema.Field.of(name, inputField.getFieldType()));
        } else {
            sum.outputFields.add(Schema.Field.of(name, Schema.FieldType.FLOAT64));
        }

        return sum;
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
        final Object inputValue;
        if(field != null) {
            inputValue = input.getPrimitiveValue(field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }
        final Object prevValue = accumulator.get(name);

        final Object sumNext = Aggregator.sum(prevValue, inputValue);
        accumulator.put(name, sumNext);
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Object stateValue = base.get(name);
        final Object accumValue = input.get(name);
        final Object sum = Aggregator.sum(stateValue, accumValue);
        base.put(name, sum);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        final Object sum = accumulator.get(name);
        if(sum == null) {
            outputs.put(name, convertNumberValue(outputFields.get(0).getFieldType(), 0D));
        } else {
            outputs.put(name, sum);
        }
        return outputs;
    }

    public static Object convertNumberValue(Schema.FieldType fieldType, Double value) {
        if(value == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case float32 -> value.floatValue();
            case float64 -> value;
            case int16 -> value.shortValue();
            case int32 -> value.intValue();
            case int64 -> value.longValue();
            default -> null;
        };
    }

}