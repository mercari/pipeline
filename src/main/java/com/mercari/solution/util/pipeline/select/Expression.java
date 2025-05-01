package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.*;

public class Expression implements SelectFunction {

    private final String name;
    private final String expressionString;
    private final Set<String> expressionVariables;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient net.objecthunter.exp4j.Expression expression;

    Expression(String name, String expressionString, Set<String> expressionVariables, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.expressionString = expressionString;
        this.expressionVariables = expressionVariables;

        this.inputFields = new ArrayList<>();
        for(String variable : expressionVariables) {
            this.inputFields.add(Schema.Field.of(variable, Schema.FieldType.FLOAT64.withNullable(true)));
        }
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Expression of(String name, JsonObject jsonObject, boolean ignore) {
        if(!jsonObject.has("expression")) {
            throw new IllegalArgumentException("SelectField expression: " + name + " requires expression parameter");
        }
        final String expression = jsonObject.get("expression").getAsString();
        final Set<String> expressionVariables = ExpressionUtil.estimateVariables(expression);

        final String type;
        if(jsonObject.has("type")) {
            type = jsonObject.get("type").getAsString();
        } else {
            type = "float64";
        }
        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));

        return new Expression(name, expression, expressionVariables, outputFieldType, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
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
    public void setup() {
        this.expression = ExpressionUtil.createDefaultExpression(expressionString, expressionVariables);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Map<String, Double> values = new HashMap<>();
        for(final String variableName : expressionVariables) {
            final Object variable = input.get(variableName);
            final Double value = ExpressionUtil.getAsDouble(variable);
            values.put(variableName, value);
        }
        double result = this.expression
                .setVariables(values)
                .evaluate();
        return ElementSchemaUtil.getAsPrimitive(outputFieldType, result);
    }

}