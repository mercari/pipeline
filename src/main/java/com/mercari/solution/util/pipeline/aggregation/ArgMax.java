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

public class ArgMax implements Aggregator {

    private List<Schema.Field> outputFields;
    private Schema.Field comparingValueField;

    private String name;
    private List<String> fields;
    private String comparingField;
    private String comparingExpression;
    private String condition;

    private Boolean ignore;

    private String separator;

    private Boolean expandOutputName;
    private Boolean outputComparingValueField;
    private Boolean opposite;


    private transient Expression comparingExp;
    private transient Set<String> comparingVariables;
    private transient Filter.ConditionNode conditionNode;

    public ArgMax() {

    }

    @Override
    public Boolean getIgnore() {
        return this.ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
    }

    public static ArgMax of(final String name,
                            final Schema inputSchema,
                            final String condition,
                            final Boolean ignore,
                            final String separator,
                            final JsonObject params) {

        return of(name, inputSchema, condition, ignore, separator, params, false);
    }

    public static ArgMax of(final String name,
                            final Schema inputSchema,
                            final String condition,
                            final Boolean ignore,
                            final String separator,
                            final JsonObject params,
                            final boolean opposite) {

        final ArgMax argmax = new ArgMax();
        argmax.name = name;
        argmax.condition = condition;
        argmax.ignore = ignore;
        argmax.separator = separator;

        argmax.fields = new ArrayList<>();
        if(params.has("fields") && params.get("fields").isJsonArray()) {
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                argmax.fields.add(element.getAsString());
            }
            argmax.expandOutputName = true;
        } else if(params.has("field")) {
            argmax.fields.add(params.get("field").getAsString());
            argmax.expandOutputName = false;
        }

        final String comparingValueFieldName;
        if(params.has("comparingValueField")) {
            argmax.outputComparingValueField = true;
            comparingValueFieldName = params.get("comparingValueField").getAsString();
        } else {
            argmax.outputComparingValueField = false;
            comparingValueFieldName = argmax.outputKeyName("comparingValue");
        }

        if(params.has("comparingField")) {
            argmax.comparingField = params.get("comparingField").getAsString();
        } else {
            argmax.comparingField = null;
        }
        if(params.has("comparingExpression")) {
            argmax.comparingExpression = params.get("comparingExpression").getAsString();
        } else {
            argmax.comparingExpression = null;
        }

        argmax.opposite = opposite;

        final String comparingKeyName = name + "." + comparingValueFieldName;
        if(argmax.comparingField != null) {
            final Schema.Field inputField = inputSchema.getField(argmax.comparingField);
            argmax.comparingValueField = Schema.Field
                    .of(comparingValueFieldName, inputField.getFieldType().withNullable(true))
                    .withOptions(Map.of(
                            FIELD_OPTION_ORIGINAL_FIELD, comparingValueFieldName,
                            FIELD_OPTION_ACCUMULATOR_KEY, comparingKeyName));
        } else {
            argmax.comparingValueField = Schema.Field
                    .of(comparingValueFieldName, Schema.FieldType.FLOAT64.withNullable(true))
                    .withOptions(Map.of(
                            FIELD_OPTION_ACCUMULATOR_KEY, comparingKeyName));
        }

        argmax.outputFields = argmax.createOutputFields(inputSchema, argmax.outputComparingValueField, argmax.comparingValueField);

        return argmax;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].name must not be null");
        }
        if(this.fields == null || this.fields.isEmpty()) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].fields size must not be zero");
        }
        if(this.comparingField == null && this.comparingExpression == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].comparingField or comparingExpression must not be null");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.comparingExpression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.comparingExpression);
            this.comparingVariables = variables;
            this.comparingExp = ExpressionUtil.createDefaultExpression(this.comparingExpression, variables);
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
    public Accumulator addInput(Accumulator accumulator, MElement element) {
        final String accumulatorComparingKeyName = Aggregator.getFieldOptionAccumulatorKey(comparingValueField);
        final Object prevComparingValue = accumulator.get(accumulatorComparingKeyName);
        final Object inputComparingValue;
        if(comparingField != null) {
            inputComparingValue = element.getPrimitiveValue(comparingField);
        } else {
            inputComparingValue = Aggregator.eval(this.comparingExp, comparingVariables, element);
        }

        if(Aggregator.compare(inputComparingValue, prevComparingValue, opposite)) {
            for(final Schema.Field field : this.outputFields) {
                if(field.getName().equals(comparingValueField.getName())) {
                    continue;
                }
                final String originalFieldName = Aggregator.getFieldOptionOriginalFieldKey(field);
                final Object fieldValue = element.getPrimitiveValue(originalFieldName);

                final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                accumulator.put(accumulatorKeyName, fieldValue);
            }

            accumulator.put(accumulatorComparingKeyName, inputComparingValue);
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(Accumulator base, Accumulator input) {
        final String accumulatorComparingKeyName = Aggregator.getFieldOptionAccumulatorKey(comparingValueField);
        final Object prevComparingValue = base.get(accumulatorComparingKeyName);
        final Object inputComparingValue = input.get(accumulatorComparingKeyName);

        if(Aggregator.compare(inputComparingValue, prevComparingValue, opposite)) {
            for(final Schema.Field field : outputFields) {
                final String accumulatorFieldKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                final Object fieldValue = input.get(accumulatorFieldKeyName);
                base.put(accumulatorFieldKeyName, fieldValue);
            }
            base.put(accumulatorComparingKeyName, inputComparingValue);
        }
        return base;
    }

    @Override
    public Map<String, Object> extractOutput(Accumulator accumulator, Map<String, Object> values) {
        for(final Schema.Field field : outputFields) {
            final String accumulatorFieldKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
            final Object fieldPrimitiveValue = accumulator.get(accumulatorFieldKeyName);
            values.put(field.getName(), fieldPrimitiveValue);
        }
        if(outputComparingValueField) {
            final String accumulatorComparingKeyName = Aggregator.getFieldOptionAccumulatorKey(comparingValueField);
            final Object fieldPrimitiveValue = accumulator.get(accumulatorComparingKeyName);
            values.put(comparingValueField.getName(), fieldPrimitiveValue);
        }
        return values;
    }

    private List<Schema.Field> createOutputFields(final Schema inputSchema,
                                                  final Boolean outputComparingValueField,
                                                  final Schema.Field comparingValueField) {

        final List<Schema.Field> outputFields = new ArrayList<>();
        for(final String field : fields) {
            if(!inputSchema.hasField(field)) {
                throw new IllegalArgumentException("field: " + field + " is not found in source schema: " + inputSchema);
            }
            final String fieldName = expandOutputName ? outputFieldName(field) : name;
            final String keyName = expandOutputName ? outputKeyName(field) : name;
            final Schema.Field schemaField = inputSchema.getField(field);
            outputFields.add(Schema.Field
                    .of(fieldName, schemaField.getFieldType().withNullable(true))
                    .withOptions(Map.of(
                            FIELD_OPTION_ORIGINAL_FIELD, field,
                            FIELD_OPTION_ACCUMULATOR_KEY, keyName)));
        }

        if(outputComparingValueField) {
            outputFields.add(comparingValueField);
        }

        return outputFields;
    }

    private String outputFieldName(String field) {
        return String.format("%s%s%s", name, separator, field);
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
