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

public class ArgMax implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String comparingKeyName;

    private String name;
    private List<String> fields;
    private String comparingField;
    private String comparingExpression;
    private String condition;

    private List<Range> ranges;

    private Boolean ignore;

    private Boolean expandOutputName;
    private Boolean opposite;


    private transient Expression comparingExp;
    private transient Set<String> comparingVariables;
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
        return AggregateFunction.filter(conditionNode, element);
    }

    @Override
    public List<Range> getRanges() {
        return ranges;
    }

    public static ArgMax of(
            final String name,
            final List<Schema.Field> inputFields,
            final String condition,
            final Boolean ignore,
            final JsonObject params) {

        return of(name, inputFields, condition, ignore, params, false);
    }

    public static ArgMax of(
            final String name,
            final List<Schema.Field> inputFields,
            final String condition,
            final Boolean ignore,
            final JsonObject params,
            final boolean opposite) {

        final ArgMax argmax = new ArgMax();
        argmax.name = name;
        argmax.condition = condition;
        argmax.ignore = ignore;
        argmax.fields = new ArrayList<>();

        argmax.inputFields = new ArrayList<>();

        if(params.has("fields") && params.get("fields").isJsonArray()) {
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                final String f = element.getAsString();
                if(argmax.fields.contains(f)) {
                    continue;
                }
                argmax.fields.add(f);
                final Schema.Field inputField = Schema.getField(inputFields, f);
                argmax.inputFields.add(Schema.Field.of(f, inputField.getFieldType()));
            }
            argmax.expandOutputName = true;
            argmax.outputFieldType = Schema.FieldType.element(new ArrayList<>(argmax.inputFields));
        } else if(params.has("field")) {
            final String f = params.get("field").getAsString();
            argmax.fields.add(f);
            final Schema.FieldType inputFieldType = Schema.getField(inputFields, f).getFieldType();
            argmax.inputFields.add(Schema.Field.of(f, inputFieldType));
            argmax.expandOutputName = false;
            argmax.outputFieldType = inputFieldType;
        }

        if(params.has("comparingField")) {
            argmax.comparingField = params.get("comparingField").getAsString();
            final Schema.Field inputField = Schema.getField(inputFields, argmax.comparingField);
            argmax.inputFields.add(Schema.Field.of(argmax.comparingField, inputField.getFieldType()));
        } else {
            argmax.comparingField = null;
        }
        if(params.has("comparingExpression")) {
            argmax.comparingExpression = params.get("comparingExpression").getAsString();
            for(final String variable : ExpressionUtil.estimateVariables(argmax.comparingExpression)) {
                argmax.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        } else {
            argmax.comparingExpression = null;
        }

        argmax.opposite = opposite;
        argmax.comparingKeyName = name + ".___comparingValue";

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
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return null;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        System.out.println(outputFieldType);
        return outputFieldType;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Instant timestamp, final Integer count) {
        final Object prevComparingValue = accumulator.get(comparingKeyName);
        final Object inputComparingValue;
        if(comparingField != null) {
            inputComparingValue = input.getPrimitiveValue(comparingField);
        } else {
            inputComparingValue = AggregateFunction.eval(this.comparingExp, comparingVariables, input);
        }

        if(AggregateFunction.compare(inputComparingValue, prevComparingValue, opposite)) {
            for(final String field : this.fields) {
                final Object fieldValue = input.getPrimitiveValue(field);
                final String accumulatorKeyName = outputKeyName(field);
                accumulator.put(accumulatorKeyName, fieldValue);
            }
            accumulator.put(comparingKeyName, inputComparingValue);
        }
        return accumulator;
    }

    @Override
    public Accumulator addInput(Accumulator accumulator, MElement element) {
        return addInput(accumulator, element);
    }

    @Override
    public Accumulator mergeAccumulator(Accumulator base, Accumulator input) {
        final Object prevComparingValue = base.get(comparingKeyName);
        final Object inputComparingValue = input.get(comparingKeyName);

        if(AggregateFunction.compare(inputComparingValue, prevComparingValue, opposite)) {
            for(final String field : fields) {
                final String accumulatorFieldKeyName = outputKeyName(field);
                final Object fieldValue = input.get(accumulatorFieldKeyName);
                base.put(accumulatorFieldKeyName, fieldValue);
            }
            base.put(comparingKeyName, inputComparingValue);
        }
        return base;
    }

    @Override
    public Object extractOutput(Accumulator accumulator, Map<String, Object> values) {
        if(expandOutputName) {
            final Map<String, Object> output = new HashMap<>();
            for(final String field : fields) {
                final String accumulatorFieldKeyName = outputKeyName(field);
                final Object fieldPrimitiveValue = accumulator.get(accumulatorFieldKeyName);
                output.put(field, fieldPrimitiveValue);
            }
            return output;
        } else {
            final String accumulatorFieldKeyName = outputKeyName(fields.getFirst());
            return accumulator.get(accumulatorFieldKeyName);
        }
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
