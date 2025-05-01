package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Nullif implements SelectFunction {

    private final String name;
    private final String field;
    private final String condition;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Filter.ConditionNode conditionNode;

    Nullif(String name, String field, String condition, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.condition = condition;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Nullif of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("condition")) {
            throw new IllegalArgumentException("SelectField nullif: " + name + " requires condition parameter");
        }
        final String condition = jsonObject.get("condition").toString();

        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField nullif: " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField nullif: " + name + " missing inputField: " + field);
        }
        fields.add(Schema.Field.of(field, inputFieldType));
        final Filter.ConditionNode conditionNode = Filter.parse(condition);
        for(final String variable : conditionNode.getRequiredVariables()) {
            final Schema.FieldType variableInputFieldType = ElementSchemaUtil.getInputFieldType(variable, inputFields);
            if(variableInputFieldType == null) {
                throw new IllegalArgumentException("SelectField nullif: " + name + " missing condition input variable: " + field);
            }
            fields.add(Schema.Field.of(variable, variableInputFieldType));
        }

        return new Nullif(name, field, condition, fields, inputFieldType, ignore);
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
        this.conditionNode = Filter.parse(condition);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(Filter.filter(conditionNode, input)) {
            return null;
        } else {
            return input.get(field);
        }
    }

}
