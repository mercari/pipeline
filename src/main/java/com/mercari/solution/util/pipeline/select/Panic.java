package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Panic implements SelectFunction {

    private final String name;
    private final String message;
    private final String condition;
    private final Double rate;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Filter.ConditionNode conditionNode;
    private transient Random random;

    Panic(String name, String message, String condition, Double rate, boolean ignore) {
        this.name = name;
        this.message = message;
        this.condition = condition;
        this.rate = rate;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.FLOAT64;
        this.ignore = ignore;
    }

    public static Panic of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("message")) {
            throw new IllegalArgumentException("SelectField panic: " + name + " requires message parameter");
        }
        final String message = jsonObject.get("message").toString();

        final String condition;
        if(jsonObject.has("condition")) {
            condition = jsonObject.get("condition").toString();
        } else {
            condition = null;
        }

        final Double rate;
        if(jsonObject.has("rate")) {
            JsonElement rateElement = jsonObject.get("rate");
            if(!rateElement.isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField panic: " + name + " rate must be primitive but: " + rateElement);
            }
            final JsonPrimitive ratePrimitive = rateElement.getAsJsonPrimitive();
            if(!ratePrimitive.isNumber()) {
                throw new IllegalArgumentException("SelectField panic: " + name + " rate must be number but: " + rateElement);
            }
            rate = ratePrimitive.getAsDouble();
            if(rate > 1 || rate < 0) {
                throw new IllegalArgumentException("SelectField panic: " + name + " rate must be between 0 and 1, but: " + rate);
            }
        } else {
            rate = 1.0D;
        }

        return new Panic(name, message, condition, rate, ignore);
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
        if(condition != null) {
            this.conditionNode = Filter.parse(condition);
        }
        this.random = new Random();
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(!Filter.filter(conditionNode, input)) {
            return -1D;
        }
        final double value = random.nextDouble();
        if(value <= rate) {
            throw new RuntimeException(message);
        }
        return value;
    }
}
