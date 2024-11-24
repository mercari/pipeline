package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Last implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private List<String> fields;
    private String condition;

    private Boolean opposite;
    private Boolean ignore;

    private String separator;
    private String timestampKeyName;
    private Boolean expandOutputName;

    private transient Filter.ConditionNode conditionNode;

    @Override
    public Boolean getIgnore() {
        return this.ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
    }


    public static Last of(
            final String name,
            final Schema inputSchema,
            final String condition,
            final Boolean ignore,
            final String separator,
            final JsonObject params,
            final Boolean opposite) {

        final Last last = new Last();
        last.name = name;
        last.condition = condition;
        last.ignore = ignore;
        last.opposite = opposite;
        last.separator = separator;
        last.fields = new ArrayList<>();

        if(params.has("fields") && params.get("fields").isJsonArray()) {
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                last.fields.add(element.getAsString());
            }
            last.expandOutputName = true;
        } else if(params.has("field")) {
            last.fields.add(params.get("field").getAsString());
            last.expandOutputName = false;
        }

        last.outputFields = last.createOutputFields(inputSchema);
        last.timestampKeyName = name + ".timestamp";

        return last;
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
        return errorMessages;
    }

    @Override
    public void setup() {
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
        final Long currentMicros = input.getEpochMillis() * 1000L;
        final Long prevMicros = (Long) accumulator.get(timestampKeyName);

        if(Aggregator.compare(currentMicros, prevMicros, opposite)) {
            for(final Schema.Field field : this.outputFields) {
                final String originalFieldName = Aggregator.getFieldOptionOriginalFieldKey(field);
                final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                final Object fieldValue = input.getPrimitiveValue(originalFieldName);
                accumulator.put(accumulatorKeyName, fieldValue);
            }
            accumulator.put(timestampKeyName, currentMicros);
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Long timestamp = (Long) base.get(timestampKeyName);
        final Long timestampAccum = (Long) input.get(timestampKeyName);
        if(Aggregator.compare(timestampAccum, timestamp, opposite)) {
            for(final Schema.Field field : outputFields) {
                final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                final Object fieldValue = input.get(accumulatorKeyName);
                base.put(accumulatorKeyName, fieldValue);
            }
            base.put(timestampKeyName, timestampAccum);
        }
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        for(final Schema.Field field : outputFields) {
            final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
            final Object fieldPrimitiveValue = accumulator.get(accumulatorKeyName);
            outputs.put(field.getName(), fieldPrimitiveValue);
        }
        return outputs;
    }

    private List<Schema.Field> createOutputFields(final Schema inputSchema) {
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
        return outputFields;
    }

    private String outputFieldName(String field) {
        return String.format("%s%s%s", name, separator, field);
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
