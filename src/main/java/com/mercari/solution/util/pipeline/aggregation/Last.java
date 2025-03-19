package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import org.joda.time.Instant;

import java.util.*;

public class Last implements Aggregator {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private List<String> fields;
    private String condition;

    private Boolean opposite;
    private Boolean ignore;

    private String timestampKeyName;
    private Boolean expandOutputName;

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
        return Aggregator.filter(conditionNode, element);
    }


    public static Last of(
            final String name,
            final Schema inputSchema,
            final String condition,
            final Boolean ignore,
            final JsonObject params,
            final Boolean opposite) {

        final Last last = new Last();
        last.name = name;
        last.condition = condition;
        last.ignore = ignore;
        last.opposite = opposite;
        last.fields = new ArrayList<>();
        last.inputFields = new ArrayList<>();

        if(params.has("fields") && params.get("fields").isJsonArray()) {
            final List<Schema.Field> fs = new ArrayList<>();
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                final Schema.Field inputField = inputSchema.getField(element.getAsString());
                last.inputFields.add(inputField);
                last.fields.add(element.getAsString());
                fs.add(Schema.Field.of(inputField.getName(), inputField.getFieldType()));
            }
            last.expandOutputName = true;
            last.outputFieldType = Schema.FieldType.element(fs);
        } else if(params.has("field")) {
            final Schema.Field inputField = inputSchema.getField(params.get("field").getAsString());
            last.inputFields.add(inputField);
            last.fields.add(params.get("field").getAsString());
            last.expandOutputName = false;
            last.outputFieldType = inputField.getFieldType();
        }

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
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        final Long currentMicros = input.getEpochMillis() * 1000L;
        final Long prevMicros = (Long) accumulator.get(timestampKeyName);

        if(Aggregator.compare(currentMicros, prevMicros, opposite)) {
            for(final String field : this.fields) {
                final String accumulatorKeyName = outputKeyName(field);
                final Object fieldValue = input.getPrimitiveValue(field);
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
            for(final String field : fields) {
                final String accumulatorKeyName = outputKeyName(field);
                final Object fieldValue = input.get(accumulatorKeyName);
                base.put(accumulatorKeyName, fieldValue);
            }
            base.put(timestampKeyName, timestampAccum);
        }
        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator, final Map<String, Object> outputs) {
        if(expandOutputName) {
            final Map<String, Object> output = new HashMap<>();
            for(final String field : fields) {
                final String accumulatorKeyName = outputKeyName(field);
                final Object fieldPrimitiveValue = accumulator.get(accumulatorKeyName);
                output.put(field, fieldPrimitiveValue);
            }
            return output;
        } else {
            final String accumulatorKeyName = outputKeyName(fields.getFirst());
            return accumulator.get(accumulatorKeyName);
        }
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
