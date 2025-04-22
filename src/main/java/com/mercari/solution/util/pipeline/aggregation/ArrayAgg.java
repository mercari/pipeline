package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

public class ArrayAgg implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private Order order;
    private List<String> fields;
    private String condition;

    private List<Range> ranges;

    private Boolean ignore;
    private Boolean expandOutputName;

    private transient Filter.ConditionNode conditionNode;

    enum Order implements Serializable {
        ascending,
        descending,
        none
    }

    public static ArrayAgg of(
            final String name,
            final List<Schema.Field> inputFields,
            final String condition,
            final Boolean ignore,
            final JsonObject params) {

        final ArrayAgg arrayAgg = new ArrayAgg();
        arrayAgg.name = name;
        arrayAgg.fields = new ArrayList<>();
        arrayAgg.inputFields = new ArrayList<>();
        arrayAgg.condition = condition;
        arrayAgg.ignore = ignore;

        if(params.has("fields") && params.get("fields").isJsonArray()) {
            final List<Schema.Field> fs = new ArrayList<>();
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                final String fieldName = element.getAsString();
                final Schema.Field inputField = Schema.getField(inputFields, fieldName);
                arrayAgg.inputFields.add(inputField);
                arrayAgg.fields.add(fieldName);
                fs.add(Schema.Field.of(inputField.getName(), inputField.getFieldType()));
            }
            arrayAgg.expandOutputName = true;
            arrayAgg.outputFieldType = Schema.FieldType.array(Schema.FieldType.element(fs).withNullable(true));
        } else if(params.has("field")) {
            final String fieldName = params.get("field").getAsString();
            final Schema.Field inputField = Schema.getField(inputFields, fieldName);
            arrayAgg.inputFields.add(inputField);
            arrayAgg.fields.add(fieldName);
            arrayAgg.expandOutputName = false;
            arrayAgg.outputFieldType = Schema.FieldType.array(inputField.getFieldType().withNullable(true));
        }

        if(params.has("order")) {
            arrayAgg.order = Order.valueOf(params.get("order").getAsString());
        } else {
            arrayAgg.order = Order.none;
        }

        return arrayAgg;
    }

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

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        return errorMessages;
    }

    @Override
    public void setup() {
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
        return outputFieldType;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Instant timestamp, final Integer count) {
        for(final Schema.Field inputField : inputFields) {
            final String key = outputKeyName(inputField.getName());
            final Object value = input.getPrimitiveValue(inputField.getName());
            accumulator.append(key, value);
        }
        return accumulator;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        for(final Schema.Field inputField : inputFields) {
            final String key = outputKeyName(inputField.getName());
            final List<Object> baseList = Optional.ofNullable(base.getList(key)).orElseGet(ArrayList::new);
            final List<Object> inputList = Optional.ofNullable(input.getList(key)).orElseGet(ArrayList::new);
            baseList.addAll(inputList);
            base.put(key, baseList);
        }
        return base;
    }

    @Override
    public Object extractOutput(
            final Accumulator accumulator,
            final Map<String, Object> values) {

        if(expandOutputName) {
            final String key_ = outputKeyName(fields.getFirst());
            final int size = accumulator.getList(key_).size();
            final List<Object> output = new ArrayList<>();
            for(int i=0; i<size; i++) {
                final Map<String, Object> rowValues = new HashMap<>();
                for(final String field : fields) {
                    final String key = outputKeyName(field);
                    final List<?> list = accumulator.getList(key);
                    final Object primitiveValue = list.get(i);
                    rowValues.put(field, primitiveValue);
                }
                output.add(rowValues);
            }
            return output;
        } else {
            final String key = outputKeyName(fields.getFirst());
            return accumulator.getList(key);
        }
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
