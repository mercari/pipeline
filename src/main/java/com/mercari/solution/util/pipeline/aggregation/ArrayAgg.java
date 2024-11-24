package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.ParameterUtil;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.*;

public class ArrayAgg implements Aggregator {

    private List<Schema.Field> outputFields;
    private String name;
    private Order order;
    private Boolean flatten;
    private String condition;
    private Boolean ignore;
    private Boolean isSingleField;

    private List<Schema.Field> inputFields;

    private transient Filter.ConditionNode conditionNode;

    enum Order implements Serializable {
        ascending,
        descending,
        none
    }

    public static ArrayAgg of(final String name,
                              final Schema inputSchema,
                              final String condition,
                              final Boolean ignore,
                              final JsonObject params) {

        final ArrayAgg arrayAgg = new ArrayAgg();
        arrayAgg.name = name;
        arrayAgg.condition = condition;
        arrayAgg.ignore = ignore;

        final List<String> fields;
        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = ParameterUtil
                    .getSingleMultiAttribute(params, "field", "fields");
            fields = fieldsAndIsSingle.getKey();
            arrayAgg.isSingleField = fieldsAndIsSingle.getValue();
        } else {
            throw new IllegalArgumentException("");
        }

        if(params.has("order")) {
            arrayAgg.order = Order.valueOf(params.get("order").getAsString());
        } else {
            arrayAgg.order = Order.none;
        }

        if(params.has("flatten")) {
            arrayAgg.flatten = params.get("flatten").getAsBoolean();
        } else {
            arrayAgg.flatten = false;
        }

        arrayAgg.inputFields = new ArrayList<>();
        for(final String field : fields) {
            final Schema.Field inputField = inputSchema.getField(field);
            final Schema.Field outputField = Schema.Field.of(field, inputField.getFieldType().withNullable(true));
            arrayAgg.inputFields.add(outputField);
        }

        arrayAgg.outputFields = new ArrayList<>();
        if(arrayAgg.isSingleField) {
            final Schema.Field inputField = inputSchema.getField(fields.get(0));
            final Schema.Field outputField = Schema.Field.of(name, Schema.FieldType.array(inputField.getFieldType().withNullable(true)));
            arrayAgg.outputFields.add(outputField);
        } else {
            if(arrayAgg.flatten) {
                for(final String field : fields) {
                    final Schema.Field inputField = inputSchema.getField(field);
                    final Schema.Field outputField = Schema.Field.of(name + "_" + field, Schema.FieldType.array(inputField.getFieldType().withNullable(true)));
                    arrayAgg.outputFields.add(outputField);
                }
            } else {
                Schema.Builder builder = Schema.builder();
                for(final String field : fields) {
                    final Schema.Field inputField = inputSchema.getField(field);
                    builder = builder.withField(field, inputField.getFieldType().withNullable(true));
                }
                final Schema.Field outputField = Schema.Field.of(name, Schema.FieldType.array(Schema.FieldType.element(builder.build()).withNullable(true)));
                arrayAgg.outputFields.add(outputField);
            }
        }

        return arrayAgg;
    }

    @Override
    public Boolean getIgnore() {
        return ignore;
    }

    @Override
    public Boolean filter(final MElement element) {
        return Aggregator.filter(conditionNode, element);
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
    public List<Schema.Field> getOutputFields() {
        return outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        for(final Schema.Field inputField : inputFields) {
            final String key = this.name + "." + inputField.getName();
            final Object value = input.getPrimitiveValue(inputField.getName());
            accumulator.add(key, value);
        }

        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        for(final Schema.Field inputField : inputFields) {
            final String key = this.name + "." + inputField.getName();
            final List<Object> baseList = Optional.ofNullable(base.getList(key)).orElseGet(ArrayList::new);
            final List<Object> inputList = Optional.ofNullable(input.getList(key)).orElseGet(ArrayList::new);
            for(Object value : inputList) {
                baseList.add(value);
            }
            base.put(name + "." + inputField.getName(), baseList);
        }
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(
            final Accumulator accumulator,
            final Map<String, Object> values) {

        if(isSingleField) {
            final Schema.Field inputField = inputFields.getFirst();
            final String key = this.name + "." + inputField.getName();
            final List<Object> list = accumulator.getList(key);
            values.put(name, list);
        } else {
            if(flatten) {
                for(final Schema.Field inputField : inputFields) {
                    final String key = this.name + "." + inputField.getName();
                    final List<Object> list = accumulator.getList(key);
                    values.put(name + "_" + inputField.getName(), list);
                }
            } else {
                final int size = accumulator.getList(this.name + "." + inputFields.getFirst().getName()).size();
                final List<Object> output = new ArrayList<>();
                for(int i=0; i<size; i++) {
                    final Map<String, Object> rowValues = new HashMap<>();
                    for(final Schema.Field inputField : inputFields) {
                        final String key = this.name + "." + inputField.getName();
                        final List<?> list = accumulator.getList(key);
                        final Object primitiveValue = list.get(i);
                        rowValues.put(inputField.getName(), primitiveValue);
                    }
                    output.add(rowValues);
                }
                values.put(name, output);
            }
        }

        return values;
    }

}
