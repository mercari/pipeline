package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.*;

public class Unnest implements Serializable {

    private final String flattenField;

    private Unnest(final String flattenField) {
        this.flattenField = flattenField;
    }

    public static Unnest of(final String flattenField) {
        return new Unnest(flattenField);
    }

    public static Schema createSchema(final List<Schema.Field> inputFields, final String flattenField) {
        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : inputFields) {
            if(field.getName().equals(flattenField)) {
                if(!Schema.Type.array.equals(field.getFieldType().getType())) {
                    throw new IllegalArgumentException("flattenField: " + flattenField + " type: " + field.getFieldType() + " is not array type");
                }
                builder.withField(field.getName(), field.getFieldType().getArrayValueType().withNullable(true));
            } else {
                builder.withField(field);
            }
        }
        return builder.build();
    }

    public void setup() {

    }

    public boolean useUnnest() {
        return flattenField != null;
    }

    public List<Map<String, Object>> unnest(final MElement input) {
        if(input == null) {
            return new ArrayList<>();
        }
        return unnest(input.asPrimitiveMap());
    }

    public List<Map<String, Object>> unnest(final Map<String, Object> primitiveValues) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        if (flattenField == null || primitiveValues == null || primitiveValues.isEmpty()) {
            outputs.add(primitiveValues);
            return outputs;
        }

        final List<?> flattenList = Optional
                .ofNullable((List<?>) primitiveValues.get(flattenField))
                .orElseGet(ArrayList::new);
        if (flattenList.isEmpty()) {
            final Map<String, Object> flattenValues = new HashMap<>(primitiveValues);
            flattenValues.put(flattenField, null);
            outputs.add(flattenValues);
        } else {
            for (final Object value : flattenList) {
                final Map<String, Object> flattenValues = new HashMap<>(primitiveValues);
                flattenValues.put(flattenField, value);
                outputs.add(flattenValues);
            }
        }
        return outputs;
    }

}
