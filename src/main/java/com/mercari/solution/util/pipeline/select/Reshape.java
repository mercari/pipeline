package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.*;

public class Reshape implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Integer> shape;
    private final List<Integer> indices;
    private final List<Integer> strides;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Reshape(
            final String name,
            final String field,
            final List<Integer> shape,
            final List<Integer> indices,
            final Schema.Field inputField,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.shape = shape;
        this.indices = indices;
        this.strides = computeStrides(shape);
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Reshape of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {

        if(!jsonObject.has("field")) {
            throw new IllegalModuleException("selectFunction reshape: " + name + " requires field parameter");
        }
        final String inputFieldName;
        final String field = jsonObject.get("field").getAsString();
        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            inputFieldName = fields[0];
        } else {
            inputFieldName = field;
        }
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(inputFieldName, inputFields);
        final Schema.Field inputField = Schema.Field.of(inputFieldName, inputFieldType);

        if(!jsonObject.has("shape") || !jsonObject.get("shape").isJsonArray()) {
            throw new IllegalModuleException("selectFunction reshape: " + name + " requires shape parameter(and must be json array)");
        }
        final JsonArray shapeArray = jsonObject.getAsJsonArray("shape");
        final List<Integer> shape = new ArrayList<>();
        for(final JsonElement shapeElement : shapeArray) {
            shape.add(shapeElement.getAsInt());
        }

        final List<Integer> indices = new ArrayList<>();
        if(jsonObject.has("indices")) {
            final JsonArray indexArray = jsonObject.getAsJsonArray("indices");
            for(final JsonElement indexElement : indexArray) {
                indices.add(indexElement.getAsInt());
            }
        }

        final Schema.FieldType outputElementFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        final Schema.FieldType outputFieldType = switch (outputElementFieldType.getType()) {
            case array -> {
                if(indices.isEmpty()) {
                    yield Schema.FieldType.matrix(outputElementFieldType.getArrayValueType(), shape);
                }
                if(indices.size() == shape.size()) {
                    yield outputElementFieldType.getArrayValueType();
                } else if(indices.size() < shape.size()) {
                    final List<Integer> newShape = new ArrayList<>();
                    for(int i=0; i<shape.size() - indices.size(); i++) {
                        newShape.add(shape.get(i));
                    }
                    yield Schema.FieldType.matrix(outputElementFieldType.getArrayValueType(), newShape);
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case matrix -> {
                if(indices.isEmpty()) {
                    yield Schema.FieldType.matrix(outputElementFieldType.getMatrixValueType(), shape);
                }
                if(indices.size() == shape.size()) {
                    yield outputElementFieldType.getMatrixValueType();
                } else if(indices.size() < shape.size()) {
                    final List<Integer> newShape = new ArrayList<>();
                    for(int i=0; i<shape.size() - indices.size(); i++) {
                        newShape.add(shape.get(i));
                    }
                    yield Schema.FieldType.matrix(outputElementFieldType.getMatrixValueType(), newShape);
                } else {
                    throw new IllegalArgumentException();
                }
            }
            default -> Schema.FieldType.matrix(outputElementFieldType, shape);
        };

        return new Reshape(name, field, shape, indices, inputField, outputFieldType, ignore);
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

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Object inputValue = ElementSchemaUtil.getValue(input, field);
        if(inputValue == null) {
            return null;
        }
        final Object value =  switch (inputFields.getFirst().getFieldType().getType()) {
            case array, matrix -> inputValue;
            default -> throw new IllegalArgumentException();
        };

        if(indices.isEmpty()) {
            return value;
        }

        return switch (value) {
            case List c -> get(c, indices);
            case Object o -> get(List.of(o), indices);
        };
    }

    private static int computeTotalSize(List<Integer> shape) {
        int size = 1;
        for (int dim : shape) {
            size *= dim;
        }
        return size;
    }

    private static List<Integer> computeStrides(List<Integer> shape) {
        List<Integer> strides = new ArrayList<>(shape.size());
        for (int i = 0; i < shape.size(); i++) {
            strides.add(1);
        }

        strides.set(shape.size() - 1, 1);

        for (int i = shape.size() - 2; i >= 0; i--) {
            strides.set(i, strides.get(i + 1) * shape.get(i + 1));
        }

        return strides;
    }

    private int flattenIndex(List<Integer> indices) {
        if (indices.size() > shape.size()) {
            throw new IllegalArgumentException("Too many indices");
        }

        int flatIndex = 0;
        for (int i = 0; i < indices.size(); i++) {
            if (indices.get(i) < 0 || indices.get(i) >= shape.get(i)) {
                throw new IndexOutOfBoundsException("Index out of bounds");
            }
            flatIndex += indices.get(i) * strides.get(i);
        }

        return flatIndex;
    }

    public Object get(
            final List<Object> data,
            final List<Integer> indices) {

        if(indices.size() == shape.size()) {
            return data.get(flattenIndex(indices));
        }

        final List<Integer> newShape = new ArrayList<>();
        for (int i = indices.size(); i < shape.size(); i++) {
            newShape.add(shape.get(i));
        }

        final int newSize = computeTotalSize(newShape);
        final List<Object> newData = new ArrayList<>(newSize);

        copySubList(data, newData, 0, flattenIndex(indices), indices.size(), newShape);

        return newData;
    }

    private void copySubList(
            final List<Object> data,
            final List<Object> dest,
            final int destOffset,
            final int srcOffset,
            final int startDim,
            final List<Integer> subShape) {

        if (startDim >= shape.size()) {
            if (destOffset < dest.size()) {
                dest.set(destOffset, data.get(srcOffset));
            } else {
                dest.add(data.get(srcOffset));
            }
            return;
        }

        if (subShape.isEmpty()) {
            if (destOffset < dest.size()) {
                dest.set(destOffset, data.get(srcOffset));
            } else {
                dest.add(data.get(srcOffset));
            }
            return;
        }

        int subSize = 1;
        for (int i = 1; i < subShape.size(); i++) {
            subSize *= subShape.get(i);
        }

        for (int i = 0; i < subShape.getFirst(); i++) {
            copySubList(
                    data,
                    dest,
                    destOffset + i * subSize,
                    srcOffset + i * strides.get(startDim),
                    startDim + 1,
                    subShape.subList(1, subShape.size()));
        }
    }
}