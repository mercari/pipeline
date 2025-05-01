package com.mercari.solution.util.schema;

import com.mercari.solution.util.DateTimeUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.Relationship;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class Neo4jSchemaUtil implements Serializable {

    public static Row convert(final Schema schema, final Map<String, Object> values) {
        final Row.Builder builder = Row.withSchema(schema);
        Row.FieldValueBuilder fieldBuilder = builder.withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final Object value = getRowValue(field.getType(), values.get(field.getName()));
            fieldBuilder = fieldBuilder.withFieldValue(field.getName(), value);
        }
        return fieldBuilder.build();
    }

    public static GenericRecord convert(final org.apache.avro.Schema schema, final Map<String, Object> values) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final org.apache.avro.Schema.Field field : schema.getFields()) {
            final Object value = getRecordValue(field.schema(), values.get(field.name()));
            builder.set(field.name(), value);
        }
        return builder.build();
    }

    private static Object getRowValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }

        switch (fieldType.getTypeName()) {
            case STRING -> {
                if (value instanceof String) {
                    return value;
                } else {
                    return value.toString();
                }
            }
            case BOOLEAN -> {
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof String) {
                    return Boolean.valueOf((String) value);
                } else if (value instanceof Long) {
                    return ((Long) value) > 0;
                } else if (value instanceof Integer) {
                    return ((Integer) value) > 0;
                } else if (value instanceof Double) {
                    return ((Double) value) > 0;
                } else if (value instanceof Float) {
                    return ((Float) value) > 0;
                } else {
                    throw new IllegalStateException("Could not convert to boolean: " + value);
                }
            }
            case INT32 -> {
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof Long) {
                    return ((Long) value).intValue();
                } else if (value instanceof Double) {
                    return ((Double) value).intValue();
                } else if (value instanceof Float) {
                    return ((Float) value).intValue();
                } else if (value instanceof String) {
                    return Integer.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1 : 0;
                } else {
                    throw new IllegalStateException("Could not convert to integer: " + value);
                }
            }
            case INT64 -> {
                if (value instanceof Long) {
                    return value;
                } else if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Double) {
                    return ((Double) value).longValue();
                } else if (value instanceof Float) {
                    return ((Float) value).longValue();
                } else if (value instanceof String) {
                    return Long.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1L : 0L;
                } else {
                    throw new IllegalStateException("Could not convert to long: " + value);
                }
            }
            case FLOAT -> {
                if (value instanceof Float) {
                    return value;
                } else if (value instanceof Double) {
                    return ((Double) value).floatValue();
                } else if (value instanceof Long) {
                    return ((Long) value).floatValue();
                } else if (value instanceof Integer) {
                    return ((Integer) value).floatValue();
                } else if (value instanceof String) {
                    return Float.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1F : 0F;
                } else {
                    throw new IllegalStateException("Could not convert to float: " + value);
                }
            }
            case DOUBLE -> {
                if (value instanceof Double) {
                    return value;
                } else if (value instanceof Float) {
                    return ((Float) value).doubleValue();
                } else if (value instanceof Long) {
                    return ((Long) value).doubleValue();
                } else if (value instanceof Integer) {
                    return ((Integer) value).doubleValue();
                } else if (value instanceof String) {
                    return Double.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1D : 0D;
                } else {
                    throw new IllegalStateException("Could not convert to float: " + value);
                }
            }
            case DATETIME -> {
                if (value instanceof String) {
                    return DateTimeUtil.toJodaInstant((String) value);
                } else if (value instanceof Long) {
                    return Instant.ofEpochMilli(((Long) value) / 1000L);
                } else {
                    return value.toString();
                }
            }
            case ROW -> {
                /*
                if (value instanceof Node) {
                    final Node node = (Node) value;
                    return convert(fieldType.getRowSchema(), node.getAllProperties());
                } else if (value instanceof Relationship) {
                    final Relationship relationship = (Relationship) value;
                    return convert(fieldType.getRowSchema(), relationship.getAllProperties());
                } else if (value instanceof Map) {
                    return convert(fieldType.getRowSchema(), (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException("Could not convert to row: " + value);
                }

                 */
                throw new RuntimeException();
            }
            case ARRAY, ITERABLE -> {
                if (value instanceof List) {
                    return ((List) value).stream()
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .collect(Collectors.toList());

                } else if(value instanceof float[]) {
                    final List<Float> floats = new ArrayList<>();
                    for(float f : (float[]) value) {
                        floats.add(f);
                    }
                    return floats.stream()
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .toList();
                } else if(value instanceof double[]) {
                    return Arrays.stream((double[]) value)
                            .boxed()
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .toList();
                } else if(value instanceof int[]) {
                    return Arrays.stream((int[]) value)
                            .boxed()
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .toList();
                } else if(value instanceof long[]) {
                    return Arrays.stream((long[]) value)
                            .boxed()
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .toList();
                } else if(value instanceof String[]) {
                    return Arrays.stream((String[]) value)
                            .map(o -> getRowValue(fieldType.getCollectionElementType(), o))
                            .toList();
                } else {
                    throw new IllegalStateException("Could not convert to list: " + value);
                }
            }
            default ->
                    throw new IllegalStateException("Not supported type: " + fieldType.getTypeName() + " for value: " + value);
        }

    }

    private static Object getRecordValue(final org.apache.avro.Schema fieldSchema, final Object value) {
        if (value == null) {
            switch (AvroSchemaUtil.unnestUnion(fieldSchema).getType()) {
                case ARRAY:
                    return new ArrayList<>();
                default:
                    return null;
            }
        }

        switch (AvroSchemaUtil.unnestUnion(fieldSchema).getType()) {
            case ENUM, STRING -> {
                return value.toString();
            }
            case BOOLEAN -> {
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof String) {
                    return Boolean.valueOf((String) value);
                } else if (value instanceof Long) {
                    return ((Long) value) > 0;
                } else if (value instanceof Integer) {
                    return ((Integer) value) > 0;
                } else if (value instanceof Double) {
                    return ((Double) value) > 0;
                } else if (value instanceof Float) {
                    return ((Float) value) > 0;
                } else {
                    throw new IllegalStateException("Could not convert to boolean: " + value);
                }
            }
            case INT -> {
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof Long) {
                    return ((Long) value).intValue();
                } else if (value instanceof Double) {
                    return ((Double) value).intValue();
                } else if (value instanceof Float) {
                    return ((Float) value).intValue();
                } else if (value instanceof String) {
                    return Integer.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1 : 0;
                } else {
                    throw new IllegalStateException("Could not convert to integer: " + value);
                }
            }
            case LONG -> {
                if (value instanceof Long) {
                    return value;
                } else if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Double) {
                    return ((Double) value).longValue();
                } else if (value instanceof Float) {
                    return ((Float) value).longValue();
                } else if (value instanceof String) {
                    return Long.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1L : 0L;
                } else {
                    throw new IllegalStateException("Could not convert to long: " + value);
                }
            }
            case FLOAT -> {
                if (value instanceof Float) {
                    return value;
                } else if (value instanceof Double) {
                    return ((Double) value).floatValue();
                } else if (value instanceof Long) {
                    return ((Long) value).floatValue();
                } else if (value instanceof Integer) {
                    return ((Integer) value).floatValue();
                } else if (value instanceof String) {
                    return Float.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1F : 0F;
                } else {
                    throw new IllegalStateException("Could not convert to float: " + value);
                }
            }
            case DOUBLE -> {
                if (value instanceof Double) {
                    return value;
                } else if (value instanceof Float) {
                    return ((Float) value).doubleValue();
                } else if (value instanceof Long) {
                    return ((Long) value).doubleValue();
                } else if (value instanceof Integer) {
                    return ((Integer) value).doubleValue();
                } else if (value instanceof String) {
                    return Double.valueOf((String) value);
                } else if (value instanceof Boolean) {
                    return (Boolean) value ? 1D : 0D;
                } else {
                    throw new IllegalStateException("Could not convert to double: " + value);
                }
            }
            case RECORD -> {
                /*
                if (value instanceof Node) {
                    final Node node = (Node) value;
                    return convert(fieldSchema, node.getAllProperties());
                } else if (value instanceof Relationship) {
                    final Relationship relationship = (Relationship) value;
                    return convert(fieldSchema, relationship.getAllProperties());
                } else if (value instanceof Map) {
                    return convert(fieldSchema, (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException("Could not convert to row: " + value);
                }
                 */
                throw new RuntimeException();
            }
            case ARRAY -> {
                if (value instanceof List) {
                    return ((List) value).stream()
                            .map(o -> getRecordValue(fieldSchema.getElementType(), o))
                            .collect(Collectors.toList());

                } else if(value instanceof float[]) {
                    final List<Float> floats = new ArrayList<>();
                    for(float f : (float[]) value) {
                        floats.add(f);
                    }
                    return floats.stream()
                            .map(f -> getRecordValue(fieldSchema.getElementType(), f))
                            .toList();
                } else if(value instanceof double[]) {
                    return Arrays.stream((double[]) value)
                            .boxed()
                            .map(f -> getRecordValue(fieldSchema.getElementType(), f))
                            .toList();
                } else if(value instanceof int[]) {
                    return Arrays.stream((int[]) value)
                            .boxed()
                            .map(f -> getRecordValue(fieldSchema.getElementType(), f))
                            .toList();
                } else if(value instanceof long[]) {
                    return Arrays.stream((long[]) value)
                            .boxed()
                            .map(f -> getRecordValue(fieldSchema.getElementType(), f))
                            .toList();
                } else if(value instanceof String[]) {
                    return Arrays.stream((String[]) value)
                            .map(f -> getRecordValue(fieldSchema.getElementType(), f))
                            .toList();
                } else {
                    throw new IllegalStateException("Could not convert to list: " + value + ", class: " + value.getClass());
                }
            }
            default -> throw new IllegalStateException("Not supported type: " + fieldSchema + " for value: " + value);
        }
    }

}
