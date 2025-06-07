package com.mercari.solution.util.schema.converter;

import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ProtoToJsonConverter {

    public static String convert(final DynamicMessage message) {
        return convert(message, null);
    }

    public static String convert(final DynamicMessage message, final Collection<String> fields) {
        return convertObject(message, null).toString();
    }

    public static JsonObject convertObject(final DynamicMessage message) {
        return convertObject(message, null);
    }

    public static JsonObject convertObject(final DynamicMessage message, final List<String> fields) {
        final JsonObject obj = new JsonObject();
        if(message == null) {
            return obj;
        }
        if(fields != null && !fields.isEmpty()) {
            message.getAllFields().entrySet().stream()
                    .filter(f -> fields.contains(f.getKey().getName()))
                    .forEach(f -> setValue(obj, f));
        } else {
            message.getAllFields().entrySet()
                    .forEach(f -> setValue(obj, f));
        }
        return obj;
    }

    private static void setValue(final JsonObject obj, final Map.Entry<Descriptors.FieldDescriptor, Object> entry) {
        final String fieldName = entry.getKey().getName();
        final Object value = entry.getValue();
        final boolean isNullField = value == null;
        switch (entry.getKey().getType()) {
            case BOOL -> obj.addProperty(fieldName, (Boolean) value);
            case ENUM, STRING -> obj.addProperty(fieldName, isNullField ? null : value.toString());
            case BYTES -> {
                if(isNullField) {
                    obj.addProperty(fieldName, (String)null);
                } else {
                    final byte[] bytes = ((ByteBuffer)value).array();
                    obj.addProperty(fieldName, Base64.getEncoder().encodeToString(bytes));
                }
            }
            case INT32 -> {
                obj.addProperty(fieldName, (Integer) value);
            }
            case INT64 -> {
                obj.addProperty(fieldName, (Long) value);
            }
            case FLOAT -> {
                final Float floatValue = (Float) value;
                if (isNullField || Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                    obj.addProperty(fieldName, (Float) null);
                } else {
                    obj.addProperty(fieldName, floatValue);
                }
            }
            case DOUBLE -> {
                final Double doubleValue = (Double) value;
                if (isNullField || Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    obj.addProperty(fieldName, (Double) null);
                } else {
                    obj.addProperty(fieldName, doubleValue);
                }
            }
            case MESSAGE -> {
                obj.add(fieldName, isNullField ? null : convertObject((DynamicMessage) value));
            }
            default -> obj.add(fieldName, null);
        }
    }

}
