package com.mercari.solution.util.schema.converter;

import com.google.gson.*;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class MapToJsonConverter {

    public static String convert(final Map<String, Object> primitiveValues) {
        final JsonObject jsonObject = convertObject(primitiveValues);
        return jsonObject.toString();
    }

    public static JsonObject convertObject(final Map<String, Object> primitiveValues) {
        final JsonObject jsonObject = new JsonObject();
        if(primitiveValues == null || primitiveValues.isEmpty()) {
            return jsonObject;
        }
        for(final Map.Entry<String, Object> entry : primitiveValues.entrySet()) {
            final JsonElement element = convertElement(entry.getValue());
            jsonObject.add(entry.getKey(), element);
        }
        return jsonObject;
    }

    private static JsonElement convertElement(final Object value) {
        return switch (value) {
            case Map map -> convertObject(map);
            case List list -> {
                final JsonArray array = new JsonArray();
                for(final Object object : list) {
                    final JsonElement e = convertElement(object);
                    array.add(e);
                }
                yield array;
            }
            case String string -> new JsonPrimitive(string);
            case Boolean bool -> new JsonPrimitive(bool);
            case Number number -> new JsonPrimitive(number);
            case byte[] bytes -> new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
            case ByteBuffer byteBuffer -> new JsonPrimitive(Base64.getEncoder().encodeToString(byteBuffer.array()));
            case null, default -> JsonNull.INSTANCE;
        };
    }

}
