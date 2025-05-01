package com.mercari.solution.util.schema.converter;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class JsonToMapConverter {

    public static Map<String, Object> convert(final JsonElement json) {
        final Map<String, Object> map = new HashMap<>();
        if(json == null) {
            return map;
        } else if(json.isJsonNull()) {
            return map;
        } else if(json.isJsonPrimitive()) {
            map.put("value", json.getAsJsonPrimitive());
            return map;
        } else if(json.isJsonObject()) {
            for(final Map.Entry<String, JsonElement> entry :json.getAsJsonObject().entrySet()) {
                map.put(entry.getKey(), getValue(entry.getValue()));
            }
        } else if(json.isJsonArray()) {
            final List<Object> values = new ArrayList<>();
            for(final JsonElement element : json.getAsJsonArray()) {
                values.add(getValue(element));
            }
            map.put("values", values);
        }

        return map;
    }

    public static Object getAsPrimitiveValue(Schema.FieldType fieldType, JsonElement element) {
        return switch (fieldType.getType()) {
            case bool -> element.getAsBoolean();
            case string, json -> {
                if(element.isJsonPrimitive()) {
                    yield element.getAsString();
                } else {
                    yield element.toString();
                }
            }
            case int32 -> element.getAsInt();
            case int64 -> element.getAsLong();
            case float32 -> element.getAsFloat();
            case float64 -> element.getAsDouble();
            case date -> DateTimeUtil.toEpochDay(element.getAsString());
            case time -> DateTimeUtil.toMicroOfDay(element.getAsString());
            case timestamp -> DateTimeUtil.toEpochMicroSecond(element.getAsString());
            case element, map -> {
                if(element.isJsonObject()) {
                    yield convert(element);
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case array -> {
                if(element.isJsonArray()) {
                    List<Object> list = new ArrayList<>();
                    for(final JsonElement e : element.getAsJsonArray()) {
                        list.add(getAsPrimitiveValue(fieldType.getArrayValueType(), e));
                    }
                    yield list;
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case enumeration -> fieldType.getSymbolIndex(element.getAsString());
            case bytes -> ByteBuffer.wrap(Base64.getDecoder().decode(element.getAsString()));
            default -> throw new IllegalArgumentException();
        };
    }

    private static Object getValue(JsonElement element) {
        if(element == null) {
            return null;
        } else if(element.isJsonNull()) {
            return null;
        } else if(element.isJsonPrimitive()) {
            final JsonPrimitive primitive = element.getAsJsonPrimitive();
            if(primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else if(primitive.isString()) {
                return primitive.getAsString();
            } else if(primitive.isNumber()) {
                if(NumberUtils.isDigits(primitive.getAsString())) {
                    return primitive.getAsLong();
                } else {
                    return primitive.getAsDouble();
                }
            } else {
                return primitive.getAsString();
            }
        } else if(element.isJsonObject()) {
            return convert(element.getAsJsonObject());
        } else if(element.isJsonArray()) {
            final List<Object> values = new ArrayList<>();
            for(final JsonElement arrayElement : element.getAsJsonArray()) {
                values.add(getValue(arrayElement));
            }
            return values;
        }
        return null;
    }

}
