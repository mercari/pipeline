package com.mercari.solution.util.schema.converter;

import com.google.gson.*;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class JsonToElementConverter {

    public static Map<String,Object> convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonElement jsonElement = new Gson().fromJson(text, JsonElement.class);
        return convert(schema, jsonElement);
    }

    public static Map<String,Object> convert(final Schema schema, final JsonElement jsonElement) {
        if(jsonElement.isJsonObject()) {
            return convert(schema, jsonElement.getAsJsonObject());
        } else if(jsonElement.isJsonArray()) {
            final Map<String, Object> map = new HashMap<>();
            final JsonArray array = jsonElement.getAsJsonArray();
            for(int i=0; i<schema.getFields().size(); i++) {
                final Schema.Field field = schema.getField(i);
                if(i < array.size()) {
                    final JsonElement arrayElement = array.get(i);
                    map.put(field.getName(), convertValue(field.getFieldType(), arrayElement));
                } else {
                    map.put(field.getName(), null);
                }
            }
            return map;
        } else {
            return null;
        }
    }

    public static Map<String,Object> convert(final Schema schema, final JsonObject jsonObject) {
        final Map<String, Object> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final String fieldName;
            if(field.getOptions().containsKey(Schema.OPTION_ORIGINAL_FIELD_NAME)) {
                fieldName = field.getOptions().get(Schema.OPTION_ORIGINAL_FIELD_NAME);
            } else {
                fieldName = field.getName();
            }
            values.put(field.getName(), convertValue(field.getFieldType(), jsonObject.get(fieldName)));
        }
        return values;
    }

    private static Object convertValue(final Schema.FieldType fieldType, final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            if(Schema.Type.array.equals(fieldType.getType())) {
                return new ArrayList<>();
            }
            return null;
        }
        return switch (fieldType.getType()) {
            case string, json -> jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
            case bytes -> jsonElement.isJsonPrimitive() ? Base64.getUrlDecoder().decode(jsonElement.getAsString()) : null;
            case int16 -> jsonElement.isJsonPrimitive() ? jsonElement.getAsShort() : null;
            case int32 -> jsonElement.isJsonPrimitive() ? Integer.valueOf(jsonElement.getAsString()) : null;
            case int64 -> jsonElement.isJsonPrimitive() ? jsonElement.getAsLong() : null;
            case float32 -> jsonElement.isJsonPrimitive() ? Float.valueOf(jsonElement.getAsString()) : null;
            case float64 -> jsonElement.isJsonPrimitive() ? jsonElement.getAsDouble() : null;
            case bool -> jsonElement.isJsonPrimitive() ? jsonElement.getAsBoolean() : null;
            case timestamp -> {
                if(jsonElement.isJsonPrimitive()) {
                    final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                    if(primitive.isString()) {
                        yield DateTimeUtil.toEpochMicroSecond(jsonElement.getAsString());
                    } else if(primitive.isNumber()) {
                        if(primitive.getAsString().contains(".")) {
                            yield DateTimeUtil.toEpochMicroSecond(primitive.getAsDouble());
                        } else {
                            yield primitive.getAsLong();
                        }
                    } else {
                        final String message = "json fieldType: " + fieldType.getType() + ", value: " + jsonElement + " could not be convert to timestamp";
                        throw new IllegalStateException(message);
                    }
                } else {
                    final String message = "json fieldType: " + fieldType.getType() + ", value: " + jsonElement + " could not be convert to timestamp";
                    throw new IllegalStateException(message);
                }
            }
            case decimal -> {
                if(!jsonElement.isJsonPrimitive()) {
                    final String message = "json fieldType: " + fieldType.getType() + ", value: " + jsonElement + " could not be convert to decimal";
                    throw new IllegalStateException(message);
                }
                final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                if(jsonPrimitive.isString()) {
                    yield new BigDecimal(jsonPrimitive.getAsString());
                } else if(jsonPrimitive.isNumber()) {
                    yield new BigDecimal(jsonPrimitive.getAsString());
                } else {
                    throw new IllegalStateException("Can not convert Decimal type from jsonElement: " + jsonElement.toString());
                }
            }
            case date -> {
                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(primitive.isString()) {
                    yield DateTimeUtil.toEpochDay(primitive.getAsString());
                } else if(primitive.isNumber()) {
                    yield primitive.getAsInt();
                } else {
                    throw new IllegalStateException("json fieldType: " + fieldType.getType() + ", value: " + jsonElement + " could not be convert to date");
                }
            }
            case time -> {
                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(primitive.isString()) {
                    yield DateTimeUtil.toMicroOfDay(primitive.getAsString());
                } else if(primitive.isNumber()) {
                    yield primitive.getAsLong();
                } else {
                    throw new IllegalStateException("json fieldType: " + fieldType.getType() + ", value: " + jsonElement + " could not be convert to time");
                }
            }
            case enumeration -> jsonElement.getAsJsonPrimitive().getAsString();
            case element -> {
                if (!jsonElement.isJsonObject() && !jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is %s",
                            fieldType.getType(), jsonElement));
                }
                yield convert(fieldType.getElementSchema(), jsonElement);
            }
            case map -> {
                if(!jsonElement.isJsonObject()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is map, but jsonElement is %s",
                            fieldType.getType(), jsonElement.toString()));
                }
                yield jsonElement.getAsJsonObject().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> convertValue(fieldType.getMapValueType(), e.getValue())));
            }

            case array -> {
                if (!jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is %s",
                            fieldType.getType(), jsonElement.toString()));
                }
                final List<Object> childValues = new ArrayList<>();
                for (final JsonElement childJsonElement : jsonElement.getAsJsonArray()) {
                    if (!Schema.Type.element.equals(fieldType.getArrayValueType().getType())
                            && childJsonElement.isJsonArray()) {
                        throw new IllegalArgumentException("Not supported Array in Array field");
                    }
                    final Object arrayValue = convertValue(fieldType.getArrayValueType(), childJsonElement);
                    if (arrayValue != null) {
                        childValues.add(arrayValue);
                    }
                }
                yield childValues;
            }
            default -> null;
        };
    }

}
