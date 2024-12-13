package com.mercari.solution.util.schema;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class ElementSchemaUtil {

    public static Schema.Field getInputField(
            final String field,
            final List<Schema.Field> inputFields) {

        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField;
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                return switch (parentFieldType.getType()) {
                    case json -> Schema.Field.of(fields[fields.length-1], Schema.FieldType.type(Schema.Type.json));
                    case element -> getInputField(fields[1], parentFieldType.getElementSchema().getFields());
                    case array -> {
                        if (!Schema.Type.element.equals(parentFieldType.getArrayValueType().getType())) {
                            throw new IllegalArgumentException();
                        }
                        yield getInputField(fields[1], parentFieldType.getArrayValueType().getElementSchema().getFields());
                    }
                    default -> throw new IllegalArgumentException();
                };
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }

    public static Schema.FieldType getInputFieldType(
            final String field,
            final List<Schema.Field> inputFields) {

        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField.getFieldType();
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                return switch (parentFieldType.getType()) {
                    case json -> Schema.FieldType.type(Schema.Type.json);
                    case element -> getInputFieldType(fields[1], parentFieldType.getElementSchema().getFields());
                    case array -> {
                        if (!Schema.Type.element.equals(parentFieldType.getArrayValueType().getType())) {
                            throw new IllegalArgumentException();
                        }
                        yield getInputFieldType(fields[1], parentFieldType.getArrayValueType().getElementSchema().getFields());
                    }
                    default -> throw new IllegalArgumentException();
                };
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }

    public static Object getValue(Map<?, ?> input, String field) {
        if(input.containsKey(field)) {
            return input.get(field);
        } else if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final Object value = input.get(fields[0]);
            return switch (value) {
                case Map<?, ?> map -> getValue(map, fields[1]);
                case String str -> {
                    try {
                        final JsonElement jsonElement = new Gson().fromJson(str, JsonElement.class);
                        if (jsonElement.isJsonObject()) {
                            final Map<String, Object> map = JsonToMapConverter.convert(jsonElement);
                            yield getValue(map, fields[1]);
                        } else if (jsonElement.isJsonArray()) {
                            List<Object> list = new ArrayList<>();
                            for (final JsonElement child : jsonElement.getAsJsonArray()) {
                                if (child.isJsonObject()) {
                                    final Map<String, Object> map = JsonToMapConverter.convert(child);
                                    final Object childValue = getValue(map, fields[1]);
                                    list.add(childValue);
                                } else {
                                    list.add(null);
                                }
                            }
                            yield list;
                        } else {
                            yield null;
                        }
                    } catch (Throwable e) {
                        throw new IllegalArgumentException("Failed to get field: " + field + ", value: " + str);
                    }
                }
                case null -> null;
                default -> throw new IllegalArgumentException("Illegal nested field: " + field + ", value: " + value);
            };
        } else {
            return null;
        }
    }

    public static Object getAsPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool -> switch (primitiveValue) {
                case Boolean b -> b;
                case String s -> Boolean.valueOf(s);
                case Number n -> n.doubleValue() > 0;
                default -> null;
            };
            case string, json -> switch (primitiveValue) {
                case String s -> s;
                case byte[] b -> new String(b, StandardCharsets.UTF_8);
                case ByteBuffer bb -> new String(bb.array(), StandardCharsets.UTF_8);
                case Object o -> o.toString();
            };
            case bytes -> switch (primitiveValue) {
                case ByteBuffer bb -> bb;
                case byte[] b -> ByteBuffer.wrap(b);
                case String s -> ByteBuffer.wrap(Base64.getDecoder().decode(s));
                default -> null;
            };
            case int16 -> switch (primitiveValue) {
                case Short s -> s;
                case Number n -> n.shortValue();
                case String s -> Short.parseShort(s);
                default -> null;
            };
            case int32 -> switch (primitiveValue) {
                case Integer i -> i;
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                default -> null;
            };
            case int64 -> switch (primitiveValue) {
                case Long l -> l;
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                default -> null;
            };
            case float32 -> switch (primitiveValue) {
                case Float f -> f;
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                default -> null;
            };
            case float64 -> switch (primitiveValue) {
                case Double d -> d;
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                default -> null;
            };
            case date -> switch (primitiveValue) {
                case String s -> Long.valueOf(DateTimeUtil.toLocalDate(s).toEpochDay()).intValue();
                case Number n -> n.intValue();
                default -> null;
            };
            case time -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toLocalTime(s).toNanoOfDay() / 1000L;
                case Number n -> n.intValue();
                default -> null;
            };
            case timestamp -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toEpochMicroSecond(s);
                case Number n -> n.longValue();
                default -> null;
            };
            case enumeration -> switch (primitiveValue) {
                case String s -> fieldType.getSymbolIndex(s);
                case Integer i -> i;
                case Number n -> n.intValue();
                default -> null;
            };
            case element, map -> switch (primitiveValue) {
                case Map<?,?> m -> m;
                case String s -> {
                    try{
                        final JsonElement jsonElement = new Gson().fromJson(s, JsonElement.class);
                        if (jsonElement.isJsonObject()) {
                            yield JsonToMapConverter.convert(jsonElement);
                        }
                        yield null;
                    } catch (Throwable e) {
                        yield null;
                    }
                }
                default -> null;
            };
            case array -> switch (primitiveValue) {
                case List<?> list -> list;
                case String s -> {
                    try{
                        final JsonElement jsonElement = new Gson().fromJson(s, JsonElement.class);
                        if (jsonElement.isJsonArray()) {
                            final List<Object> list = new ArrayList<>();
                            for(final JsonElement e : jsonElement.getAsJsonArray()) {
                                final Object o = getAsPrimitive(fieldType.getArrayValueType(), e.toString());
                                list.add(o);
                            }
                            yield list;
                        }
                        yield null;
                    } catch (Throwable e) {
                        yield null;
                    }
                }
                default -> null;
            };
            default -> null;
        };
    }

}
