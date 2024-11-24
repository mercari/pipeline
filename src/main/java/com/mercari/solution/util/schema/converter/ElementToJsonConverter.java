package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ElementToJsonConverter {

    private ElementToJsonConverter() {}

    public static JsonObject convert(final Schema schema, final MElement element) {
        if(element == null || element.getValue() == null) {
            return null;
        }
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String, Object>) element.getValue());
            case AVRO -> AvroToJsonConverter.convertObject((GenericRecord) element.getValue());
            case ROW -> RowToJsonConverter.convertObject((Row) element.getValue());
            case STRUCT -> StructToJsonConverter.convertObject((Struct) element.getValue());
            case DOCUMENT -> DocumentToJsonConverter.convertObject(((Document) element.getValue()).getFieldsMap());
            case ENTITY -> EntityToJsonConverter.convertObject(((Entity) element.getValue()));
            default -> throw new IllegalArgumentException();
        };
    }

    public static JsonObject convert(final Schema schema, final Map<String,Object> primitiveValues) {
        return convert(schema, primitiveValues, null);
    }

    public static JsonObject convert(final Schema schema, final Map<String,Object> primitiveValues, final List<String> fields) {
        final JsonObject obj = new JsonObject();
        if(primitiveValues == null || primitiveValues.isEmpty()) {
            return obj;
        }
        schema.getFields()
                .stream()
                .filter(f -> fields == null || fields.isEmpty() || fields.contains(f.getName()))
                .forEach(f -> setValue(obj, f, primitiveValues));
        return obj;
    }

    private static void setValue(final JsonObject obj, final Schema.Field field, final Map<String,Object> primitiveValues) {
        final String fieldName = field.getName();
        final Object primitiveValue = primitiveValues.get(fieldName);
        if(primitiveValue == null) {
            obj.add(fieldName, null);
        } else {
            switch (field.getFieldType().getType()) {
                case bool -> obj.addProperty(fieldName, (Boolean) primitiveValue);
                case int8, int16, int32, int64, float8, float16, float32, float64, decimal -> obj.addProperty(fieldName, (Number) primitiveValue);
                case string, json -> obj.addProperty(fieldName, primitiveValue.toString());
                case bytes -> {
                    final byte[] bytes = switch (primitiveValue) {
                        case ByteBuffer b -> b.array();
                        case byte[] b -> b;
                        case String s -> s.getBytes(StandardCharsets.UTF_8);
                        default -> null;
                    };
                    if(bytes != null) {
                        obj.addProperty(fieldName, java.util.Base64.getEncoder().encodeToString(bytes));
                    } else {
                        obj.add(fieldName, null);
                    }
                }
                case date -> obj.addProperty(fieldName, LocalDate.ofEpochDay((Integer) primitiveValue).toString());
                case time -> obj.addProperty(fieldName, LocalTime.ofNanoOfDay((Long) primitiveValue * 1000L).toString());
                case timestamp -> obj.addProperty(fieldName, DateTimeUtil.toInstant((Long) primitiveValue).toString());
                case enumeration -> obj.addProperty(fieldName, field.getFieldType().getSymbols().get((Integer) primitiveValue));
                case map -> {
                    final JsonObject mapObj = convertMap(field.getFieldType().getMapValueType(), (Map<String,Object>) primitiveValue);
                    obj.add(field.getName(), mapObj);
                }
                case element -> {
                    final JsonObject elementObj = convert(field.getFieldType().getElementSchema(), (Map<String,Object>) primitiveValue);
                    obj.add(field.getName(), elementObj);
                }
                case array -> {
                    obj.add(field.getName(), convertArray(field.getFieldType().getArrayValueType(), (List) primitiveValue));
                }
            }
        }
    }

    private static JsonArray convertArray(final Schema.FieldType arrayValueType, final Collection<?> arrayValue) {
        final JsonArray array = new JsonArray();
        if(arrayValue == null || arrayValue.isEmpty()) {
            return array;
        }
        switch (arrayValueType.getType()) {
            case bool -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Boolean) v)
                    .forEach(array::add);
            case int8 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Byte) v)
                    .forEach(array::add);
            case int16 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Short) v)
                    .forEach(array::add);
            case int32 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Integer) v)
                    .forEach(array::add);
            case int64 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Long) v)
                    .forEach(array::add);
            case float32 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> {
                        final Float floatValue = (Float) v;
                        if (Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                            return null;
                        } else {
                            return floatValue;
                        }
                    })
                    .forEach(array::add);
            case float64 -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> {
                        final Double doubleValue = (Double) v;
                        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                            return null;
                        } else {
                            return doubleValue;
                        }
                    })
                    .forEach(array::add);
            case string, json -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .forEach(array::add);
            case bytes -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(byteBuffer -> java.util.Base64.getEncoder().encodeToString(((ByteBuffer)byteBuffer).array()))
                    .forEach(array::add);
            case decimal -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (BigDecimal) v)
                    .map(BigDecimal::doubleValue)
                    .forEach(array::add);
            case date -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Integer) v)
                    .map(LocalDate::ofEpochDay)
                    .map(LocalDate::toString)
                    .forEach(array::add);
            case time -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Long) v / 1000L)
                    .map(LocalTime::ofNanoOfDay)
                    .map(LocalTime::toString)
                    .forEach(array::add);
            case timestamp -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Long) v)
                    .map(DateTimeUtil::toInstant)
                    .map(Instant::toString)
                    .forEach(array::add);
            case map -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Map) v)
                    .map(m -> convertMap(arrayValueType.getMapValueType(), m))
                    .forEach(array::add);
            case element -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Map) v)
                    .map(m -> convert(arrayValueType.getElementSchema(), m))
                    .forEach(array::add);
            case enumeration -> arrayValue.stream()
                    .filter(Objects::nonNull)
                    .map(v -> (Integer) v)
                    .map(m -> arrayValueType.getSymbols().get(m))
                    .forEach(array::add);
        }
        return array;
    }

    private static JsonObject convertMap(final Schema.FieldType mapValueType, final Map<?,?> map) {
        final JsonObject mapObject = new JsonObject();
        for(Map.Entry<?,?> entry : map.entrySet()) {
            if(entry.getValue() == null) {
                mapObject.addProperty(entry.getKey().toString(), (String) null);
                continue;
            }
            final String name = entry.getKey().toString();
            switch (mapValueType.getType()) {
                case bool -> mapObject.addProperty(name, (Boolean) entry.getValue());
                case int8, int16, int32, int64, float8, float16, float32, float64 -> mapObject.addProperty(name, (Number) entry.getValue());
                case string, json -> mapObject.addProperty(name, entry.getValue().toString());
                case bytes -> mapObject.addProperty(name, java.util.Base64.getEncoder().encodeToString(((ByteBuffer)entry.getValue()).array()));
                case date -> mapObject.addProperty(name, LocalDate.ofEpochDay((Integer)entry.getValue()).toString());
                case time -> mapObject.addProperty(name, LocalTime.ofNanoOfDay((Long)entry.getValue() / 1000L).toString());
                case timestamp -> mapObject.addProperty(name, DateTimeUtil.toInstant(((Long)entry.getValue())).toString());
                case enumeration -> mapObject.addProperty(name, mapValueType.getSymbols().get((Integer) entry.getValue()));
                case element -> mapObject.add(name, convert(mapValueType.getElementSchema(), (Map) entry.getValue()));
                case map -> mapObject.add(name, convertMap(mapValueType.getMapValueType(), (Map) entry.getValue()));
            }
        }
        return mapObject;
    }

}
