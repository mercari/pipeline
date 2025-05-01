package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import org.joda.time.Instant;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToJsonConverter {

    public static String convert(final Document document) {
        return convert(document, null);
    }

    public static String convert(final Document document, final List<String> fields) {
        if(fields == null || fields.size() == 0) {
            return convertObject(document.getFieldsMap()).toString();
        } else {
            final Map<String, Value> values = document.getFieldsMap().entrySet().stream()
                    .filter(e -> fields.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return convertObject(values).toString();
        }
    }

    public static JsonObject convertObject(final Map<String, Value> values) {
        final JsonObject obj = new JsonObject();
        if(values == null) {
            return obj;
        }
        values.entrySet().forEach(kv -> setValue(obj, kv));
        return obj;
    }

    private static void setValue(final JsonObject obj, final Map.Entry<String, Value> kv) {
        final String fieldName = kv.getKey();
        final Value value = kv.getValue();
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> obj.addProperty(fieldName, value.getBooleanValue());
            case INTEGER_VALUE -> obj.addProperty(fieldName, value.getIntegerValue());
            case STRING_VALUE -> obj.addProperty(fieldName, value.getStringValue());
            case DOUBLE_VALUE -> {
                final double doubleValue = value.getDoubleValue();
                if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    obj.addProperty(fieldName, (Double) null);
                } else {
                    obj.addProperty(fieldName, doubleValue);
                }
            }
            case TIMESTAMP_VALUE ->
                    obj.addProperty(fieldName, Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString());
            case BYTES_VALUE ->
                    obj.addProperty(fieldName, Base64.getEncoder().encodeToString(value.getBytesValue().toByteArray()));
            case GEO_POINT_VALUE -> {
                final LatLng latLng = value.getGeoPointValue();
                obj.addProperty(fieldName, String.format("%f,%f", latLng.getLatitude(), latLng.getLongitude()));
            }
            case MAP_VALUE -> obj.add(fieldName, convertObject(value.getMapValue().getFieldsMap()));
            case ARRAY_VALUE -> {
                final JsonArray array = new JsonArray();
                value.getArrayValue().getValuesList()
                        .forEach(v -> {
                            switch (v.getValueTypeCase()) {
                                case BOOLEAN_VALUE:
                                    array.add(v.getBooleanValue());
                                    break;
                                case STRING_VALUE:
                                    array.add(v.getStringValue());
                                    break;
                                case INTEGER_VALUE:
                                    array.add(v.getIntegerValue());
                                    break;
                                case DOUBLE_VALUE:
                                    array.add(v.getDoubleValue());
                                    break;
                                case TIMESTAMP_VALUE:
                                    array.add(Instant.ofEpochMilli(Timestamps.toMillis(v.getTimestampValue())).toString());
                                    break;
                                case BYTES_VALUE:
                                    array.add(Base64.getEncoder().encodeToString(v.getBytesValue().toByteArray()));
                                    break;
                                case ARRAY_VALUE:
                                    break;
                                case MAP_VALUE:
                                    array.add(convertObject(v.getMapValue().getFieldsMap()));
                                    break;
                                case GEO_POINT_VALUE:
                                    final LatLng latLng = v.getGeoPointValue();
                                    array.add(String.format("%f,%f", latLng.getLatitude(), latLng.getLongitude()));
                                    break;
                                case NULL_VALUE:
                                case VALUETYPE_NOT_SET:
                                default:
                                    break;
                            }
                        });
                obj.add(fieldName, array);
            }
            case VALUETYPE_NOT_SET, NULL_VALUE -> obj.add(fieldName, null);
            default -> {
            }
        }
    }

}
