package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import org.joda.time.Instant;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityToJsonConverter {

    public static String convert(final Entity entity, final List<String> fields) {
        return convert(entity);
    }

    public static String convert(final Entity entity) {
        return convertObject(entity).toString();
    }

    public static JsonObject convertObject(final Entity entity) {
        final JsonObject obj = new JsonObject();
        if(entity == null) {
            return obj;
        }
        entity.getPropertiesMap().entrySet().forEach(kv -> setValue(obj, kv));
        return obj;
    }

    private static void setValue(final JsonObject obj, final Map.Entry<String, Value> kv) {
        final String fieldName = kv.getKey();
        final Value value = kv.getValue();
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                obj.addProperty(fieldName, value.getBooleanValue());
                break;
            case INTEGER_VALUE:
                obj.addProperty(fieldName, value.getIntegerValue());
                break;
            case STRING_VALUE:
                obj.addProperty(fieldName, value.getStringValue());
                break;
            case DOUBLE_VALUE: {
                final double doubleValue = value.getDoubleValue();
                if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    obj.addProperty(fieldName, (Double) null);
                } else {
                    obj.addProperty(fieldName, doubleValue);
                }
                break;
            }
            case TIMESTAMP_VALUE:
                obj.addProperty(fieldName, Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString());
                break;
            case BLOB_VALUE:
                obj.addProperty(fieldName, Base64.getEncoder().encodeToString(value.getBlobValue().toByteArray()));
                break;
            case GEO_POINT_VALUE: {
                final LatLng latLng = value.getGeoPointValue();
                obj.addProperty(fieldName, String.format("%f,%f", latLng.getLatitude(), latLng.getLongitude()));
                break;
            }
            case KEY_VALUE:
                obj.add(fieldName, convertKey(value.getKeyValue()));
                break;
            case ENTITY_VALUE:
                obj.add(fieldName, convertObject(value.getEntityValue()));
                break;
            case ARRAY_VALUE: {
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
                                case BLOB_VALUE:
                                    array.add(Base64.getEncoder().encodeToString(v.getBlobValue().toByteArray()));
                                    break;
                                case ARRAY_VALUE:
                                    break;
                                case ENTITY_VALUE:
                                    array.add(convertObject(v.getEntityValue()));
                                    break;
                                case GEO_POINT_VALUE:
                                    final LatLng latLng = v.getGeoPointValue();
                                    array.add(String.format("%f,%f", latLng.getLatitude(), latLng.getLongitude()));
                                    break;
                                case NULL_VALUE:
                                case VALUETYPE_NOT_SET:
                                    break;
                                case KEY_VALUE:
                                    array.add(convertKey(v.getKeyValue()));
                                    break;
                                default:
                                    break;
                            }
                        });
                obj.add(fieldName, array);
                break;
            }
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                obj.add(fieldName, null);
                break;
            default:
                break;

        }
    }

    private static JsonObject convertKey(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key.toString() + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        final String path = key.getPathList().stream()
                .map(e -> String.format("\"%s\", %s", e.getKind(), e.getName() == null ?
                        Long.toString(e.getId()) : String.format("\"%s\"", e.getName())))
                .collect(Collectors.joining(", "));

        final JsonObject obj = new JsonObject();
        obj.addProperty("namespace", key.getPartitionId().getNamespaceId());
        obj.addProperty("app", key.getPartitionId().getProjectId());
        obj.addProperty("path", path);
        obj.addProperty("kind", lastPath.getKind());
        obj.addProperty("name", lastPath.getName());
        obj.addProperty("id", lastPath.getId() == 0 ? null : lastPath.getId());
        return obj;
    }


}
