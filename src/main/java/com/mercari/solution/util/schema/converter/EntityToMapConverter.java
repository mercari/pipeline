package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.mercari.solution.util.DateTimeUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityToMapConverter {

    public static Map<String, Object> convert(final Entity entity) {
        return convertWithFields(entity, null);
    }

    public static Map<String, Object> convertWithFields(final Entity entity, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(entity == null) {
            return map;
        }
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            if(fields == null || fields.contains(entry.getKey())) {
                map.put(entry.getKey(), getValue(entry.getValue()));
            }
        }
        return map;
    }

    private static Object getValue(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case STRING_VALUE -> value.getStringValue();
            case BLOB_VALUE -> value.getBlobValue().asReadOnlyByteBuffer();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case TIMESTAMP_VALUE -> DateTimeUtil.toInstant(value.getTimestampValue());
            case ENTITY_VALUE -> convert(value.getEntityValue());
            case ARRAY_VALUE -> value.getArrayValue().getValuesList().stream()
                        .map(EntityToMapConverter::getValue)
                        .collect(Collectors.toList());
            case KEY_VALUE -> {
                final Key key = value.getKeyValue();
                final Map<String, Object> keyMap = new HashMap<>();
                final List<Key.PathElement> paths = key.getPathList();
                keyMap.put("kind", paths.get(0).getKind());
                keyMap.put("name", paths.get(0).getName());
                keyMap.put("id", paths.get(0).getId());
                yield keyMap;
            }
            case GEO_POINT_VALUE -> value.getGeoPointValue().toString();
            case NULL_VALUE, VALUETYPE_NOT_SET -> null;
        };
    }

}
