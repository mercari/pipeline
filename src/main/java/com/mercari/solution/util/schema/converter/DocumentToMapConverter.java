package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.Value;
import com.google.firestore.v1.Document;
import com.mercari.solution.util.DateTimeUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToMapConverter {

    public static Map<String, Object> convert(final Document document) {
        if(document == null) {
            return new HashMap<>();
        }
        return convert(document.getFieldsMap());
    }

    public static Map<String, Object> convert(final Map<String, Value> values) {
        return convertWithFields(values, null);
    }

    public static Map<String, Object> convertWithFields(final Document document, final Collection<String> fields) {
        if(document == null) {
            return new HashMap<>();
        }
        return convertWithFields(document.getFieldsMap(), fields);
    }

    public static Map<String, Object> convertWithFields(final Map<String, Value> values, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(values == null) {
            return map;
        }
        for(final Map.Entry<String, Value> entry : values.entrySet()) {
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
            case BYTES_VALUE -> value.getBytesValue().asReadOnlyByteBuffer();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case TIMESTAMP_VALUE -> DateTimeUtil.toInstant(value.getTimestampValue());
            case MAP_VALUE -> convert(value.getMapValue().getFieldsMap());
            case ARRAY_VALUE -> value.getArrayValue().getValuesList().stream()
                        .map(DocumentToMapConverter::getValue)
                        .collect(Collectors.toList());
            case REFERENCE_VALUE -> value.getReferenceValue();
            case GEO_POINT_VALUE -> value.getGeoPointValue().toString();
            case NULL_VALUE, VALUETYPE_NOT_SET -> null;
        };
    }

}
