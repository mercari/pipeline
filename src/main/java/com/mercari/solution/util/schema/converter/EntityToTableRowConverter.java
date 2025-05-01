package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(EntityToTableRowConverter.class);

    public static TableRow convertWithKey(final Entity entity) {
        return convert(entity, true);
    }

    public static TableRow convertWithoutKey(final Entity entity) {
        return convert(entity, false);
    }

    private static TableRow convert(final Entity entity, final boolean withKey) {
        if(entity == null) {
            return null;
        }
        final TableRow row = new TableRow();
        if (withKey && entity.getKey().getPathCount() > 0) {
            row.set("__key__", convertKey(entity.getKey()));
        }
        for(Map.Entry<String, Value> kv : entity.getPropertiesMap().entrySet()) {
            row.set(kv.getKey(), getValue(kv.getValue()));
        }
        return row;
    }

    private static Object getValue(final Value value) {
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BLOB_VALUE:
                return Base64.getEncoder().encode(value.getBlobValue().toByteArray());
            case INTEGER_VALUE:
                return value.getIntegerValue();
            case DOUBLE_VALUE: {
                final double doubleValue = value.getDoubleValue();
                if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    return null;
                } else {
                    return doubleValue;
                }
            }
            case TIMESTAMP_VALUE:
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString(ISODateTimeFormat.dateTime());
            case GEO_POINT_VALUE: {
                final LatLng latLng = value.getGeoPointValue();
                return String.format("%f,%f", latLng.getLatitude(), latLng.getLongitude());
            }
            case KEY_VALUE:
                return convertKey(value.getKeyValue());
            case ENTITY_VALUE:
                return convert(value.getEntityValue(), false);
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList().stream()
                        .map(EntityToTableRowConverter::getValue)
                        .collect(Collectors.toList());
            }
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    private static TableRow convertKey(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        final String path = key.getPathList().stream()
                .map(e -> String.format("\"%s\", %s", e.getKind(), e.getName() == null ?
                        Long.toString(e.getId()) : String.format("\"%s\"", e.getName())))
                .collect(Collectors.joining(", "));

        final TableRow row = new TableRow();
        row.set("namespace", key.getPartitionId().getNamespaceId());
        row.set("app", key.getPartitionId().getProjectId());
        row.set("path", path);
        row.set("kind", lastPath.getKind());
        row.set("name", lastPath.getName());
        row.set("id", lastPath.getId() == 0 ? null : lastPath.getId());
        return row;
    }

}