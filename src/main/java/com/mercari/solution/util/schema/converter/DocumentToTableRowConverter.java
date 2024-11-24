package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentToTableRowConverter.class);

    public static SerializableFunction<Document, TableRow> createConverter(final boolean withName, final boolean withCreateTime, final boolean withUpdateTime) {
        return document -> convert(document, withName, withCreateTime, withUpdateTime);
    }

    public static TableRow convert(final Document document) {
        return convert(document, false, false, false);
    }

    private static TableRow convert(final Document document, boolean withName, boolean withCreateTime, boolean withUpdateTime) {
        if(document == null) {
            return null;
        }
        final TableRow row = new TableRow();
        if(withName) {
            row.set("__name__", document.getName());
        }
        if(withCreateTime) {
            row.set("__createtime__", DateTimeUtil.toJodaInstant(document.getCreateTime()).toString(ISODateTimeFormat.dateTime()));
        }
        if(withUpdateTime) {
            row.set("__updatetime__", DateTimeUtil.toJodaInstant(document.getUpdateTime()).toString(ISODateTimeFormat.dateTime()));
        }

        for(Map.Entry<String, Value> kv : document.getFieldsMap().entrySet()) {
            row.set(kv.getKey(), getValue(kv.getValue()));
        }
        return row;
    }

    private static TableRow convert(final Map<String, Value> map) {
        if(map == null) {
            return null;
        }
        final TableRow row = new TableRow();
        for(Map.Entry<String, Value> kv : map.entrySet()) {
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
            case BYTES_VALUE:
                return Base64.getEncoder().encode(value.getBytesValue().toByteArray());
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
            case MAP_VALUE:
                return convert(value.getMapValue().getFieldsMap());
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList().stream()
                        .map(DocumentToTableRowConverter::getValue)
                        .collect(Collectors.toList());
            }
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

}
