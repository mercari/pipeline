package com.mercari.solution.util.schema.converter;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;

import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToAvroConverter {

    public static GenericRecord convert(final RunQueryResponse response, final Schema schema) {
        return convert(schema, response.getDocument());
    }

    public static GenericRecord convert(final Schema schema, final Document document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            if("__name__".equals(field.name()) && !document.containsFields(field.name())) {
                builder.set(field, document.getName());
            } else if("__createtime__".equals(field.name()) && !document.containsFields(field.name())) {
                builder.set(field, DateTimeUtil.toEpochMicroSecond(document.getCreateTime()));
            } else if("__updatetime__".equals(field.name()) && !document.containsFields(field.name())) {
                builder.set(field, DateTimeUtil.toEpochMicroSecond(document.getUpdateTime()));
            } else {
                builder.set(field, getValue(field.schema(), document.getFieldsMap().get(field.name())));
            }
        }
        return builder.build();
    }

    private static GenericRecord convert(final Schema schema, final MapValue document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            builder.set(field, getValue(field.schema(), document.getFieldsMap().get(field.name())));
        }
        return builder.build();
    }

    public static GenericRecord convert(final AvroWriteRequest<Document> request) {
        return convert(request.getSchema(), request.getElement());
    }

    private static Object getValue(final Schema schema, final Value value) {
        if (value == null || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
            return null;
        }

        switch (schema.getType()) {
            case ENUM:
            case STRING: {
                switch (value.getValueTypeCase()) {
                    case STRING_VALUE:
                        return value.getStringValue();
                    case TIMESTAMP_VALUE:
                        return value.getTimestampValue().toString();
                    case INTEGER_VALUE:
                        return Long.toString(value.getIntegerValue());
                    case DOUBLE_VALUE:
                        return Double.toString(value.getDoubleValue());
                    case BOOLEAN_VALUE:
                        return Boolean.toString(value.getBooleanValue());
                    case REFERENCE_VALUE:
                        return value.getReferenceValue();
                    case GEO_POINT_VALUE: {
                        final LatLng latlng = value.getGeoPointValue();
                        return String.format("%f,%f",latlng.getLatitude(), latlng.getLongitude());
                    }
                    case MAP_VALUE:
                        return value.getMapValue().toString();
                    case ARRAY_VALUE:
                        return value.getArrayValue().toString();
                    default:
                        throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                }
            }
            case FIXED:
            case BYTES:
                return value.getBytesValue().asReadOnlyByteBuffer();
            case BOOLEAN: {
                switch (value.getValueTypeCase()) {
                    case STRING_VALUE:
                        return Boolean.valueOf(value.getStringValue());
                    case INTEGER_VALUE:
                        return value.getIntegerValue() > 0;
                    case DOUBLE_VALUE:
                        return value.getDoubleValue() > 0D;
                    case BOOLEAN_VALUE:
                        return value.getBooleanValue();
                    default:
                        throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                }
            }
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final String date = value.getStringValue();
                            return Long.valueOf(DateTimeUtil.toLocalDate(date).toEpochDay()).intValue();
                        }
                        case INTEGER_VALUE:
                            return Long.valueOf(value.getIntegerValue()).intValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).intValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final String time = value.getStringValue();
                            return DateTimeUtil.toLocalTime(time).toNanoOfDay() / 1000_000L;
                        }
                        case INTEGER_VALUE:
                            return Long.valueOf(value.getIntegerValue()).intValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).intValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            return Integer.valueOf(value.getStringValue());
                        case INTEGER_VALUE:
                            return Long.valueOf(value.getIntegerValue()).intValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).intValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case TIMESTAMP_VALUE: {
                            final Timestamp timestamp = value.getTimestampValue();
                            return DateTimeUtil.toEpochMicroSecond(timestamp) / 1000L;
                        }
                        case STRING_VALUE:
                            return DateTimeUtil.toEpochMicroSecond(value.getStringValue()) / 1000L;
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case TIMESTAMP_VALUE: {
                            final Timestamp timestamp = value.getTimestampValue();
                            return DateTimeUtil.toEpochMicroSecond(timestamp);
                        }
                        case STRING_VALUE:
                            return DateTimeUtil.toEpochMicroSecond(value.getStringValue());
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final String time = value.getStringValue();
                            return DateTimeUtil.toLocalTime(time).toNanoOfDay() / 1000L;
                        }
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else if (AvroSchemaUtil.isLogicalTypeLocalTimestampMillis(schema)) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final String timestamp = value.getStringValue();
                            return DateTimeUtil.toEpochMicroSecond(timestamp) / 1000L;
                        }
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        case TIMESTAMP_VALUE:
                            return DateTimeUtil.toEpochMicroSecond(value.getTimestampValue()) / 1000L;
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else if (AvroSchemaUtil.isLogicalTypeLocalTimestampMicros(schema)) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final String timestamp = value.getStringValue();
                            return DateTimeUtil.toEpochMicroSecond(timestamp);
                        }
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        case TIMESTAMP_VALUE:
                            return DateTimeUtil.toEpochMicroSecond(value.getTimestampValue());
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                } else {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            return Long.valueOf(value.getStringValue());
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        case DOUBLE_VALUE:
                            return Double.valueOf(value.getDoubleValue()).longValue();
                        default:
                            throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                    }
                }
            case FLOAT: {
                switch (value.getValueTypeCase()) {
                    case STRING_VALUE:
                        return Float.valueOf(value.getStringValue());
                    case INTEGER_VALUE:
                        return Long.valueOf(value.getIntegerValue()).floatValue();
                    case DOUBLE_VALUE:
                        return Double.valueOf(value.getDoubleValue()).floatValue();
                    default:
                        throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                }
            }
            case DOUBLE: {
                switch (value.getValueTypeCase()) {
                    case STRING_VALUE:
                        return Double.valueOf(value.getStringValue());
                    case INTEGER_VALUE:
                        return Long.valueOf(value.getIntegerValue()).doubleValue();
                    case DOUBLE_VALUE:
                        return value.getDoubleValue();
                    default:
                        throw new IllegalArgumentException("Schema type is " + schema.getType() + ", but value type is " + value.getValueTypeCase().name());
                }
            }
            case MAP:
                return value.getMapValue()
                        .getFieldsMap()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> getValue(schema.getValueType(), e.getValue())));
            case RECORD:
                return convert(schema, value.getMapValue());
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> getValue(schema.getElementType(), v))
                        .collect(Collectors.toList());
            case UNION: {
                final Schema unnestedSchema = AvroSchemaUtil.unnestUnion(schema);
                return getValue(unnestedSchema, value);
            }
            case NULL:
            default:
                return null;
        }
    }
}
