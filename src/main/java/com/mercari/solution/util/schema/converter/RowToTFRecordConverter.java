package com.mercari.solution.util.schema.converter;

/*
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.tensorflow.example.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

 */

public class RowToTFRecordConverter {

    /*
    public static byte[] convert(final Schema schema, final Row row) {
        final Features features = Features.newBuilder().putAllFeature(getFeatureMap(null, schema, row)).build();
        final Example example = Example.newBuilder().setFeatures(features).build();
        return example.toByteArray();
    }

    private static Feature getFeature(final Schema.FieldType fieldType, Object value) {
        final Feature.Builder builder = Feature.newBuilder();
        switch (fieldType.getTypeName()) {
            case STRING:
                return builder.setBytesList(BytesList.newBuilder().addValue(ByteString.copyFrom(value.toString().getBytes()))).build();
            case BYTES:
                return builder.setBytesList(BytesList.newBuilder().addValue(ByteString.copyFrom((ByteBuffer)value))).build();
            case FLOAT:
                return builder.setFloatList(FloatList.newBuilder().addValue((Float)value)).build();
            case DOUBLE:
                return builder.setFloatList(FloatList.newBuilder().addValue((float)(double)value)).build();
            case INT16:
                return builder.setInt64List(Int64List.newBuilder().addValue((Short)value)).build();
            case INT32:
                return builder.setInt64List(Int64List.newBuilder().addValue((Integer)value)).build();
            case INT64:
                return builder.setInt64List(Int64List.newBuilder().addValue((Long)value)).build();
            case BOOLEAN:
                return builder.setInt64List(Int64List.newBuilder().addValue((Boolean)value ? 1 : 0)).build();
            default:
                throw new IllegalArgumentException(String.format("Not supported schema type %s", fieldType.getTypeName()));
        }
    }

    private static Feature getFeatureArray(final Schema.FieldType fieldType, Object value) {
        final Feature.Builder builder = Feature.newBuilder();
        switch (fieldType.getTypeName()) {
            case STRING:
                return builder.setBytesList(BytesList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> s == null ? "" : s)
                                .map(s -> ByteString.copyFrom(s.toString().getBytes()))
                                .collect(Collectors.toList()))).build();
            case BYTES:
                return builder.setBytesList(BytesList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> ByteString.copyFrom((ByteBuffer)s))
                                .collect(Collectors.toList()))).build();
            case FLOAT:
                return builder.setFloatList(FloatList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Float)s)
                                .collect(Collectors.toList()))).build();
            case DOUBLE:
                return builder.setFloatList(FloatList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Double)s)
                                .map(Double::floatValue)
                                .collect(Collectors.toList()))).build();
            case INT16:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Short)s)
                                .map(Short::longValue)
                                .collect(Collectors.toList()))).build();
            case INT32:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Integer)s)
                                .map(Integer::longValue)
                                .collect(Collectors.toList()))).build();
            case INT64:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Long)s)
                                .collect(Collectors.toList()))).build();
            case BOOLEAN:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Boolean)s)
                                .map(s -> s ? 1L : 0L)
                                .collect(Collectors.toList()))).build();
            default:
                throw new IllegalArgumentException(String.format("Not supported schema type %s", fieldType.getTypeName()));
        }
    }

    private static Map<String,Feature> getFeatureMap(final String fieldName, final Schema schema, final Row row) {
        final Map<String,Feature> featureMap = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final String childFieldName = (fieldName == null ? "" : fieldName + "_") + field.getName();
            switch (field.getType().getTypeName()) {
                case ROW:
                    featureMap.putAll(getFeatureMap(childFieldName, field.getType().getRowSchema(), row.getRow(field.getName())));
                    break;
                case ARRAY:
                    featureMap.put(childFieldName, getFeatureArray(field.getType().getCollectionElementType(), row));
                    break;
                case MAP:
                    break;
                default:
                    featureMap.put(childFieldName, getFeature(field.getType(), row.getValue(field.getName())));
                    break;
            }
        }
        return featureMap;
    }

     */

}
