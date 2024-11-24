package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroToOnnxConverter {

    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment, final OrtSession session, final List<GenericRecord> records)
            throws OrtException {

        final Map<String, OnnxTensor> tensors = new HashMap<>();
        for(final Map.Entry<String, NodeInfo> entry : session.getInputInfo().entrySet()) {
            if(entry.getValue().getInfo() instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) entry.getValue().getInfo();
                final List<Object> values = records.stream()
                        .map(r -> r.get(entry.getKey()))
                        .collect(Collectors.toList());
                tensors.put(entry.getKey(), ONNXRuntimeUtil.convertTensor(environment, tensorInfo, values));
            } else if(entry.getValue().getInfo() instanceof MapInfo) {
                final MapInfo mapInfo = (MapInfo) entry.getValue().getInfo();
            } else if(entry.getValue().getInfo() instanceof SequenceInfo) {
                final SequenceInfo sequenceInfo = (SequenceInfo) entry.getValue().getInfo();
            } else {
                throw new IllegalArgumentException("Not supported node type: " + entry.getValue().getInfo());
            }
        }

        return tensors;
    }

    public static Object getValue(final TensorInfo tensorInfo, final String field, final GenericRecord record, final Object defaultValue) {
        if(!record.hasField(field)) {
            return defaultValue;
        }
        final Object value = record.get(field);
        if(value == null) {
            return defaultValue;
        }
        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(record.getSchema().getField(field).schema());
        return switch (fieldSchema.getType()) {
            case ARRAY -> ((List<Object>) value).stream()
                        .map(v -> getValue(tensorInfo, fieldSchema.getElementType(), v))
                        .collect(Collectors.toList());

            case RECORD -> throw new IllegalArgumentException();
            default -> getValue(tensorInfo, fieldSchema, value);
        };
    }

    private static Object getValue(final TensorInfo tensorInfo, final Schema fieldSchema, final Object value) {
        switch (fieldSchema.getType()) {
            case BYTES -> {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                return switch (tensorInfo.type) {
                    case INT8 -> byteBuffer.array();
                    case UINT8, INT16 -> byteBuffer.array();
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            case ENUM, STRING -> {
                final String stringValue = value.toString();
                return switch (tensorInfo.type) {
                    case STRING -> stringValue;
                    case BOOL -> Boolean.valueOf(stringValue);
                    case INT32 -> Integer.valueOf(stringValue);
                    case INT64 -> Long.valueOf(stringValue);
                    case FLOAT -> Float.valueOf(stringValue);
                    case DOUBLE -> Double.valueOf(stringValue);
                    case INT8 -> Byte.valueOf(stringValue);
                    case INT16 -> Short.valueOf(stringValue);
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            case INT -> {
                final Integer intValue = (Integer) value;
                return switch (tensorInfo.type) {
                    case BOOL -> intValue > 0;
                    case INT32 -> intValue;
                    case INT64 -> intValue.longValue();
                    case FLOAT -> intValue.floatValue();
                    case DOUBLE -> intValue.doubleValue();
                    case INT8 -> intValue.byteValue();
                    case INT16 -> intValue.shortValue();
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            case LONG -> {
                final Long longValue = (Long) value;
                return switch (tensorInfo.type) {
                    case BOOL -> longValue > 0;
                    case INT32 -> longValue.intValue();
                    case INT64 -> longValue;
                    case FLOAT -> longValue.floatValue();
                    case DOUBLE -> longValue.doubleValue();
                    case INT8 -> longValue.byteValue();
                    case INT16 -> longValue.shortValue();
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            case FLOAT -> {
                final Float floatValue = (Float) value;
                return switch (tensorInfo.type) {
                    case BOOL -> floatValue > 0;
                    case INT32 -> floatValue.intValue();
                    case INT64 -> floatValue.longValue();
                    case FLOAT -> floatValue;
                    case DOUBLE -> floatValue.doubleValue();
                    case INT8 -> floatValue.byteValue();
                    case INT16 -> floatValue.shortValue();
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            case DOUBLE -> {
                final Double doubleValue = (Double) value;
                return switch (tensorInfo.type) {
                    case BOOL -> doubleValue > 0;
                    case INT32 -> doubleValue.intValue();
                    case INT64 -> doubleValue.longValue();
                    case FLOAT -> doubleValue.floatValue();
                    case DOUBLE -> doubleValue;
                    case INT8 -> doubleValue.byteValue();
                    case INT16 -> doubleValue.shortValue();
                    default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType() + " for tensorInfoType: " + tensorInfo.type);
                };
            }
            default -> throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
        }
    }

}
