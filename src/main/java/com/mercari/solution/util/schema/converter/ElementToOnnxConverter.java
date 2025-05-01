package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElementToOnnxConverter {

    /*
    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment,
            final Map<String, NodeInfo> inputsInfo,
            final List<MElement> values) throws OrtException {

        return convert(environment, inputsInfo, values, null);
    }

     */

    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment,
            final Map<String, NodeInfo> inputsInfo,
            final List<MElement> elements) throws OrtException {

        return convert(environment, inputsInfo, elements, null);
    }

    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment,
            final Map<String, NodeInfo> inputsInfo,
            final List<MElement> elements,
            final List<Map<String, String>> renameFieldsList) throws OrtException {

        final Map<String, OnnxTensor> tensors = new HashMap<>();
        for(final Map.Entry<String, NodeInfo> entry : inputsInfo.entrySet()) {
            switch (entry.getValue().getInfo()) {
                case TensorInfo tensorInfo -> {
                    final List<Object> tensorValues = elements.stream()
                            .map(element -> {
                                final String field;
                                if(renameFieldsList == null || renameFieldsList.size() <= element.getIndex()) {
                                    field = entry.getKey();
                                } else {
                                    field = renameFieldsList.get(element.getIndex()).getOrDefault(entry.getKey(), entry.getKey());
                                }
                                return getValue(tensorInfo, field, element);
                            })
                            .collect(Collectors.toList());
                    final OnnxTensor tensor = ONNXRuntimeUtil.convertTensor(environment, tensorInfo, tensorValues);
                    tensors.put(entry.getKey(), tensor);
                }
                case MapInfo mapInfo -> {
                    //TODO
                }
                case SequenceInfo sequenceInfo -> {
                    //TODO
                }
                default -> throw new IllegalArgumentException("Not supported onnx node info type: " + entry.getValue().getInfo());
            }
        }

        return tensors;
    }

    private static Object getValue(final TensorInfo tensorInfo, final String field, final MElement element) {
        switch (element.getType()) {
            case ELEMENT -> {
                final Map<String, Object> values = (Map<String, Object>) element.getValue();
                return getValue(tensorInfo, field, values, null);
            }
            case ROW -> {
                final Row row = (Row) element.getValue();
                return RowToOnnxConverter.getValue(tensorInfo, field, row, null);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) element.getValue();
                return AvroToOnnxConverter.getValue(tensorInfo, field, record, null);
            }
            default -> throw new IllegalArgumentException("Not supported onnx convert type: " + element.getType());
        }
    }

    private static Object getValue(
            final TensorInfo tensorInfo,
            final String field,
            final Map<String, Object> values,
            final Object defaultValue) {

        if(!values.containsKey(field)) {
            return defaultValue;
        }
        final Object value = values.getOrDefault(field, defaultValue);
        return switch (value) {
            case List<?> list -> list.stream()
                    .map(v -> getValue(tensorInfo, v))
                    .collect(Collectors.toList());
            default -> getValue(tensorInfo, value);
        };
    }

    private static Object getValue(final TensorInfo tensorInfo, final Object value) {
        return switch (tensorInfo.type) {
            case STRING -> switch (value) {
                case String s -> s;
                case ByteBuffer bb -> new String(bb.array(), StandardCharsets.UTF_8);
                default -> value.toString();
            };
            case INT8 -> switch (value) {
                case Boolean b -> b ? 1 : 0;
                case Number n -> n.byteValue();
                case String s -> Byte.parseByte(s);
                default -> throw new IllegalArgumentException();
            };
            case INT16 -> switch (value) {
                case Boolean b -> b ? 1 : 0;
                case Number n -> n.shortValue();
                case String s -> Short.parseShort(s);
                default -> throw new IllegalArgumentException();
            };
            case INT32 -> switch (value) {
                case Boolean b -> b ? 1 : 0;
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                default -> throw new IllegalArgumentException();
            };
            case INT64 -> switch (value) {
                case Boolean b -> b ? 1L : 0L;
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                default -> throw new IllegalArgumentException();
            };
            case FLOAT16, FLOAT -> switch (value) {
                case Boolean b -> b ? 1f : 0f;
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (value) {
                case Boolean b -> b ? 1d : 0d;
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException("Not supported tensorInfo type: " + tensorInfo.type);
        };
    }

}
