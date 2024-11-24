package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import com.mercari.solution.module.Schema;

import java.util.*;

public class OnnxToElementConverter {

    public static Schema convertOutputSchema(final Map<String, NodeInfo> outputsInfo, final List<String> outputs) {
        final List<String> outputNames;
        if(outputs == null || outputs.isEmpty()) {
            outputNames = new ArrayList<>(outputsInfo.keySet());
        } else {
            outputNames = outputs;
        }

        final Schema.Builder builder = Schema.builder();
        for(final String outputName : outputNames) {
            final NodeInfo nodeInfo = outputsInfo.get(outputName);
            if(nodeInfo == null) {
                throw new IllegalArgumentException("Not found output name: " + outputName + " in outputs info: " + outputsInfo);
            }

            switch (nodeInfo.getInfo()) {
                case TensorInfo tensorInfo -> {
                    Schema.FieldType elementType = switch (tensorInfo.type) {
                        case BOOL -> Schema.FieldType.BOOLEAN.withNullable(false);
                        case STRING -> Schema.FieldType.STRING.withNullable(false);
                        case INT8, UINT8, INT16, INT32 -> Schema.FieldType.INT32.withNullable(false);
                        case INT64 -> Schema.FieldType.INT64.withNullable(false);
                        case FLOAT -> Schema.FieldType.FLOAT32.withNullable(false);
                        case DOUBLE -> Schema.FieldType.FLOAT64.withNullable(false);
                        default -> throw new IllegalArgumentException("Not supported output type: " + tensorInfo.type);
                    };
                    if(!tensorInfo.isScalar() || tensorInfo.getShape()[tensorInfo.getShape().length - 1] == 1) {
                        elementType = Schema.FieldType.array(elementType);
                    }
                    builder.withField(outputName, elementType);
                }
                default -> throw new IllegalArgumentException("Not supported node type: " + nodeInfo.getInfo());
            }
        }
        return builder.build();
    }

    public static List<Map<String,Object>> convert(final OrtSession.Result result) {
        final List<Map<String,Object>> outputs = new ArrayList<>();
        for (final Map.Entry<String, OnnxValue> entry : result) {
            if (Objects.requireNonNull(entry.getValue()) instanceof OnnxTensor tensor) {
                final List<?> values = getValues(tensor);
                for (int i = 0; i < values.size(); i++) {
                    if (i >= outputs.size()) {
                        outputs.add(new HashMap<>());
                    }
                    outputs.get(i).put(entry.getKey(), values);
                }
            } else {
                switch (entry.getValue().getInfo()) {
                    case MapInfo mapInfo -> {}
                    case SequenceInfo sequenceInfo -> {}
                    default -> throw new IllegalArgumentException();
                }
            }
        }
        return outputs;
    }

    private static List<?> getValues(final OnnxTensor tensor) {
        final long[] shape = tensor.getInfo().getShape();
        final boolean isScalar = tensor.getInfo().isScalar() || shape[shape.length - 1] == 1;

        if(isScalar) {
            throw new IllegalArgumentException("Not support");
        } else {
            switch (tensor.getInfo().type) {
                case INT8, UINT8 -> {
                    final List<Byte> bytes = new ArrayList<>();
                    for (byte i : tensor.getByteBuffer().array()) {
                        bytes.add(i);
                    }
                    return bytes;
                }
                case INT16 -> {
                    final List<Short> shorts = new ArrayList<>();
                    for (short i : tensor.getShortBuffer().array()) {
                        shorts.add(i);
                    }
                    return shorts;
                }
                case INT32 -> {
                    final List<Integer> ints = new ArrayList<>();
                    for (int i : tensor.getIntBuffer().array()) {
                        ints.add(i);
                    }
                    return ints;
                }
                case INT64 -> {
                    final List<Long> longs = new ArrayList<>();
                    for (long i : tensor.getLongBuffer().array()) {
                        longs.add(i);
                    }
                    return longs;
                }
                case FLOAT16, FLOAT -> {
                    final List<Float> floats = new ArrayList<>();
                    for (float i : tensor.getFloatBuffer().array()) {
                        floats.add(i);
                    }
                    return floats;
                }
                case DOUBLE -> {
                    final List<Double> doubles = new ArrayList<>();
                    for (double i : tensor.getDoubleBuffer().array()) {
                        doubles.add(i);
                    }
                    return doubles;
                }
                default -> throw new IllegalArgumentException("Not supported tensor info type: " + tensor.getInfo().type);
            }
        }
    }

}
