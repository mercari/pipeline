package com.mercari.solution.util.domain.ml;

import ai.onnxruntime.*;
import com.google.gson.JsonObject;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ONNXRuntimeUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ONNXRuntimeUtil.class);

    public static long[] convertShape(
            final ValueInfo valueInfo,
            final List<Object> values) {

        return switch (valueInfo) {
            case TensorInfo tensorInfo -> {
                final long[] shape = new long[tensorInfo.getShape().length];
                for(int idx = 0; idx < shape.length; idx++) {
                    if(tensorInfo.getShape()[idx] < 0) {
                        if(idx == 0) {
                            shape[idx] = values.size();
                        } else {
                            if(values.isEmpty()) {
                                throw new IllegalArgumentException();
                            }
                            shape[idx] = switch (values.getFirst()) {
                                case Object[] o -> o.length;
                                case Collection<?> c -> c.size();
                                default -> throw new RuntimeException();
                            };
                        }
                    } else {
                        shape[idx] = tensorInfo.getShape()[idx];
                    }
                }
                yield shape;
            }
            case MapInfo mapInfo -> {
                throw new IllegalArgumentException("not supported mapInfo: " + mapInfo);
            }
            case SequenceInfo sequenceInfo -> {
                throw new IllegalArgumentException("not supported sequenceInfo: " + sequenceInfo);
            }
            default -> {
                throw new IllegalArgumentException("not supported valueInfo: " + valueInfo);
            }
        };

    }

    public static OnnxTensor convertTensor(
            final OrtEnvironment environment, final TensorInfo tensorInfo, final List<Object> values)
            throws OrtException {

        final long[] shape = convertShape(tensorInfo, values);

        final List<Object> flattenValues = flatten(values, shape.length);
        return switch (tensorInfo.type) {
            case STRING -> {
                final String[] stringValues = new String[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    stringValues[i] = (String) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, stringValues, shape);
            }
            case BOOL -> {
                final int[] intValues = new int[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    intValues[i] = (Boolean) flattenValues.get(i) ? 1 : 0;
                }
                yield OnnxTensor.createTensor(environment, IntBuffer.wrap(intValues), shape);
            }
            case INT8 -> {
                final byte[] byteValues = new byte[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    byteValues[i] = (Byte) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, ByteBuffer.wrap(byteValues), shape);
            }
            case INT16 -> {
                final short[] shortValues = new short[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    shortValues[i] = (Short) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, ShortBuffer.wrap(shortValues), shape);
            }
            case INT32 -> {
                final int[] intValues = new int[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    intValues[i] = (Integer) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, IntBuffer.wrap(intValues), shape);
            }
            case INT64 -> {
                final long[] longValues = new long[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    longValues[i] = (Long) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, LongBuffer.wrap(longValues), shape);
            }
            case FLOAT16, FLOAT -> {
                final float[] floatValues = new float[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    floatValues[i] = (Float) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, FloatBuffer.wrap(floatValues), shape);
            }
            case DOUBLE -> {
                final double[] doubleValues = new double[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    doubleValues[i] = (Double) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, DoubleBuffer.wrap(doubleValues), shape);
            }
            case UNKNOWN -> throw new IllegalArgumentException("");
            default -> throw new IllegalArgumentException();
        };
    }

    public static List<?> convertMatrix(final OnnxValue onnxValue) throws OrtException {
        switch (onnxValue.getType()) {
            case ONNX_TYPE_TENSOR -> {
                return switch (onnxValue.getValue()) {
                    case Float f -> List.of(f);
                    case float[] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { list.add(v); }
                        yield list;
                    }
                    case float[][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { list.add(vv); }}
                        yield list;
                    }
                    case float[][][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) {list.add(vvv); }}}
                        yield list;
                    }
                    case float[][][][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) { for(var vvvv : vvv) {list.add(vvvv); }}}}
                        yield list;
                    }
                    case Double f -> List.of(f);
                    case double[] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { list.add(v); }
                        yield list;
                    }
                    case double[][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { list.add(vv); }}
                        yield list;
                    }
                    case double[][][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) {list.add(vvv); }}}
                        yield list;
                    }
                    case double[][][][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) { for(var vvvv : vvv) {list.add(vvvv); }}}}
                        yield list;
                    }
                    default -> new ArrayList<>();
                };
            }
            default -> throw new RuntimeException();
        }
    }

    public static List<Object> flatten(final List<Object> list, final int rank) {
        Stream<Object> stream = list.stream();
        for(int i=1; i<rank; i++) {
            stream = stream.flatMap(v -> ((List<Object>)v).stream());
        }
        return stream.collect(Collectors.toList());
    }

    public static String getExtensionsLibraryPath() {
        try {
            final Object result = Class.forName("ai.onnxruntime.extensions.OrtxPackage")
                    .getMethod("getLibraryPath")
                    .invoke(null);
            return (String) result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to refer onnxruntime extensions package", e);
        }

    }

    public static KV<Map<String, NodeInfo>, Map<String, NodeInfo>> getNodesInfo(final String model) {
        try(final OrtEnvironment environment = OrtEnvironment.getEnvironment();
            final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {

            sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);
            try {
                final String extensionsLibraryPath = getExtensionsLibraryPath();
                sessionOptions.registerCustomOpLibrary(extensionsLibraryPath);
            } catch (Throwable e) {
                LOG.warn("Skip to load extensions library");
            }

            final byte[] bytes = StorageUtil.readBytes(model);
            try(OrtSession session = environment.createSession(bytes, sessionOptions)) {
                return KV.of(session.getInputInfo(), session.getOutputInfo());
            }
        } catch (OrtException e) {
            throw new RuntimeException("Failed to load model: " + model, e);
        }
    }

    public static String getModelDescription(final OrtSession session) {
        try {
            final JsonObject message = new JsonObject();
            message.addProperty("numInputs", session.getNumInputs());
            message.addProperty("numOutputs", session.getNumOutputs());

            final JsonObject inputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getInputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                inputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("inputInfo", inputInfo);

            final JsonObject outputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getOutputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                outputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("outputInfo", outputInfo);

            final JsonObject metadata = new JsonObject();
            metadata.addProperty("domain", session.getMetadata().getDomain());
            metadata.addProperty("description", session.getMetadata().getDescription());
            metadata.addProperty("graphName", session.getMetadata().getGraphName());
            metadata.addProperty("graphDescription", session.getMetadata().getGraphDescription());
            metadata.addProperty("producerName", session.getMetadata().getProducerName());
            metadata.addProperty("version", session.getMetadata().getVersion());
            final JsonObject customMetadata = new JsonObject();
            for (final Map.Entry<String, String> entry : session.getMetadata().getCustomMetadata().entrySet()) {
                customMetadata.addProperty(entry.getKey(), entry.getValue());
            }
            metadata.add("customMetadata", customMetadata);

            message.add("metadata", metadata);

            return message.toString();
        } catch (OrtException e) {
            throw new IllegalStateException(e);
        }

    }

}
