package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.*;

public class OnnxToAvroConverter {

    public static Schema convertOutputSchema(final Map<String, NodeInfo> outputsInfo, final List<String> outputs) {
        final List<String> outputNames;
        if(outputs == null || outputs.size() == 0) {
            outputNames = new ArrayList<>(outputsInfo.keySet());
        } else {
            outputNames = outputs;
        }

        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("root").fields();
        for(final String outputName : outputNames) {
            final NodeInfo nodeInfo = outputsInfo.get(outputName);
            if(nodeInfo == null) {
                throw new IllegalArgumentException("Not found output name: " + outputName + " in outputs info: " + outputsInfo);
            }

            if(nodeInfo.getInfo() instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) nodeInfo.getInfo();
                final Schema elementSchema;
                switch (tensorInfo.type) {
                    case BOOL:
                        elementSchema = Schema.create(Schema.Type.BOOLEAN);
                        break;
                    case STRING:
                        elementSchema = Schema.create(Schema.Type.STRING);
                        break;
                    case UINT8:
                    case INT8:
                    case INT16:
                    case INT32:
                        elementSchema = Schema.create(Schema.Type.INT);
                        break;
                    case INT64:
                        elementSchema = Schema.create(Schema.Type.LONG);
                        break;
                    case FLOAT:
                        elementSchema = Schema.create(Schema.Type.FLOAT);
                        break;
                    case DOUBLE:
                        elementSchema = Schema.create(Schema.Type.DOUBLE);
                        break;
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported output type: " + tensorInfo.type);
                }
                final boolean isScalar = tensorInfo.isScalar() || tensorInfo.getShape()[tensorInfo.getShape().length - 1] == 1;
                if(isScalar) {
                    builder = builder.name(outputName).type(elementSchema).noDefault();
                } else {
                    builder = builder.name(outputName).type(Schema.createArray(elementSchema)).noDefault();
                }
            } else {
                throw new IllegalArgumentException("Not supported node type: " + nodeInfo.getInfo());
            }

        }
        return builder.endRecord();
    }

    public static List<Map<String,Object>> convert(final OrtSession.Result result) {

        final List<Map<String,Object>> outputs = new ArrayList<>();

        final Iterator<Map.Entry<String, OnnxValue>> iterator = result.iterator();
        while(iterator.hasNext()) {
            final Map.Entry<String, OnnxValue> entry = iterator.next();
            if(entry.getValue() instanceof OnnxTensor) {
                final OnnxTensor tensor = (OnnxTensor) entry.getValue();
                final List<?> values = getValues(tensor);
                for(int i=0; i<values.size(); i++) {
                    if(i >= outputs.size()) {
                        outputs.add(new HashMap<>());
                    }
                    outputs.get(i).put(entry.getKey(), values);
                }
            } else if(entry.getValue().getInfo() instanceof MapInfo) {
                final MapInfo mapInfo = (MapInfo) entry.getValue().getInfo();
            } else if(entry.getValue().getInfo() instanceof SequenceInfo) {
                final SequenceInfo sequenceInfo = (SequenceInfo) entry.getValue().getInfo();
            } else {
                throw new IllegalArgumentException("Not supported node type: " + entry.getValue().getInfo());
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
                case INT32: {
                    final List<Integer> ints = new ArrayList<>();
                    for (int i : tensor.getIntBuffer().array()) {
                        ints.add(i);
                    }
                    return ints;
                }
                case INT64: {
                    final List<Long> longs = new ArrayList<>();
                    for (long i : tensor.getLongBuffer().array()) {
                        longs.add(i);
                    }
                    return longs;
                }
                case FLOAT: {
                    final List<Float> floats = new ArrayList<>();
                    for (float i : tensor.getFloatBuffer().array()) {
                        floats.add(i);
                    }
                    return floats;
                }
                case DOUBLE: {
                    final List<Double> doubles = new ArrayList<>();
                    for (double i : tensor.getDoubleBuffer().array()) {
                        doubles.add(i);
                    }
                    return doubles;
                }
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

}
