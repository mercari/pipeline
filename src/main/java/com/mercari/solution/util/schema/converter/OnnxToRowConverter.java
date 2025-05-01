package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import org.apache.beam.sdk.schemas.Schema;

import java.util.*;

public class OnnxToRowConverter {

    public static Schema convertOutputSchema(final Map<String, NodeInfo> outputsInfo, final List<String> outputs) {
        final List<String> outputNames;
        if(outputs == null || outputs.size() == 0) {
            outputNames = new ArrayList<>(outputsInfo.keySet());
        } else {
            outputNames = outputs;
        }

        Schema.Builder builder = Schema.builder();
        for(final String outputName : outputNames) {
            final NodeInfo nodeInfo = outputsInfo.get(outputName);
            if(nodeInfo == null) {
                throw new IllegalArgumentException("Not found output name: " + outputName + " in outputs info: " + outputsInfo);
            }

            if(nodeInfo.getInfo() instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) nodeInfo.getInfo();
                final Schema.FieldType elementType;
                switch (tensorInfo.type) {
                    case BOOL:
                        elementType = Schema.FieldType.BOOLEAN.withNullable(false);
                        break;
                    case STRING:
                        elementType = Schema.FieldType.STRING.withNullable(false);
                        break;
                    case INT8:
                        elementType = Schema.FieldType.BYTE.withNullable(false);
                        break;
                    case UINT8:
                    case INT16:
                        elementType = Schema.FieldType.INT16.withNullable(false);
                        break;
                    case INT32:
                        elementType = Schema.FieldType.INT32.withNullable(false);
                        break;
                    case INT64:
                        elementType = Schema.FieldType.INT64.withNullable(false);
                        break;
                    case FLOAT:
                        elementType = Schema.FieldType.FLOAT.withNullable(false);
                        break;
                    case DOUBLE:
                        elementType = Schema.FieldType.DOUBLE.withNullable(false);
                        break;
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported output type: " + tensorInfo.type);
                }

                final boolean isScalar = tensorInfo.isScalar() || tensorInfo.getShape()[tensorInfo.getShape().length - 1] == 1;
                if(isScalar) {
                    builder = builder.addField(outputName, elementType);
                } else {
                    builder = builder.addField(outputName, Schema.FieldType.array(elementType));
                }

            } else {
                throw new IllegalArgumentException("Not supported node type: " + nodeInfo.getInfo());
            }

        }
        return builder.build();
    }

    public static List<Map<String,Object>> convert(final OrtSession.Result result) {

        final List<Map<String,Object>> outputs = new ArrayList<>();

        return outputs;
    }

}
