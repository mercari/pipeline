package com.mercari.solution.util.converter;

import ai.onnxruntime.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.converter.AvroToOnnxConverter;
import com.mercari.solution.util.schema.converter.RowToOnnxConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UnionValueToOnnxConverter {

    public static Map<String, OnnxTensor> convert(final OrtEnvironment environment, final Map<String, NodeInfo> inputsInfo, final List<UnionValue> values) throws OrtException {
        return convert(environment, inputsInfo, values, null);
    }

    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment,
            final Map<String, NodeInfo> inputsInfo,
            final List<UnionValue> values,
            final List<Map<String, String>> renameFieldsList) throws OrtException {

        final Map<String, OnnxTensor> tensors = new HashMap<>();
        for(final Map.Entry<String, NodeInfo> entry : inputsInfo.entrySet()) {
            if(entry.getValue().getInfo() instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) entry.getValue().getInfo();
                final List<Object> tensorValues = values.stream()
                        .map(unionValue -> {
                            final String field;
                            if(renameFieldsList == null || renameFieldsList.size() <= unionValue.getIndex()) {
                                field = entry.getKey();
                            } else {
                                field = renameFieldsList.get(unionValue.getIndex()).getOrDefault(entry.getKey(), entry.getKey());
                            }
                            return getValue(tensorInfo, field, unionValue);
                        })
                        .collect(Collectors.toList());
                final OnnxTensor tensor = ONNXRuntimeUtil.convertTensor(environment, tensorInfo, tensorValues);
                tensors.put(entry.getKey(), tensor);
            } else {
                throw new IllegalArgumentException("Not supported node type: " + entry.getValue().getInfo());
            }
        }

        return tensors;
    }

    private static Object getValue(final TensorInfo tensorInfo, final String field, final UnionValue unionValue) {
        switch (unionValue.getType()) {
            case ROW -> {
                final Row row = (Row) unionValue.getValue();
                return RowToOnnxConverter.getValue(tensorInfo, field, row, null);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.getValue();
                return AvroToOnnxConverter.getValue(tensorInfo, field, record, null);
            }
            default -> throw new IllegalArgumentException();
        }
    }

}
