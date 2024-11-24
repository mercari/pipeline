package com.mercari.solution.util.pipeline.processing.processor.window;

import com.google.gson.JsonObject;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;

import java.util.*;

public class ArgMax extends WindowProcessor {

    private final String comparingField;
    private final String comparingExpression;
    private final Set<String> comparingVariables;


    private final Map<String, Integer> bufferSizes;

    private final Boolean opposite;

    private transient Expression comparingExp;

    public ArgMax(final String name, final String condition, final Boolean ignore, final JsonObject params, final boolean opposite) {

        super(name, condition, ignore, params);

        this.opposite = opposite;

        if(ranges.isEmpty()) {
            throw new IllegalArgumentException("ArgMax step: " + name + " requires ranges over size zero");
        }

        if(params.has("comparingField")) {
            this.comparingField = params.get("comparingField").getAsString();
            this.comparingExpression = null;
            this.comparingVariables = new HashSet<>();
        } else if(params.has("comparingExpression")) {
            this.comparingField = null;
            this.comparingExpression = params.get("comparingExpression").getAsString();
            this.comparingVariables = ExpressionUtil.estimateVariables(comparingExpression);
        } else {
            throw new IllegalArgumentException("ArgMax step: " + name + " requires comparingField or comparingExpression");
        }

        this.bufferSizes = super.getBufferSizes();
    }

    public static ArgMax of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return of(name, condition, ignore, params, false);
    }

    public static ArgMax of(final String name, final String condition, final Boolean ignore, final JsonObject params, final boolean opposite) {
        return new ArgMax(name, condition, ignore, params, opposite);
    }

    @Override
    public Op getOp() {
        return opposite ? Op.argmin : Op.argmax;
    }

    @Override
    public Integer getOffsetMax() {
        return rangeMax;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final List<Schema.Field> outputFields = new ArrayList<>();
        for(final String field : fields) {
            for(final KV<Integer, Integer> range : ranges) {
                final String outputFieldName = createOutputName(field, range);
                outputFields.add(Schema.Field.of(outputFieldName, inputTypes.get(field)).withNullable(true));
            }
        }
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        for(final String field : fields) {
            final Schema.FieldType fieldType = inputTypes.get(field);
            if(fieldType == null) {
                throw new IllegalArgumentException("ArgMax step: " + name + " input field: " + field + " not found");
            }
            bufferTypes.put(field, fieldType);
        }

        if(this.comparingField != null) {
            final Schema.FieldType fieldType = inputTypes.get(comparingField);
            if(fieldType == null) {
                throw new IllegalArgumentException("ArgMax step: " + name + " input field: " + comparingField + " not found");
            }
            bufferTypes.put(comparingField, fieldType);
        } else if(this.comparingExpression != null) {
            bufferTypes.put(name + ".comparingExpression", Schema.FieldType.DOUBLE.withNullable(true));
            for(final String variable : bufferSizes.keySet()) {
                final Schema.FieldType fieldType = inputTypes.get(variable);
                if(fieldType == null) {
                    throw new IllegalArgumentException("ArgMax step: " + name + " input variable: " + variable + " not found");
                }
                bufferTypes.put(variable, inputTypes.get(variable));
            }
        } else {
            throw new IllegalStateException("ArgMax step: " + name + " both: comparingField and  comparingExpression are null");
        }

        return bufferTypes;
    }

    @Override
    public void setup() {
        super.setup();
        if(comparingExpression != null) {
            this.comparingExp = ExpressionUtil.createDefaultExpression(comparingExpression, comparingVariables);
        }
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {

        final Map<String, Object> outputs = new HashMap<>();

        for(KV<Integer,Integer> range : ranges) {
            if(comparingField != null) {
                Comparable max = null;
                int maxIndex = -1;
                final List<Object> values = buffer.getValues(comparingField, range.getKey(), range.getValue() - range.getKey(), getSizeUnit());
                for(int i=0; i<values.size(); i++) {
                    final Object value = values.get(i);
                    if(max == null) {
                        max = (Comparable) value;
                        maxIndex = i;
                    } else if(value != null) {
                        if(opposite) {
                            if(((Comparable) value).compareTo(max) < 0) {
                                max = (Comparable) value;
                                maxIndex = i;
                            }
                        } else {
                            if(((Comparable) value).compareTo(max) > 0) {
                                max = (Comparable) value;
                                maxIndex = i;
                            }
                        }
                    }
                }

                for(final String field : fields) {
                    final String outputName = createOutputName(field, range);
                    if(maxIndex >= 0) {
                        final Object value = buffer.get(field, maxIndex);
                        outputs.put(outputName, value);
                    } else {
                        outputs.put(outputName, null);
                    }
                }
            } else if(comparingExpression != null) {
                /*
                Double max = opposite ? Double.MAX_VALUE : Double.MIN_VALUE;

                final List<Object> values = buffer.getValues(comparingField, range.getKey(), range.getValue() - range.getKey(), getSizeUnit());


                for(int r=range.getKey(); r<range.getValue(); r++) {
                    final Map<String, Double> values = new HashMap<>();
                    for(final String input : this.bufferSizes.keySet()) {
                        if(comparingVariables.contains(input)) {
                            final Double firstLagValue = buffer.getAsDouble(input, 0);
                            values.put(input, firstLagValue);
                        }
                        final int size = this.bufferSizes.get(input) - rangeMax + 1;
                        for(int variableIndex=0; variableIndex<size; variableIndex++) {
                            final int index = r + variableIndex;
                            final String variableName = Processor.createVariableName(input, DEFAULT_VARIABLE_NAME_SEPARATOR, variableIndex);
                            final Double variableValue = buffer.getAsDouble(input, index);
                            values.put(variableName, variableValue);
                        }
                    }

                    for(final String field : fields) {
                        int maxIndex = -1;
                        try {
                            final double output = comparingExp.setVariables(values).evaluate();
                            if(!Double.isNaN(output) && !Double.isInfinite(output)) {
                                if(opposite) {
                                    if(output < max) {
                                        max = output;
                                        maxIndex =
                                    }
                                } else {
                                    if(output > max) {
                                        max = output;
                                    }
                                }
                            }
                        } catch (final NullPointerException | ArithmeticException ee) {
                            LOG.warn("Sum[" + name + "] failed evaluate expression: " + expressions.get(e) + " for variables: " + values + " cause: " + ee);
                        }
                        final String outputName = createOutputName(field, range);
                        if(maxIndex >= 0) {
                            final Object value = buffer.get(field, maxIndex);
                            outputs.put(outputName, value);
                        } else {
                            outputs.put(outputName, null);
                        }
                    }


                }
                final String outputName = createOutputName(e, range);
                outputs.put(outputName, max);
                 */
            }
        }

        return outputs;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        throw new NotImplementedException("");
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        throw new NotImplementedException("");
    }

}
