package com.mercari.solution.util.pipeline.processing.processor.feature;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;

import java.util.*;

public class Binning implements Processor {

    private static final String DEFAULT_SYMBOL_SUFFIX = "_bin";

    private final String name;

    private final List<String> fields;
    private final List<String> expressions;
    private final List<Double> bins;

    private final String condition;
    private final Boolean ignore;

    //
    private final Boolean isSimpleName;

    private final List<Set<String>> variablesList;

    private final Map<String, Integer> bufferSizes;

    private transient List<Expression> expressionList;
    private transient Filter.ConditionNode conditionNode;


    public Binning(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(!params.has("bins") || params.get("bins").isJsonNull()) {
            throw new IllegalArgumentException("Binning: " + name + " requires bins");
        }

        this.bins = new ArrayList<>();
        for(final JsonElement bin : params.get("bins").getAsJsonArray()) {
            if(bin.isJsonNull() || !bin.isJsonPrimitive()) {
                continue;
            }
            bins.add(bin.getAsDouble());
        }
        if(bins.isEmpty()) {
            throw new IllegalArgumentException("Binning: " + name + " requires bins over size zero");
        }

        this.variablesList = new ArrayList<>();
        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "field", "fields");
            this.fields = fieldsAndIsSingle.getKey();
            this.expressions = new ArrayList<>();
            this.variablesList.add(new HashSet<>(this.fields));
            this.isSimpleName = fieldsAndIsSingle.getValue();
        } else if(params.has("expression") || params.has("expressions")) {
            final KV<List<String>,Boolean> expressionsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "expression", "expressions");
            this.fields = new ArrayList<>();
            this.expressions = expressionsAndIsSingle.getKey();
            for(final String expression : expressions) {
                final Set<String> variables = ExpressionUtil.estimateVariables(expression);
                this.variablesList.add(variables);
            }
            this.isSimpleName = expressionsAndIsSingle.getValue();
        } else {
            throw new IllegalArgumentException("Binning: " + name + " requires expressions or fields");
        }

        final Set<String> allVariables = new HashSet<>(fields);
        for(final Set<String> variables : this.variablesList) {
            allVariables.addAll(variables);
        }
        this.bufferSizes = ExpressionUtil.extractBufferSizes(allVariables, 0, DEFAULT_VARIABLE_NAME_SEPARATOR);
    }

    public static Binning of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Binning(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.binning;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public boolean filter(Map<String, Object> input) {
        if(condition == null) {
            return true;
        }
        return Filter.filter(this.conditionNode, input);
    }

    @Override
    public SizeUnit getSizeUnit() {
        return SizeUnit.count;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {

        final List<String> enumSymbols = new ArrayList<>();
        for(int i=0; i<bins.size(); i++) {
            final String enumSymbol = createEnumSymbolName(i);
            enumSymbols.add(enumSymbol);
        }
        final Schema.FieldType enumFieldType = Schema.FieldType
                .logicalType(EnumerationType.create(enumSymbols)).withNullable(true);

        final List<Schema.Field> outputFields = new ArrayList<>();
        if(isSimpleName) {
            outputFields.add(Schema.Field.of(name, enumFieldType));
        } else if(fields.size() > 0) {
            for(final String field : fields) {
                final String outputName = createOutputName(field);
                outputFields.add(Schema.Field.of(outputName, enumFieldType));
            }
        } else {
            for(int i=0; i<expressions.size(); i++) {
                final String outputName = createOutputName(i);
                outputFields.add(Schema.Field.of(outputName, enumFieldType));
            }
        }
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        if(!this.fields.isEmpty()) {
            for(final String field : fields) {
                bufferTypes.put(field, inputTypes.get(field));
            }
        } else if(!this.expressions.isEmpty()) {
            for(final String variable : bufferSizes.keySet()) {
                bufferTypes.put(variable, inputTypes.get(variable));
            }
        } else {
            throw new IllegalStateException();
        }
        return bufferTypes;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return this.bufferSizes;
    }

    @Override
    public void setup() {
        this.expressionList = new ArrayList<>();
        if(!this.expressions.isEmpty()) {
            for(int i=0; i<this.expressions.size(); i++) {
                this.expressionList.add(ExpressionUtil.createDefaultExpression(this.expressions.get(i), this.variablesList.get(i)));
            }
        }
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {

        final Map<String, Object> outputs = new HashMap<>();
        if(!this.fields.isEmpty()) {
            for(final String field : fields) {
                final Double value = buffer.getAsDouble(field, 0);
                final String outputName = createOutputName(field);
                final Integer enumId = getEnumId(value);
                outputs.put(outputName, enumId);
            }
        } else if(!this.expressions.isEmpty()) {
            for(int i=0; i<expressions.size(); i++) {
                final String outputName = createOutputName(i);
                final Map<String, Double> values = new HashMap<>();
                for(final String input : this.bufferSizes.keySet()) {
                    final Set<String> variables = variablesList.get(i);
                    if(variables.contains(input)) {
                        final Double firstLagValue = buffer.getAsDouble(input, 0);
                        values.put(input, firstLagValue);
                    }
                    final int size = this.bufferSizes.get(input) + 1;
                    for(int index=0; index<size; index++) {
                        final String variableName = Processor.createVariableName(input, DEFAULT_VARIABLE_NAME_SEPARATOR, index);
                        final Double variableValue = buffer.getAsDouble(input, index);
                        values.put(variableName, variableValue);
                    }
                }

                try {
                    final Double output = expressionList.get(i).setVariables(values).evaluate();
                    if(Double.isNaN(output) || Double.isInfinite(output)) {
                        outputs.put(outputName, null);
                    } else {
                        final Integer enumId = getEnumId(output);
                        outputs.put(outputName, enumId);
                    }
                } catch (final NullPointerException | ArithmeticException e) {
                    //LOG.warn("Lag[" + outputName + "] failed evaluate expression: " + expression + " for variables: " + values + " cause: " + e.toString());
                    outputs.put(outputName, null);
                }
            }

        } else {
            throw new IllegalStateException();
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

    private Integer getEnumId(final Double value) {
        if(value == null) {
            return null;
        }
        for(int bin=0; bin<bins.size(); bin++) {
            if(value.compareTo(bins.get(bin)) < 0) {
                return bin;
            }
        }
        return bins.size();
    }

    private String createEnumSymbolName(final Integer index) {
        return String.format("%s%s%d", name, DEFAULT_SYMBOL_SUFFIX, index);
    }

    private String createOutputName(final String field) {
        if(isSimpleName) {
            return name;
        } else {
            return String.format("%s_%s", name, field);
        }
    }

    private String createOutputName(final Integer no) {
        if(isSimpleName) {
            return name;
        } else {
            return String.format("%s_exp%d", name, no);
        }
    }

}