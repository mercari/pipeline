package com.mercari.solution.util.pipeline.processing.processor.learner;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.domain.ml.Model;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public abstract class Learner implements Processor {

    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    protected static final String DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX = "_horizon";
    protected static final String DEFAULT_OUTPUT_NAME_EXPRESSION_TARGET_SUFFIX = "_target";


    protected final String name;
    private final String condition;
    private final boolean ignore;

    protected final List<String> targetFields;
    protected final List<String> targetExpressions;
    protected final List<Integer> horizons;
    protected final Integer trainSize;
    protected final Processor.SizeUnit trainSizeUnit;
    protected final List<String> featureFields;
    protected final Integer trainIntervalSize;
    protected final TrainType trainType;
    protected final Boolean skipNullRows;
    protected final Boolean standardize;

    protected final List<String> expressionNames;
    protected final Boolean isSingleTargetField;
    protected final Boolean isSingleHorizon;

    //
    protected final Integer horizonMax;
    protected final List<Set<String>> targetVariablesList;
    protected final Map<String, Integer> bufferSizes;

    protected final Set<String> targetAllVariables;

    private transient List<Expression> targetExpList;
    private transient Filter.ConditionNode conditionNode;

    Learner(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        // validation
        if(!params.has("featureFields") || !params.get("featureFields").isJsonArray()) {
            throw new IllegalArgumentException("Learner: " + name + " requires featureFields in array format");
        }
        if(!params.has("trainSize") || !params.get("trainSize").isJsonPrimitive()) {
            if(params.has("trainType") && params.get("trainType").isJsonPrimitive() && !TrainType.online.name().equals(params.get("trainType").getAsString())) {
                throw new IllegalArgumentException("Learner: " + name + " requires trainSize");
            }
        }
        if((!params.has("horizons") || !params.get("horizons").isJsonArray())
                && (!params.has("horizon") || !params.get("horizon").isJsonPrimitive())) {
            throw new IllegalArgumentException("Learner: " + name + " requires horizon or horizons");
        }

        // set attributes
        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        targetVariablesList = new ArrayList<>();
        expressionNames = new ArrayList<>();
        if(params.has("targetField") || params.has("targetFields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "targetField", "targetFields");
            this.targetFields = fieldsAndIsSingle.getKey();
            this.targetExpressions = new ArrayList<>();
            this.targetVariablesList.add(new HashSet<>(this.targetFields));
            this.isSingleTargetField = fieldsAndIsSingle.getValue();
        } else if(params.has("targetExpression") || params.has("targetExpressions")) {
            final KV<List<String>,Boolean> expressionsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "targetExpression", "targetExpressions");
            this.targetFields = new ArrayList<>();
            this.targetExpressions = expressionsAndIsSingle.getKey();
            for(final String expression : targetExpressions) {
                final Set<String> variables = ExpressionUtil.estimateVariables(expression);
                this.targetVariablesList.add(variables);
            }
            this.isSingleTargetField = expressionsAndIsSingle.getValue();
            if(!this.isSingleTargetField && params.has("expressionNames")) {
                for(final JsonElement expressionName : params.get("expressionNames").getAsJsonArray()) {
                    expressionNames.add(expressionName.getAsString());
                }
            }
        } else {
            throw new IllegalArgumentException("Learner step: " + name + " requires targetField(s) or targetExpression(s)");
        }

        this.featureFields = new ArrayList<>();
        for(final JsonElement featureField : params.get("featureFields").getAsJsonArray()) {
            if(featureField.isJsonNull() || !featureField.isJsonPrimitive()) {
                continue;
            }
            featureFields.add(featureField.getAsString());
        }
        if(featureFields.isEmpty()) {
            throw new IllegalArgumentException("Learner step: " + name + " requires featureFields over size zero");
        }

        final KV<List<String>,Boolean> horizonsAndIsSingle = Processor
                .getSingleMultiAttribute(params, "horizon", "horizons");
        this.horizons = horizonsAndIsSingle.getKey().stream()
                .map(Integer::valueOf)
                .collect(Collectors.toList());
        this.isSingleHorizon = horizonsAndIsSingle.getValue();
        if(horizons.isEmpty()) {
            throw new IllegalArgumentException("Learner step: " + name + " requires horizons over size zero");
        }
        this.horizonMax = horizons.stream().max(Integer::compareTo).orElse(0);

        this.trainSize = params.get("trainSize").getAsInt();
        this.trainSizeUnit = SizeUnit.valueOf(params.get("trainSizeUnit").getAsString());

        // train parameters
        if(params.has("trainType") && params.get("trainType").isJsonPrimitive()) {
            trainType = TrainType.valueOf(params.get("trainType").getAsString());
        } else {
            trainType = TrainType.minibatch;
        }

        if(params.has("trainIntervalSize") && params.get("trainIntervalSize").isJsonPrimitive()) {
            this.trainIntervalSize = params.get("trainIntervalSize").getAsInt();
        } else {
            this.trainIntervalSize = 1;
        }

        if(params.has("skipNullRows") && params.get("skipNullRows").isJsonPrimitive()) {
            this.skipNullRows = params.get("skipNullRows").getAsBoolean();
        } else {
            this.skipNullRows = false;
        }

        if(params.has("standardize") && params.get("standardize").isJsonPrimitive()) {
            this.standardize = params.get("standardize").getAsBoolean();
        } else {
            this.standardize = true;
        }

        this.targetAllVariables = new HashSet<>();
        final Set<String> set = new HashSet<>(featureFields);
        if(!targetFields.isEmpty()) {
            set.addAll(targetFields);
        } else if(!targetExpressions.isEmpty()) {
            for(int i = 0; i< targetExpressions.size(); i++) {
                final Set<String> targetVariables = targetVariablesList.get(i);
                set.addAll(targetVariables);
                for(final Integer horizon : horizons) {
                    final String targetBufferName = createTargetBufferName(i, horizon);
                    set.add(targetBufferName);
                }
                targetAllVariables.addAll(targetVariables);
            }
        } else {
            throw new IllegalStateException();
        }
        this.bufferSizes = ExpressionUtil.extractBufferSizes(set, horizonMax + trainSize, DEFAULT_VARIABLE_NAME_SEPARATOR);
    }

    abstract Model train(double[][] X, double[][] Y);
    abstract void save(Model model, ProcessingState state, int horizon);
    abstract Model load(ProcessingState state, int horizon, int inputSize, int outputSize);
    abstract Map<String,Object> predict(Model model, ProcessingBuffer buffer, Integer horizon);

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean ignore() {
        return this.ignore;
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
        return this.trainSizeUnit;
    }
    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        if(!this.targetFields.isEmpty()) {
            for(final String targetField : targetFields) {
                bufferTypes.put(targetField, inputTypes.get(targetField));
            }
        } else if(!this.targetExpressions.isEmpty()) {
            final Set<String> inputFields = ExpressionUtil.extractInputs(targetVariablesList, DEFAULT_VARIABLE_NAME_SEPARATOR);
            for(final String inputField : inputFields) {
                bufferTypes.put(inputField, inputTypes.get(inputField));
            }
            for(int i = 0; i<this.targetExpressions.size(); i++) {
                for(final Integer horizon : horizons) {
                    final String targetBufferName = createTargetBufferName(i, horizon);
                    bufferTypes.put(targetBufferName, Schema.FieldType.DOUBLE.withNullable(true));
                }
            }
        } else {
            throw new IllegalArgumentException("Processor: " + name + " has no target");
        }
        for(final String field : this.featureFields) {
            if(inputTypes.containsKey(field)) {
                bufferTypes.put(field, inputTypes.get(field));
            } else {
                throw new IllegalArgumentException("Processor: " + this.name + " field: " + field + " is not usable in inputs: " + inputTypes);
            }
        }
        return bufferTypes;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return this.bufferSizes;
    }

    @Override
    public void setup() {
        this.targetExpList = new ArrayList<>();
        if(!this.targetExpressions.isEmpty()) {
            for(int i = 0; i< targetExpressions.size(); i++) {
                final String  targetExpression = targetExpressions.get(i);
                final Set<String> targetVariables = targetVariablesList.get(i);
                final Expression expression = ExpressionUtil.createDefaultExpression(targetExpression, targetVariables);
                this.targetExpList.add(expression);
            }
        }
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {

        final String stateNameInterval = String.format("%s.interval", name);

        final int inputSize = featureFields.size() + 1;
        final int outputSize = 1;
        final Integer interval = state.getInt(stateNameInterval, 0) + 1;
        if(interval < trainIntervalSize) {
            state.putInt(stateNameInterval, interval);
        } else {
            state.putInt(stateNameInterval, 0);
        }

        // buffer expression target
        if(!targetExpList.isEmpty()) {
            bufferExpressionTargets(buffer, timestamp);
        }

        final Map<String, Object> outputs = new HashMap<>();
        for(int horizon : horizons) {
            // train
            Model model = load(state, horizon, inputSize, outputSize);
            if(interval >= trainIntervalSize) {
                double[][] X = buffer.getAsMatrix(featureFields, horizon, trainSize, trainSizeUnit, false);
                if(X.length < trainSize) {
                    continue;
                }
                double[][] Y = getTargets(buffer, horizon);
                if(Y.length < trainSize) {
                    continue;
                }

                if(skipNullRows) {
                    final List<Integer> notNullIndexes = getNotNullIndexes(X, Y);
                    if(notNullIndexes.size() < X.length) {
                        X = selectRows(X, notNullIndexes);
                        Y = selectRows(Y, notNullIndexes);
                    }
                }

                model = train(X, Y);

                if(model != null) {
                    save(model, state, horizon);
                }
            }

            // predict
            if(model != null) {
                final Map<String, Object> output = predict(model, buffer, horizon);
                outputs.putAll(output);
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

    private double[][] getTargets(final ProcessingBuffer buffer, final int horizon) {
        if(!targetFields.isEmpty()) {
            return buffer.getAsMatrix(targetFields, 0, trainSize, trainSizeUnit);
        } else if(!this.targetExpList.isEmpty()) {
            final List<String> targetBufferNames = new ArrayList<>();
            for(int i=0; i<targetExpList.size(); i++) {
                final String targetBufferName = createTargetBufferName(i, horizon);
                targetBufferNames.add(targetBufferName);
            }
            return buffer.getAsMatrix(targetBufferNames, 0, trainSize, trainSizeUnit);
        } else {
            throw new IllegalStateException();
        }
    }

    private void bufferExpressionTargets(ProcessingBuffer buffer, Instant timestamp) {
        if(targetExpList.isEmpty()) {
            return;
        }
        for(int horizon : horizons) {
            final Map<String, Double> values = new HashMap<>();
            for (final String input : this.bufferSizes.keySet()) {
                if (this.targetAllVariables.contains(input)) {
                    final Double latestTargetValue = buffer.getAsDouble(input, 0);
                    values.put(input, latestTargetValue);
                }

                final int bufferSize = this.bufferSizes.get(input);
                for (int variableIndex = 0; variableIndex < bufferSize; variableIndex++) {
                    final String variableName = Processor.createVariableName(input, DEFAULT_VARIABLE_NAME_SEPARATOR, variableIndex);
                    if (this.targetAllVariables.contains(variableName)) {
                        final int index = variableIndex + horizon;
                        final Double variableValue = buffer.getAsDouble(input, index);
                        values.put(variableName, variableValue);
                    }
                }
            }

            for(int i=0; i<targetExpList.size(); i++) {
                final Expression targetExpression = targetExpList.get(i);
                Double targetValue;
                try {
                    targetValue = targetExpression.setVariables(values).evaluate();
                } catch (Exception e) {
                    LOG.error("Failed to get y data cause: " + e + ", expression: " + this.targetExpressions.get(i) + " with inputs: " + values);
                    targetValue = Double.NaN;
                }
                final String targetBufferName = createTargetBufferName(i, horizon);
                buffer.add(targetBufferName, targetValue, timestamp);
            }

        }
    }

    private List<Integer> getNotNullIndexes(double[][] X, double[][] Y) {
        final List<Integer> notNullIndexes = new ArrayList<>();
        for(int index=0; index<X.length; index++) {
            boolean isNull = false;
            for(int col=0; col<X[index].length; col++) {
                double value = X[index][col];
                if(Double.isNaN(value) || Double.isInfinite(value)) {
                    isNull = true;
                    break;
                }
            }
            if(isNull) {
                continue;
            }

            for(int col=0; col<Y[index].length; col++) {
                double value = Y[index][col];
                if(Double.isNaN(value) || Double.isInfinite(value)) {
                    isNull = true;
                    break;
                }
            }
            if(isNull) {
                continue;
            }

            notNullIndexes.add(index);
        }

        return notNullIndexes;
    }

    private double[][] selectRows(double[][] data, List<Integer> indexes) {
        final double[][] newData = new double[indexes.size()][data[0].length];
        for(int i=0; i<indexes.size(); i++) {
            newData[i] = data[indexes.get(i)];
        }
        return newData;
    }

    private String createTargetBufferName(final Integer no, final Integer horizon) {
        return String.format("%s.%s.%d.%s.%d", name, DEFAULT_OUTPUT_NAME_EXPRESSION_TARGET_SUFFIX, no, DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX, horizon);
    }

    public enum TrainType {
        batch,
        minibatch,
        online

    }

}
