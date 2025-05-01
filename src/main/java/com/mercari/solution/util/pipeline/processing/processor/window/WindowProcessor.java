package com.mercari.solution.util.pipeline.processing.processor.window;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.values.KV;

import java.util.*;

public abstract class WindowProcessor implements Processor {

    protected final String name;

    protected final List<String> fields;
    protected final List<String> expressions;
    protected final List<KV<Integer,Integer>> ranges;

    protected final List<String> expressionNames;

    protected final String condition;
    protected final Boolean ignore;
    protected final SizeUnit sizeUnit;

    protected final Integer rangeMax;

    //
    protected final Boolean isSingleField;
    protected final Boolean isSingleRange;

    protected final List<Set<String>> variablesList;

    private final Set<String> allVariables;

    protected transient List<Expression> expressionList;
    protected transient Filter.ConditionNode conditionNode;


    public WindowProcessor(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(params.has("sizeUnit")) {
            this.sizeUnit = SizeUnit.valueOf(params.get("sizeUnit").getAsString());
        } else {
            this.sizeUnit = SizeUnit.count;
        }

        this.expressionNames = new ArrayList<>();
        this.variablesList = new ArrayList<>();
        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "field", "fields");
            this.fields = fieldsAndIsSingle.getKey();
            this.expressions = new ArrayList<>();
            this.variablesList.add(new HashSet<>(this.fields));
            this.isSingleField = fieldsAndIsSingle.getValue();
        } else if(params.has("expression") || params.has("expressions")) {
            final KV<List<String>,Boolean> expressionsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "expression", "expressions");
            this.fields = new ArrayList<>();
            this.expressions = expressionsAndIsSingle.getKey();
            for(final String expression : expressions) {
                final Set<String> variables = ExpressionUtil.estimateVariables(expression);
                this.variablesList.add(variables);
            }
            this.isSingleField = expressionsAndIsSingle.getValue();
            if(!this.isSingleField && params.has("expressionNames")) {
                for(final JsonElement expressionName : params.get("expressionNames").getAsJsonArray()) {
                    expressionNames.add(expressionName.getAsString());
                }
            }
        } else {
            throw new IllegalArgumentException("WindowProcessor step: " + name + " requires expression or field");
        }

        this.ranges = new ArrayList<>();
        if(params.has("range")) {
            final JsonElement range = params.get("range");
            if(range.isJsonPrimitive()) {
                ranges.add(KV.of(0, range.getAsInt()));
            } else if(range.isJsonArray() && range.getAsJsonArray().size() > 1) {
                ranges.add(KV.of(range.getAsJsonArray().get(0).getAsInt(), range.getAsJsonArray().get(1).getAsInt()));
            }
            this.isSingleRange = true;
        } else if(params.has("ranges")) {
            for(final JsonElement range : params.get("ranges").getAsJsonArray()) {
                if(range.isJsonPrimitive()) {
                    ranges.add(KV.of(0, range.getAsInt()));
                } else if(range.isJsonArray() && range.getAsJsonArray().size() > 1) {
                    ranges.add(KV.of(range.getAsJsonArray().get(0).getAsInt(), range.getAsJsonArray().get(1).getAsInt()));
                }
            }
            this.isSingleRange = false;
        } else {
            this.isSingleRange = true;
        }
        this.rangeMax = ranges.stream().map(KV::getValue).filter(Objects::nonNull).max(Integer::compareTo).orElse(0);

        this.allVariables = new HashSet<>(fields);
        for(final Set<String> variables : this.variablesList) {
            allVariables.addAll(variables);
        }

    }

    public abstract Integer getOffsetMax();

    @Override
    public String getName() {
        return name;
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
        return sizeUnit;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return ExpressionUtil.extractBufferSizes(allVariables, getOffsetMax(), DEFAULT_VARIABLE_NAME_SEPARATOR);
    }

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

    protected String createOutputName(final String field, final KV<Integer,Integer> range) {
        if(isSingleField && isSingleRange) {
            return name;
        } else if(isSingleRange) {
            return String.format("%s_%s", name, field);
        } else if(isSingleField) {
            return String.format("%s_%dto%d", name, range.getKey(), range.getValue());
        } else {
            return String.format("%s_%s_%dto%d", name, field, range.getKey(), range.getValue());
        }
    }

    protected String createOutputName(final Integer no, final KV<Integer,Integer> range) {
        if(isSingleField && isSingleRange) {
            return name;
        } else if(isSingleField) {
            return String.format("%s_%dto%d", name, range.getKey(), range.getValue());
        } else {
            final String expressionName;
            if(no < expressionNames.size()) {
                expressionName = expressionNames.get(no);
            } else {
                expressionName = "exp" + no;
            }
            if(isSingleRange) {
                return String.format("%s_%s", name, expressionName);
            } else {
                return String.format("%s_%s_%dto%d", name, expressionName, range.getKey(), range.getValue());
            }
        }
    }

}
