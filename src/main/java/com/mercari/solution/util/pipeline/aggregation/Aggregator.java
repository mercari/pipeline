package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import net.objecthunter.exp4j.Expression;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;


public interface Aggregator extends SelectFunction {

    class Range implements Serializable {
        public Integer count;
        public Integer duration;
        public DateTimeUtil.TimeUnit durationUnit;
    }

    enum Op {
        count,
        max,
        min,
        argmax,
        argmin,
        last,
        first,
        sum,
        avg,
        std,
        regression,
        array_agg,
        any
    }

    Boolean filter(MElement input);
    List<String> validate(int parent, int index);
    Accumulator addInput(Accumulator accumulator, MElement input);
    Accumulator mergeAccumulator(Accumulator base, Accumulator input);
    Object extractOutput(Accumulator accumulator, Map<String, Object> values);


    static Aggregator of(final JsonElement element, final Schema inputSchema) {
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            return null;
        }

        final JsonObject params = element.getAsJsonObject();
        if (!params.has("op") && !params.has("func")) {
            throw new IllegalArgumentException("Aggregator requires func or op parameter");
        }

        final String name;
        final String field;
        final String expression;
        final String condition;
        final Boolean ignore;

        if(params.has("name")) {
            name = params.get("name").getAsString();
        } else {
            name = null;
        }

        if(params.has("field")) {
            field = params.get("field").getAsString();
        } else {
            field = null;
        }
        if(params.has("expression")) {
            expression = params.get("expression").getAsString();
        } else {
            expression = null;
        }

        if(params.has("condition")) {
            condition = params.get("condition").toString();
        } else {
            condition = null;
        }

        if(params.has("ignore")) {
            ignore = params.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        final Op op;
        if(params.has("op")) {
            op = Op.valueOf(params.get("op").getAsString());
        } else {
            op = Op.valueOf(params.get("func").getAsString());
        }

        return switch (op) {
            case count -> Count.of(name, condition, ignore);
            case sum -> Sum.of(name, inputSchema, field, expression, condition, ignore);
            case max -> Max.of(name, inputSchema, field, expression, condition, ignore, false);
            case min -> Max.of(name, inputSchema, field, expression, condition, ignore, true);
            case last -> Last.of(name, inputSchema, condition, ignore, params, false);
            case first -> Last.of(name, inputSchema, condition, ignore, params, true);
            case argmax -> ArgMax.of(name, inputSchema, condition, ignore, params);
            case argmin -> ArgMax.of(name, inputSchema, condition, ignore, params, true);
            case avg -> Avg.of(name, inputSchema, field, expression, condition, ignore, params);
            case std -> Std.of(name, inputSchema, field, expression, condition, ignore, params);
            case regression -> SimpleRegression.of(name, inputSchema, field, expression, condition, ignore, params);
            case array_agg -> ArrayAgg.of(name, inputSchema, condition, ignore, params);
            default -> throw new IllegalArgumentException("Not supported aggregation op: " + op);
        };
    }

    static Boolean filter(final Filter.ConditionNode conditionNode, final MElement element) {
        if(conditionNode == null) {
            return true;
        }
        final Map<String, Object> values = new HashMap<>();
        for(final String variable : conditionNode.getRequiredVariables()) {
            values.put(variable, element.getPrimitiveValue(variable));
        }
        return Filter.filter(conditionNode, values);
    }

    static Double eval(final Expression expression, final Set<String> variables, final MElement element) {
        final Map<String, Double> values = new HashMap<>();
        for(final String variable : variables) {
            final Double value = element.getAsDouble(variable);
            values.put(variable, Optional.ofNullable(value).orElse(Double.NaN));
        }
        double expResult = expression.setVariables(values).evaluate();
        return Double.isNaN(expResult) ? null : expResult;
    }

    static boolean compare(final Object v1, final Object v2) {
        return compare(v1, v2, false);
    }

    static boolean compare(final Object v1, final Object v2, final boolean opposite) {
        if(v1 == null) {
            return false;
        } else if(v2 == null) {
            return true;
        }

        final Comparable m = (Comparable) v1;
        final Comparable i = (Comparable) v2;
        final boolean result = m.compareTo(i) > 0;

        if(opposite) {
            return !result;
        } else {
            return result;
        }
    }

    static Object max(final Object v1, final Object v2) {
        return max(v1, v2, false);
    }

    static Object max(final Object v1, final Object v2, final boolean opposite) {
        if(v1 == null) {
            return v2;
        } else if(v2 == null) {
            return v1;
        }

        final Comparable m = (Comparable) v1;
        final Comparable i = (Comparable) v2;
        final boolean flag = m.compareTo(i) > 0;

        if(opposite) {
            return flag ? v2 : v1;
        } else {
            return flag ? v1 : v2;
        }
    }

    static Object sum(final Object v1, final Object v2) {
        if(v1 == null) {
            return v2;
        } else if(v2 == null) {
            return v1;
        }

        return switch (v1) {
            case Double v1_ -> v1_ + (Double) v2;
            case Long v1_ -> v1_ + (Long) v2;
            case Float v1_ -> v1_ + (Float) v2;
            case Integer v1_ -> v1_ + (Integer) v2;
            case Short v1_ -> v1_ + (Short) v2;
            case BigDecimal v1_ -> v1_.add((BigDecimal) v2);
            case Boolean v1_ -> v1_ || (Boolean) v2;
            case String v1_ -> v1_ + (String) v2;
            default -> new IllegalArgumentException("Sum not supported type object: " + v1);
        };
    }

    static Double avg(final Double avg1, final Double weight1, final Double avg2, final Double weight2) {
        if(avg1 == null) {
            return avg2;
        } else if(avg2 == null) {
            return avg1;
        }

        if(weight1 == null || weight1 == 0) {
            return avg2;
        } else if(weight2 == null || weight2 == 0) {
            return avg1;
        }

        return (weight1 * avg1 + weight2 * avg2) / (weight1 + weight2);
    }

}
