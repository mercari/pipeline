package com.mercari.solution.util.pipeline.select.stateful;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.aggregation.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

public interface StatefulFunction extends SelectFunction {

    class Range implements Serializable {

        public Integer count;
        public Duration duration;

        public Integer offsetCount;
        public Duration offsetDuration;

        public RangeType type;

        private static Range of(final JsonObject jsonObject) {
            if(jsonObject == null || jsonObject.isJsonNull()) {
                return empty();
            }

            final Range range = new Range();
            if(jsonObject.has("count")) {
                range.count = jsonObject.get("count").getAsInt();
            }

            final DateTimeUtil.TimeUnit unit;
            if(jsonObject.has("duration")) {
                final Long durationAmount = jsonObject.get("duration").getAsLong();

                if(jsonObject.has("unit")) {
                    unit = DateTimeUtil.TimeUnit.valueOf(jsonObject.get("unit").getAsString());
                } else {
                    unit = DateTimeUtil.TimeUnit.second;
                }
                range.duration = DateTimeUtil.getDuration(unit, durationAmount);
            } else {
                unit = DateTimeUtil.TimeUnit.second;
            }

            if(range.count != null) {
                range.type = RangeType.count;
            } else if(range.duration != null) {
                range.type = RangeType.duration;
            } else {
                throw new IllegalModuleException("select function range must contain count or duration parameter");
            }

            if(jsonObject.has("offset")) {
                final Integer offset = jsonObject.get("offset").getAsInt();
                switch (range.type) {
                    case count -> {
                        range.offsetCount = offset;
                        range.offsetDuration = Duration.ZERO;
                    }
                    case duration -> {
                        range.offsetCount = 0;
                        range.offsetDuration = DateTimeUtil.getDuration(unit, offset.longValue());
                    }
                }
            } else {
                range.offsetCount = 0;
                range.offsetDuration = Duration.ZERO;
            }

            return range;
        }

        private static Range empty() {
            final Range range = new Range();
            range.type = RangeType.empty;
            return range;
        }

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(RangeType.empty.equals(type)) {
                return errorMessages;
            }

            if(count == null && duration == null) {
                errorMessages.add("");
            } else if(count != null && duration != null) {
                errorMessages.add("");
            } else if(count != null && count < 1) {
                errorMessages.add("");
            } else if(duration != null && duration.getStandardSeconds() < 1) {
                errorMessages.add("");
            }

            if(offsetCount != null && offsetCount < 0) {
                errorMessages.add("");
            } else if(offsetDuration != null && offsetDuration.getMillis() < 0) {
                errorMessages.add("");
            }
            return errorMessages;
        }

        public boolean filter(final Instant timestamp, final Instant bufferTimestamp, final Integer bufferCount) {
            return switch (type) {
                case count -> {
                    final boolean flag = bufferCount < this.count && bufferCount >= 0;
                    if(offsetCount != null && offsetCount == 0) {
                        yield flag;
                    } else {
                        yield flag; // TODO offsetCount
                    }
                }
                case duration -> {
                    final boolean flag = bufferTimestamp.isAfter(timestamp.minus(duration)) || bufferTimestamp.isEqual(timestamp.minus(duration));
                    if(offsetDuration != null && offsetDuration.getMillis() == 0) {
                        yield flag;
                    } else {
                        yield flag && (bufferTimestamp.isBefore(timestamp.minus(offsetDuration)) || bufferTimestamp.isEqual(timestamp.minus(offsetDuration)));
                    }
                }
                case empty -> true;
            };
        }

        private Duration getDuration() {
            if(duration == null) {
                return Duration.ZERO;
            }
            return duration;
        }

        public enum RangeType {
            count,
            duration,
            empty
        }

    }

    class RangeBound implements Serializable {

        public final Integer maxCount;
        public final Duration maxDuration;

        private RangeBound(final Integer maxCount, final Duration maxDuration) {
            this.maxCount = Optional.ofNullable(maxCount).orElse(0);
            this.maxDuration = Optional.ofNullable(maxDuration).orElse(Duration.ZERO);
        }

        public static RangeBound of(final Integer maxCount, final Duration maxDuration) {
            return new RangeBound(maxCount, maxDuration);
        }

        public Instant firstTimestamp(final Instant eventTime) {
            return eventTime.minus(maxDuration);
        }

        @Override
        public String toString() {
            return String.format("maxCount: %d, maxDuration: %s", maxCount, maxDuration);
        }
    }

    enum Func implements Serializable {
        lag;

        public static Func is(String value) {
            for(final Func func : values()) {
                if(func.name().equals(value)) {
                    return func;
                }
            }
            return null;
        }
    }

    Boolean filter(MElement input);
    List<String> validate(int parent, int index);
    Accumulator addInput(Accumulator accumulator, MElement values, Integer count, Instant timestamp);
    Object extractOutput(Accumulator accumulator, Map<String, Object> values);
    Range getRange();

    static StatefulFunction of(final JsonElement element, final List<Schema.Field> inputFields) {
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            return null;
        }

        final JsonObject params = element.getAsJsonObject();
        if (!params.has("op") && !params.has("func")) {
            throw new IllegalArgumentException("Aggregator requires func or op parameter");
        }

        final String name;
        if(params.has("name")) {
            name = params.get("name").getAsString();
        } else {
            name = null;
        }

        final String condition;
        if(params.has("condition")) {
            condition = params.get("condition").toString();
        } else {
            condition = null;
        }

        final String expression;
        if(params.has("expression")) {
            expression = params.get("expression").getAsString();
        } else {
            expression = null;
        }

        final boolean ignore;
        if(params.has("ignore")) {
            ignore = params.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        final Range range;
        if(params.has("range")) {
            final JsonElement rangeJson = params.get("range");
            if(!rangeJson.isJsonObject()) {
                throw new IllegalArgumentException("Aggregator requires func or op parameter");
            }
            range = Range.of(rangeJson.getAsJsonObject());
        } else {
            range = Range.empty();
        }

        final Func op;
        if(params.has("op")) {
            op = Func.is(params.get("op").getAsString());
        } else {
            op = Func.is(params.get("func").getAsString());
        }

        return switch (op) {
            case lag -> Lag.of(name, inputFields, expression, condition, ignore);
            case null -> AggregateFunction.of(element, inputFields, range);
        };
    }

    static RangeBound calcMaxRange(final List<SelectFunction> selectFunctions) {

        int countMax = 0;
        Duration durationMax = Duration.standardSeconds(0L);
        for(final SelectFunction selectFunction : selectFunctions) {
            if(!(selectFunction instanceof StatefulFunction statefulFunction)) {
                continue;
            }

            if(statefulFunction.getRange() != null) {
                switch (statefulFunction.getRange().type) {
                    case count -> {
                        if(statefulFunction.getRange().count > countMax) {
                            countMax = statefulFunction.getRange().count;
                        }
                    }
                    case duration -> {
                        if(statefulFunction.getRange().getDuration().compareTo(durationMax) > 0) {
                            durationMax = statefulFunction.getRange().getDuration();
                        }
                    }
                }
            }
        }

        return RangeBound.of(countMax, durationMax);
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

}
