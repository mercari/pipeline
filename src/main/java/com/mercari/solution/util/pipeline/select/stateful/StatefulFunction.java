package com.mercari.solution.util.pipeline.select.stateful;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.aggregation.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface StatefulFunction extends SelectFunction {

    class Range implements Serializable {

        public String name;

        public Integer count;
        public Duration duration;

        public Integer offsetCount;
        public Duration offsetDuration;

        public RangeType type;

        public Boolean isSingle;

        public static List<Range> of(final JsonArray jsonArray) {
            final List<Range> ranges = new ArrayList<>();
            for(final JsonElement element : jsonArray) {
                final Range range = of(null, element.getAsJsonObject());
                range.isSingle = false;
                ranges.add(range);
            }
            return ranges;
        }

        public static Range of(final String name, final JsonObject jsonObject) {
            if(jsonObject == null || jsonObject.isJsonNull()) {
                return null;
            }

            final Range range = new Range();
            if(jsonObject.has("name")) {
                range.name = jsonObject.get("name").getAsString();
            } else {
                range.name = name;
            }
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

            range.isSingle = true;
            return range;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
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
                case count -> bufferCount < this.count;
                case duration -> {
                    final boolean flag = bufferTimestamp.isAfter(timestamp.minus(duration));
                    if(offsetDuration != null && offsetDuration.getMillis() == 0) {
                        yield flag;
                    } else {
                        yield flag && (bufferTimestamp.isBefore(timestamp.minus(offsetDuration)) || bufferTimestamp.isEqual(timestamp.minus(offsetDuration)));
                    }
                }
            };
        }

        public Duration getDuration() {
            if(duration == null) {
                return Duration.ZERO;
            }
            return duration;
        }

        public enum RangeType {
            count,
            duration
        }

    }

    class RangeBound implements Serializable {

        public final Integer maxCount;
        public final Duration maxDuration;

        private RangeBound(final Integer maxCount, final Duration maxDuration) {
            this.maxCount = maxCount;
            this.maxDuration = maxDuration;
        }

        public static RangeBound of(final Integer maxCount, final Duration maxDuration) {
            return new RangeBound(maxCount, maxDuration);
        }

        public Instant firstTimestamp(final Instant eventTime) {
            return eventTime.minus(maxDuration);
        }

        public Integer firstCount() {
            return maxCount;
        }

        @Override
        public String toString() {
            return String.format("maxCount: %d, maxDuration: %s", maxCount, maxDuration);
        }
    }

    enum Func implements Serializable {
        multi_regression;

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
    List<Range> getRanges();

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

        final boolean ignore;
        if(params.has("ignore")) {
            ignore = params.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        final List<Range> ranges = new ArrayList<>();
        if(params.has("range")) {
            final JsonElement rangeJson = params.get("range");
            if(!rangeJson.isJsonObject()) {
                throw new IllegalArgumentException("Aggregator requires func or op parameter");
            }
            final Range range = Range.of(name, rangeJson.getAsJsonObject());
            ranges.add(range);
        } else if(params.has("ranges")) {
            final JsonElement rangesJson = params.get("ranges");
            if(!rangesJson.isJsonArray()) {
                throw new IllegalArgumentException("Aggregator requires func or op parameter");
            }
            final List<Range> rangesList = Range.of(rangesJson.getAsJsonArray());
            ranges.addAll(rangesList);
        }

        final Func op;
        if(params.has("op")) {
            op = Func.is(params.get("op").getAsString());
        } else {
            op = Func.is(params.get("func").getAsString());
        }

        return switch (op) {
            case multi_regression -> Count.of(name, condition, ranges, ignore);
            case null -> AggregateFunction.of(element, inputFields, ranges);
        };
    }

    static Accumulator addInput(
            Accumulator accumulator,
            final List<SelectFunction> selectFunctions,
            final MElement input,
            final Instant timestamp,
            final Integer count) {

        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            if(selectFunction instanceof StatefulFunction statefulFunction) {
                accumulator = statefulFunction.addInput(accumulator, input, count, timestamp);
            }
        }
        return accumulator;
    }

    static RangeBound calcMaxRange(final List<SelectFunction> selectFunctions) {

        int countMax = 0;
        Duration durationMax = Duration.standardSeconds(0L);
        for(final SelectFunction selectFunction : selectFunctions) {
            if(!(selectFunction instanceof StatefulFunction statefulFunction)) {
                continue;
            }
            final List<Range> ranges = statefulFunction.getRanges();
            for(final Range range : ranges) {
                switch (range.type) {
                    case count -> {
                        if(range.count > countMax) {
                            countMax = range.count;
                        }
                    }
                    case duration -> {
                        if(range.getDuration().compareTo(durationMax) > 0) {
                            durationMax = range.getDuration();
                        }
                    }
                }
            }
        }

        return RangeBound.of(countMax, durationMax);
    }

}
