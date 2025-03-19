package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EventTimestamp implements SelectFunction {

    private final String name;
    private final Long duration;
    private final DateTimeUtil.TimeUnit durationUnit;
    private final DateTimeUtil.TimeUnit cutoffUnit;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    EventTimestamp(String name, Long duration, DateTimeUtil.TimeUnit durationUnit, DateTimeUtil.TimeUnit cutoffUnit, boolean ignore) {
        this.name = name;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.TIMESTAMP.withNullable(true);

        this.duration = duration;
        this.durationUnit = durationUnit;
        this.cutoffUnit = cutoffUnit;
        this.ignore = ignore;
    }

    public static EventTimestamp of(String name, final JsonObject jsonObject, boolean ignore) {

        final Long duration;
        final DateTimeUtil.TimeUnit durationUnit;
        if(jsonObject.has("duration") || jsonObject.has("durationUnit")) {
            if(!jsonObject.has("duration") || !jsonObject.has("durationUnit")) {
                throw new IllegalArgumentException("SelectField event_timestamp: " + name + " requires both duration and durationUnit if duration is specified");
            }
            duration = jsonObject.get("duration").getAsLong();
            durationUnit = DateTimeUtil.TimeUnit.valueOf(jsonObject.get("durationUnit").getAsString());
        } else {
            duration = null;
            durationUnit = null;
        }

        final DateTimeUtil.TimeUnit cutoffUnit;
        if(jsonObject.has("cutoff")) {
            cutoffUnit = DateTimeUtil.TimeUnit.valueOf(jsonObject.get("cutoff").getAsString());
        } else {
            cutoffUnit = null;
        }

        return new EventTimestamp(name, duration, durationUnit, cutoffUnit, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        Instant instant;
        if(duration != null) {
            final Duration d = DateTimeUtil.getDuration(durationUnit, duration);
            instant = timestamp.plus(d);
        } else {
            instant = timestamp;
        }

        if(cutoffUnit != null) {
            //instant = DateTimeUtil.reduceAccuracy()
        }
        return timestamp.getMillis() * 1000L;
    }

}
