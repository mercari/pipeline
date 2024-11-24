package com.mercari.solution.module;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@DefaultCoder(AvroCoder.class)
public class MFailure implements Serializable {

    private String job;
    private String module;
    private String input;
    private String error;
    private Instant timestamp;
    private Instant eventtime;

    public String getJob() {
        return job;
    }

    public String getModule() {
        return module;
    }

    public String getInput() {
        return input;
    }

    public String getError() {
        return error;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Instant getEventtime() {
        return eventtime;
    }

    public static MFailure of(
            final String job,
            final String module,
            final String input,
            final String error,
            final Instant eventtime) {

        final MFailure failureElement = new MFailure();
        failureElement.job = job;
        failureElement.module = module;
        failureElement.input = input;
        failureElement.error = error;
        failureElement.timestamp = Instant.now();
        failureElement.eventtime = eventtime;
        return failureElement;
    }

    public static MFailure of(
            final String job,
            final String module,
            final String input,
            final Throwable error,
            final Instant eventtime) {

        final MFailure failure = new MFailure();
        failure.job = job;
        failure.module = module;
        failure.input = input;
        failure.error = convertThrowableMessage(error);
        failure.timestamp = Instant.now();
        failure.eventtime = eventtime;
        return failure;
    }

    public MElement toElement(Instant eventTime) {
        final Map<String, Object> values = new HashMap<>();
        values.put("job", job);
        values.put("module", module);
        values.put("input", input);
        values.put("error", error);
        values.put("timestamp", timestamp.getMillis() * 1000L);
        values.put("eventtime", eventtime.getMillis() * 1000L);
        return MElement.of(schema(), values, eventTime);
    }

    public static Schema schema() {
        return Schema.builder()
                .withField("job", Schema.FieldType.STRING.withNullable(true))
                .withField("module", Schema.FieldType.STRING.withNullable(false))
                .withField("input", Schema.FieldType.STRING.withNullable(true))
                .withField("error", Schema.FieldType.STRING.withNullable(true))
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .withField("eventtime", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static String convertThrowableMessage(final Throwable e) {

        final StringBuilder sb = new StringBuilder();
        if(e.getMessage() != null) {
            sb.append(e.getMessage());
            sb.append("\n");
        } else if(e.getLocalizedMessage() != null) {
            sb.append(e.getLocalizedMessage());
            sb.append("\n");
        }
        if(e.getCause() != null) {
            sb.append(e.getCause());
            sb.append("\n");
        }
        for(final StackTraceElement stackTrace : e.getStackTrace()) {
            sb.append(stackTrace.toString());
            sb.append("\n");
        }

        return sb.toString();
    }

}
