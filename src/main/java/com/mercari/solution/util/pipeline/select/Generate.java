package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.ExpressionUtil;
import freemarker.template.Template;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Generate implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Generate.class);

    private final String name;
    private final String from;
    private final String to;
    private final Integer interval;
    private final DateTimeUtil.TimeUnit intervalUnit;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Expression fromExpression;
    private transient Expression toExpression;
    private transient Template fromTemplate;
    private transient Template toTemplate;

    private transient Set<String> expressionVariables;

    private Generate(
            final String name,
            final String from,
            final String to,
            final Integer interval,
            final DateTimeUtil.TimeUnit intervalUnit,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.from = from;
        this.to = to;
        this.interval = interval;
        this.intervalUnit = intervalUnit;

        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Generate of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {

        if(!jsonObject.has("from") || !jsonObject.has("to") ) {
            throw new IllegalArgumentException("SelectField generate: " + name + " requires from or to parameter");
        }
        final String from = jsonObject.get("from").getAsString();
        final String to   = jsonObject.get("to").getAsString();

        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField generate: " + name + " requires type parameter");
        }
        final String type = SelectFunction.getStringParameter(name, jsonObject, "type", "integer");
        final Schema.FieldType outputFieldType = Schema.FieldType.type(type);

        final DateTimeUtil.TimeUnit intervalUnit;
        if(jsonObject.has("intervalUnit")) {
            final String intervalTypeString = jsonObject.get("intervalUnit").getAsString();
            intervalUnit = DateTimeUtil.TimeUnit.valueOf(intervalTypeString);
        } else {
            intervalUnit = switch (outputFieldType.getType()) {
                case timestamp, time -> DateTimeUtil.TimeUnit.minute;
                case date -> DateTimeUtil.TimeUnit.day;
                default -> DateTimeUtil.TimeUnit.minute;
            };
        }

        final int interval;
        if(jsonObject.has("interval")) {
            if(!jsonObject.get("interval").isJsonPrimitive() || !jsonObject.get("interval").getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("SelectField generate: " + name + ".interval parameter must be integer");
            }
            interval = jsonObject.get("interval").getAsInt();
        } else {
            interval = 1;
        }

        return new Generate(
                name, from, to, interval, intervalUnit,
                inputFields, outputFieldType, ignore);
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
        return Schema.FieldType.array(outputFieldType);
    }

    @Override
    public void setup() {
        switch (outputFieldType.getType()) {
            case int32, int64, float32, float64 -> {
                final Set<String> fromExpressionVariables = ExpressionUtil.estimateVariables(this.from);
                final Set<String> toExpressionVariables = ExpressionUtil.estimateVariables(this.to);
                this.fromExpression = ExpressionUtil.createDefaultExpression(this.from, fromExpressionVariables);
                this.toExpression = ExpressionUtil.createDefaultExpression(this.to, toExpressionVariables);
                fromExpressionVariables.addAll(toExpressionVariables);
                this.expressionVariables = fromExpressionVariables;
            }
            case date, time, timestamp -> {
                this.fromTemplate = TemplateUtil.createStrictTemplate(name, this.from);
                this.toTemplate = TemplateUtil.createStrictTemplate(name, this.to);
            }
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(input == null) {
            return null;
        }
        final List<Object> outputs = new ArrayList<>();
        switch (outputFieldType.getType()) {
            case int32, int64, float32, float64 -> {
                final Map<String, Double> values = new HashMap<>();
                for(final String variable : expressionVariables) {
                    final Object object = input.get(variable);
                    final Double value = ExpressionUtil.getAsDouble(object, Double.NaN);
                    values.put(variable, value);
                }
                final long fromN = Double.valueOf(this.fromExpression.setVariables(values).evaluate()).longValue();
                final long toN   = Double.valueOf(this.toExpression.setVariables(values).evaluate()).longValue();
                long currentN = fromN;
                while(currentN < toN) {
                    final Object output = switch (outputFieldType.getType()) {
                        case int64 -> currentN;
                        case int32 -> Long.valueOf(currentN).intValue();
                        case float32 -> Long.valueOf(currentN).floatValue();
                        case float64 -> Long.valueOf(currentN).doubleValue();
                        default -> throw new IllegalArgumentException();
                    };
                    outputs.add(output);
                    currentN += interval;
                }
            }
            case timestamp -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(intervalUnit);
                final String fromString = TemplateUtil.executeStrictTemplate(this.fromTemplate, input);
                final String toString   = TemplateUtil.executeStrictTemplate(this.toTemplate, input);
                final java.time.Instant fromInstant = DateTimeUtil.toInstant(fromString);
                final java.time.Instant toInstant = DateTimeUtil.toInstant(toString);
                java.time.Instant currentInstant = java.time.Instant.from(fromInstant);
                while (currentInstant.isBefore(toInstant)) {
                    outputs.add(DateTimeUtil.toEpochMicroSecond(currentInstant));
                    currentInstant = currentInstant.plus(interval, chronoUnit);
                }
            }
            case date, time -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(intervalUnit);
                final String fromString = TemplateUtil.executeStrictTemplate(this.fromTemplate, input);
                final String toString   = TemplateUtil.executeStrictTemplate(this.toTemplate, input);
                if(Schema.Type.date.equals(outputFieldType.getType())) {
                    final LocalDate fromDate = DateTimeUtil.toLocalDate(fromString);
                    final LocalDate toDate = DateTimeUtil.toLocalDate(toString);
                    LocalDate currentDate = LocalDate.from(fromDate);
                    while (currentDate.isBefore(toDate)) {
                        outputs.add(Long.valueOf(currentDate.toEpochDay()).intValue());
                        currentDate = currentDate.plus(interval, chronoUnit);
                    }
                } else if(Schema.Type.time.equals(outputFieldType.getType())) {
                    final LocalTime fromTime = DateTimeUtil.toLocalTime(fromString);
                    final LocalTime toTime   = DateTimeUtil.toLocalTime(toString);
                    LocalTime currentTime = LocalTime.from(fromTime);
                    while(currentTime.isBefore(toTime)) {
                        outputs.add(DateTimeUtil.toMicroOfDay(currentTime));
                        currentTime = currentTime.plus(interval, chronoUnit);
                        if(!currentTime.isAfter(fromTime)) {
                            break;
                        }
                    }
                }
            }
            default -> throw new IllegalArgumentException();
        }

        return outputs;
    }

}
