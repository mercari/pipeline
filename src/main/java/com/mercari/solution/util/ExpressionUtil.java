package com.mercari.solution.util;

import com.google.common.collect.Sets;
import com.mercari.solution.module.MElement;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.function.Function;
import net.objecthunter.exp4j.operator.Operator;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class ExpressionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionUtil.class);

    private static final String DEFAULT_SEPARATOR = "_";
    public static final Pattern DELIMITER_PATTERN = Pattern.compile("[()+\\-*/%^<>=!&|#§$~:,]");
    public static final Pattern FIELD_NO_PATTERN = Pattern.compile("[a-zA-Z_]\\w*_([0-9]\\d*)$");

    private static final String REPLACEMENT_FIELD_FORMAT = "%s___%d";
    private static final String REPLACEMENT_ARRAY = "$1___$2";
    private static final String REGEX_ARRAY = "(\\w+)\\[(\\d+)]";
    private static final Pattern PATTERN_ARRAY = Pattern.compile(REGEX_ARRAY);

    private static final String[] RESERVED_NAMES = {
            "pi","π","e","φ",
            "abs","acos","asin","atan","cbrt","ceil","cos","cosh",
            "exp","floor","log","log10","log2","sin","sinh","sqrt","tan","tanh","signum",
            "if","switch","max","min",
            "timestamp_to_date",
            "timestamp_diff_millisecond","timestamp_diff_second","timestamp_diff_minute","timestamp_diff_hour","timestamp_diff_day"};
    private static final Set<String> RESERVED_NAMES_SET = new HashSet<>(Arrays.asList(RESERVED_NAMES));

    public static Set<String> estimateVariables(final String expression) {
        return estimateVariables(expression, true);
    }

    private static Set<String> estimateVariables(String expression, boolean includeArray) {
        if(expression == null) {
            return new HashSet<>();
        }

        if(includeArray) {
            expression = expression.replaceAll(REGEX_ARRAY, "$1");
        } else {
            expression = expression.replaceAll(REGEX_ARRAY, "");
        }

        final String str = expression.replaceAll(" ","");
        final Scanner scanner = new Scanner(str);
        scanner.useDelimiter(DELIMITER_PATTERN);

        final Set<String> variables = new HashSet<>();
        while(scanner.hasNext()) {
            final String variable = scanner.next();
            if(!variable.isEmpty() && !NumberUtils.isCreatable(variable) && !RESERVED_NAMES_SET.contains(variable)) {
                variables.add(variable);
            }
        }

        return variables;
    }

    public static Expression createDefaultExpression(final String expression) {
        return createDefaultExpression(expression, null);
    }

    public static Expression createDefaultExpression(final String expression, Collection<String> variables) {
        if(variables == null) {
            variables = estimateVariables(expression);
        }
        return new ExpressionBuilder(expression)
                .variables(new HashSet<>(variables))
                .operator(
                        new EqualOperator(),
                        new NotEqualOperator(),
                        new GreaterOperator(),
                        new GreaterOrEqualOperator(),
                        new LesserOperator(),
                        new LesserOrEqualOperator(),
                        new NotOperator(),
                        new AndOperator(),
                        new OrOperator())
                .functions(
                        new IfFunction(),
                        new SwitchFunction(3),
                        new SwitchFunction(4),
                        new SwitchFunction(5),
                        new SwitchFunction(6),
                        new SwitchFunction(7),
                        new SwitchFunction(8),
                        new MaxFunction(),
                        new MinFunction(),
                        new TimestampToDateFunction(),
                        new TimestampDiffFunction("millisecond"),
                        new TimestampDiffFunction("second"),
                        new TimestampDiffFunction("minute"),
                        new TimestampDiffFunction("hour"),
                        new TimestampDiffFunction("day"))
                .build();
    }

    public static String replaceArrayFieldName(final String variable, final int index) {
        return String.format(REPLACEMENT_FIELD_FORMAT, variable, index);
    }

    public static String replaceArrayExpression(final String expression) {
        if(expression == null) {
            return null;
        }
        String a = expression.replaceAll(REGEX_ARRAY, REPLACEMENT_ARRAY);
        return a.replaceAll("___0", "");
    }

    public static Map<String,Set<Integer>> extractArrayIndexes(final String expression) {
        final Map<String, Set<Integer>> variables = new HashMap<>();
        final Matcher matcher = PATTERN_ARRAY.matcher(expression);
        while(matcher.find()) {
            final String name = matcher.group(1);
            final Integer value = Integer.parseInt(matcher.group(2));
            variables.merge(name, Set.of(value), Sets::union);
        }
        final Set<String> notArrayVariables = estimateVariables(expression, false);
        for(final String variable : notArrayVariables) {
            variables.merge(variable, Set.of(0), Sets::union);
        }
        return variables;
    }

    public static Map<Integer,Set<String>> reverseArrayIndexes(final String expression) {
        final Map<String,Set<Integer>> arrayIndexes = extractArrayIndexes(expression);
        return reverseArrayIndexes(arrayIndexes);
    }

    public static Map<Integer,Set<String>> reverseArrayIndexes(final Map<String,Set<Integer>> arrayIndexes) {
        final Map<Integer, Set<String>> reverseVariables = new HashMap<>();
        if(arrayIndexes == null || arrayIndexes.isEmpty()) {
            return reverseVariables;
        }
        final Set<Integer> indexes = arrayIndexes.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        for(final Integer index : indexes) {
            for(Map.Entry<String,Set<Integer>> entry : arrayIndexes.entrySet()) {
                if(entry.getValue().contains(index)) {
                    reverseVariables.merge(index, Set.of(entry.getKey()), Sets::union);
                }
            }
        }
        return reverseVariables;
    }

    public static Integer maxArrayIndex(final Map<Integer,Set<String>> reverseArrayIndexes) {
        return reverseArrayIndexes.keySet().stream().max(Integer::compareTo).orElse(0);
    }


    public static Map<String, Integer> extractBufferSizes(final Set<String> variables) {
        return extractBufferSizes(variables, 0, DEFAULT_SEPARATOR);
    }

    public static Map<String, Integer> extractBufferSizes(final Set<String> variables, final String separator) {

        return extractBufferSizes(variables, 0, separator);
    }

    public static Map<String, Integer> extractBufferSizes(
            final Set<String> variables,
            final Integer offset,
            final String separator) {

        final Map<String, Integer> bufferSizes = new HashMap<>();
        if(variables == null || variables.isEmpty()) {
            return bufferSizes;
        }

        final Pattern indexPattern;
        if(DEFAULT_SEPARATOR.equals(separator)) {
            indexPattern = FIELD_NO_PATTERN;
        } else {
            final String indexFieldPatternText = String.format("[a-zA-Z_]\\w*%s([0-9]\\d*)$", separator);
            indexPattern = Pattern.compile(indexFieldPatternText);
        }

        for(final String variable : variables) {
            final Matcher matcher = indexPattern.matcher(variable);
            if(matcher.find()) {
                final String var = matcher.group();
                final String[] fieldAndArg = var.split(separator);
                final String field = String.join(separator, Arrays.copyOfRange(fieldAndArg, 0, fieldAndArg.length-1));
                final Integer size = Integer.parseInt(fieldAndArg[fieldAndArg.length-1]);
                if(size + offset > bufferSizes.getOrDefault(field, 0)) {
                    bufferSizes.put(field, size + offset);
                }
            } else if(!bufferSizes.containsKey(variable)) {
                bufferSizes.put(variable, offset);
            }
        }
        return bufferSizes;
    }

    public static Set<String> extractInputs(final Set<String> variables, final String separator) {
        final Set<String> inputs = new HashSet<>();
        if(variables == null || variables.isEmpty()) {
            return inputs;
        }

        final Pattern indexPattern;
        if(DEFAULT_SEPARATOR.equals(separator)) {
            indexPattern = FIELD_NO_PATTERN;
        } else {
            final String indexFieldPatternText = String.format("[a-zA-Z_]\\w*%s([0-9]\\d*)$", separator);
            indexPattern = Pattern.compile(indexFieldPatternText);
        }

        for(final String variable : variables) {
            final Matcher matcher = indexPattern.matcher(variable);
            if(matcher.find()) {
                final String var = matcher.group();
                final String[] fieldAndArg = var.split(separator);
                final String input = String.join(separator, Arrays.copyOfRange(fieldAndArg, 0, fieldAndArg.length-1));
                inputs.add(input);
            } else {
                inputs.add(variable);
            }
        }
        return inputs;
    }

    public static Set<String> extractInputs(final List<Set<String>> variablesList, final String separator) {
        return variablesList.stream().flatMap(v -> extractInputs(v, separator).stream()).collect(Collectors.toSet());
    }

    public static Double eval(final Expression expression, final Set<String> variables, final MElement element) {
        final Map<String, Double> values = new HashMap<>();
        for(final String variable : variables) {
            final Double value = element.getAsDouble(variable);
            values.put(variable, Optional.ofNullable(value).orElse(Double.NaN));
        }
        double expResult = expression.setVariables(values).evaluate();
        return Double.isNaN(expResult) ? null : expResult;
    }

    public static Double getAsDouble(final Object value) {
        return getAsDouble(value, null);
    }

    public static Double getAsDouble(final Object value, final Double defaultValue) {
        if(value == null) {
            return defaultValue;
        }
        return switch (value) {
            case Double d -> d;
            case Number l -> l.doubleValue();
            case Instant i -> Long.valueOf(i.getMillis() * 1000L).doubleValue();
            case java.time.Instant i -> DateTimeUtil.toEpochMicroSecond(i).doubleValue();
            case LocalDate d -> Long.valueOf(d.toEpochDay()).doubleValue();
            case LocalTime t -> Long.valueOf(t.toNanoOfDay() / 1000L).doubleValue();
            case com.google.cloud.Date d -> DateTimeUtil.toEpochDay(d).doubleValue();
            case com.google.cloud.Timestamp t -> DateTimeUtil.toEpochMicroSecond(t).doubleValue();
            case com.google.protobuf.Timestamp t -> DateTimeUtil.toEpochMicroSecond(t).doubleValue();
            case String s -> Double.valueOf(s);
            case org.apache.avro.util.Utf8 s -> Double.valueOf(s.toString());
            default -> Double.NaN;
        };
    }

    public static class EqualOperator extends Operator {

        public EqualOperator() {
            super("=", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] == values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class NotEqualOperator extends Operator {

        public NotEqualOperator() {
            super("!=", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] != values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class GreaterOperator extends Operator {

        public GreaterOperator() {
            super(">", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] > values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class GreaterOrEqualOperator extends Operator {

        public GreaterOrEqualOperator() {
            super(">=", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] >= values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class LesserOperator extends Operator {

        public LesserOperator() {
            super("<", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] < values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class LesserOrEqualOperator extends Operator {

        public LesserOrEqualOperator() {
            super("<=", 2, true, Operator.PRECEDENCE_ADDITION - 1);
        }

        @Override
        public double apply(double... values) {
            if(values[0] <= values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class NotOperator extends Operator {

        public NotOperator() {
            super("!", 1, true, Operator.PRECEDENCE_ADDITION - 2);
        }

        @Override
        public double apply(double... values) {
            if(values[0] > 0) {
                return 0d;
            } else {
                return 1d;
            }
        }
    }

    public static class AndOperator extends Operator {

        public AndOperator() {
            super("&", 2, true, Operator.PRECEDENCE_ADDITION - 3);
        }

        @Override
        public double apply(double... values) {
            if(values[0] > 0 && values[1] > 0) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class OrOperator extends Operator {

        public OrOperator() {
            super("|", 2, true, Operator.PRECEDENCE_ADDITION - 4);
        }

        @Override
        public double apply(double... values) {
            if(values[0] > 0 || values[1] > 0) {
                return 1d;
            } else {
                return 0d;
            }
        }
    }

    public static class IfFunction extends Function {

        IfFunction() {
            super("if", 3);
        }

        @Override
        public double apply(double... args) {
            if(args[0] > 0) {
                return args[1];
            }
            return args[2];
        }

    }

    public static class SwitchFunction extends Function {

        private final int caseNum;

        SwitchFunction(int caseNum) {
            super(String.format("switch%d", caseNum), caseNum * 2);
            this.caseNum = caseNum;
        }

        @Override
        public double apply(double... args) {
            for(int i=0; i<caseNum; i+=2) {
                if(args[i] > 0) {
                    return args[i+1];
                }
            }
            return 0d;
        }

    }

    public static class MaxFunction extends Function {

        MaxFunction() {
            super("max", 2);
        }

        @Override
        public double apply(double... args) {
            return Math.max(args[0], args[1]);
        }

    }

    public static class MinFunction extends Function {

        MinFunction() {
            super("min", 2);
        }

        @Override
        public double apply(double... args) {
            return Math.min(args[0], args[1]);
        }

    }

    public static class TimestampToDateFunction extends Function {
        TimestampToDateFunction() {
            super("timestamp_to_date", 2);
        }

        @Override
        public double apply(double... args) {
            final Double epoch_micros = args[0];
            final Double timezone_micros = args[1] * 60 * 60 * 1000 * 1000;
            if(epoch_micros.isNaN() || timezone_micros.isNaN()) {
                return Double.NaN;
            }
            final long epoch_micros_with_tz = epoch_micros.longValue() + timezone_micros.longValue();
            final Instant instant = Instant.ofEpochMilli(epoch_micros_with_tz / 1000L);

            int year = instant.get(DateTimeFieldType.year());
            int month = instant.get(DateTimeFieldType.monthOfYear());
            int day = instant.get(DateTimeFieldType.dayOfMonth());
            final LocalDate date = LocalDate.of(year, month, day);

            return date.toEpochDay();
        }
    }

    public static class TimestampDiffFunction extends Function {

        private final String part;

        TimestampDiffFunction(final String part) {
            super("timestamp_diff_" + part, 2);
            this.part = part;
        }

        @Override
        public double apply(double... args) {
            final double diff_micros = args[0] - args[1];
            if(Double.isNaN(diff_micros)) {
                return Double.NaN;
            }
            return switch (part) {
                case "microsecond" -> diff_micros;
                case "millisecond" -> Double.valueOf(diff_micros / 1000L).longValue();
                case "second" -> Double.valueOf(diff_micros / 1000000L).longValue();
                case "minute" -> Double.valueOf(diff_micros / 60000000L).longValue();
                case "hour" -> Double.valueOf(diff_micros / 3600000000L).longValue();
                case "day" -> Double.valueOf(diff_micros / 86400000000L).longValue();
                default -> throw new IllegalArgumentException("Not supported part: " + part);
            };
        }

    }

}
