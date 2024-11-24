package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MFailure;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Filter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    public enum Type implements Serializable {
        AND,
        OR,
        TRUE,
        FALSE
    }

    public enum Op implements Serializable {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER(">"),
        GREATER_OR_EQUAL(">="),
        LESSER("<"),
        LESSER_OR_EQUAL("<="),
        IN("in"),
        NOT_IN("not in"),
        MATCH("match"),
        TRUE("true"),
        FALSE("false");

        private String name;

        Op(final String name) {
            this.name = name;
        }

        public static Op of(final String name) {
            for(final Op op : values()) {
                if(op.name.equalsIgnoreCase(name.trim())) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Filter.Op: " + name + " not found.");
        }
    }

    public static class ConditionNode implements Serializable {

        private Type type;
        private List<ConditionNode> nodes;
        private List<ConditionLeaf> leaves;

        private Set<String> variables;

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public List<ConditionNode> getNodes() {
            return nodes;
        }

        public void setNodes(List<ConditionNode> nodes) {
            this.nodes = nodes;
        }

        public List<ConditionLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(List<ConditionLeaf> leaves) {
            this.leaves = leaves;
        }

        public Set<String> getRequiredVariables() {
            final Set<String> variables = new HashSet<>();
            if(this.nodes != null && !this.nodes.isEmpty()) {
                for(final ConditionNode node : this.nodes) {
                    variables.addAll(node.getRequiredVariables());
                }
            }
            if(this.leaves != null && !this.leaves.isEmpty()) {
                for(final ConditionLeaf leaf : this.leaves) {
                    variables.addAll(leaf.getRequiredVariables());
                }
            }
            return variables;
        }

        @Override
        public String toString() {
            return String.format("{ Type: %s, Conditions: [ %s ], Children: [ %s ] }",
                    this.type,
                    Optional.ofNullable(this.leaves).orElse(new ArrayList<>())
                            .stream()
                            .map(ConditionLeaf::toString)
                            .collect(Collectors.joining(", ")),
                    Optional.ofNullable(this.nodes).orElse(new ArrayList<>())
                            .stream()
                            .map(ConditionNode::toString)
                            .collect(Collectors.joining(", "))
            );
        }

    }

    public static class ConditionLeaf implements Serializable {

        private String key;
        private Op op;
        private JsonElement value;

        private Expression expression;
        private Set<String> expressionVariables;
        private String expressionString;

        private Pattern pattern;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Op getOp() {
            return op;
        }

        public void setOp(Op op) {
            this.op = op;
        }

        public JsonElement getValue() {
            return value;
        }

        public void setValue(JsonElement value) {
            this.value = value;
        }



        @Override
        public String toString() {
            return String.format("%s %s %s", this.expression != null ? "(" + this.expressionString + ")" : this.key, this.op, this.value);
        }

        public Double evaluateExpression(final MElement input) {
            final Map<String, Double> variables = new HashMap<>();
            for(final String variableName : this.expression.getVariableNames()) {
                final Double fieldValue = input.getAsDouble(variableName);
                variables.put(variableName, fieldValue);
            }
            return expression.setVariables(variables).evaluate();
        }

        public Double evaluateExpression(final MElement input, final Map<String, Object> values) {
            final Map<String, Double> variables = new HashMap<>();
            for(final String variableName : this.expression.getVariableNames()) {
                final Object fieldValue;
                if(values == null) {
                    fieldValue = input.getAsDouble(variableName);
                } else {
                    fieldValue = Optional.ofNullable(values.get(variableName)).orElseGet(() -> input.getAsDouble(variableName));
                }
                variables.put(variableName, ExpressionUtil.getAsDouble(fieldValue));
            }
            return expression.setVariables(variables).evaluate();
        }

        public Set<String> getRequiredVariables() {
            final Set<String> variables = new HashSet<>();
            if(this.expression != null) {
                variables.addAll(this.expression.getVariableNames());
            } else if(this.key != null) {
                variables.add(this.key);
            }
            return variables;
        }

    }

    public static ConditionNode parse(final String filterJson) {
        return parse(new Gson().fromJson(filterJson, JsonElement.class));
    }

    public static ConditionNode parse(final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            final ConditionNode node = new ConditionNode();
            node.setType(Type.TRUE);
            return node;
        }

        if(jsonElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("Illegal condition json: " + jsonElement);
        }

        if(jsonElement.isJsonObject()) {
            return parse(jsonElement.getAsJsonObject());
        } else if(jsonElement.isJsonArray()) {
            final List<ConditionLeaf> leaves = new ArrayList<>();
            for(JsonElement child : jsonElement.getAsJsonArray()) {
                if(!child.isJsonObject()) {
                    throw new IllegalArgumentException("Simple conditions must be jsonObject. json: " + child);
                }
                final JsonObject childObject = child.getAsJsonObject();
                if(childObject.size() == 1 && (childObject.has("or") || childObject.has("and"))) {
                    throw new IllegalArgumentException("`or`, `and` conditions should be defined at the top level, not in an array. json: " + childObject);
                }
                final ConditionLeaf leaf = createLeaf(childObject);
                leaves.add(leaf);
            }
            ConditionNode node = new ConditionNode();
            node.setType(Type.AND);
            node.setLeaves(leaves);
            return node;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static ConditionNode parse(final JsonObject jsonObject) {
        if(!jsonObject.has("and") && !jsonObject.has("or")) {
            final List<ConditionLeaf> leaves = new ArrayList<>();
            final ConditionLeaf leaf = createLeaf(jsonObject);
            leaves.add(leaf);
            ConditionNode node = new ConditionNode();
            node.setType(Type.AND);
            node.setLeaves(leaves);
            return node;
        } else if(jsonObject.has("and") && jsonObject.has("or")) {
            throw new IllegalArgumentException("Condition must contain only one of `and` or `or`. Condition json: " + jsonObject.toString());
        }

        final Type type = jsonObject.has("and") ? Type.AND : Type.OR;
        final JsonElement conditions = jsonObject.has("and") ? jsonObject.get("and") : jsonObject.get("or");
        if(!conditions.isJsonArray()) {
            throw new IllegalArgumentException("Condition `and`, `or` parameter must be array. Condition json: " + conditions.toString());
        }
        final List<ConditionNode> nodes = new ArrayList<>();
        final List<ConditionLeaf> leaves = new ArrayList<>();
        for(final JsonElement condition : conditions.getAsJsonArray()) {
            final JsonObject child = condition.getAsJsonObject();
            if(child.has("and") || child.has("or")) {
                final ConditionNode node = parse(child);
                nodes.add(node);
            } else {
                final ConditionLeaf leaf = createLeaf(child);
                leaves.add(leaf);
            }
        }

        final ConditionNode node = new ConditionNode();
        node.setType(type);
        node.setNodes(nodes);
        node.setLeaves(leaves);
        node.variables = node.getRequiredVariables();
        return node;
    }

    private static ConditionLeaf createLeaf(final JsonObject jsonObject) {
        if((!jsonObject.has("key") && !jsonObject.has("expression")) || !jsonObject.has("op") || !jsonObject.has("value")) {
            throw new IllegalArgumentException("Simple conditions must contain `key`,`op`,`value`. json: " + jsonObject);
        }
        final ConditionLeaf leaf = new ConditionLeaf();
        leaf.setOp(Op.of(jsonObject.get("op").getAsString()));
        leaf.setValue(jsonObject.get("value"));

        if(jsonObject.has("expression")) {
            if(!jsonObject.get("expression").isJsonPrimitive() || !jsonObject.get("expression").getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("useExpression must be boolean, json: " + jsonObject);
            }
            final String expression = jsonObject.get("expression").getAsString();
            leaf.key = expression;
            leaf.pattern = null;
            leaf.expressionVariables = ExpressionUtil.estimateVariables(expression);
            leaf.expression = ExpressionUtil.createDefaultExpression(expression, leaf.expressionVariables);
            leaf.expressionString = expression;
        } else if(Op.MATCH.equals(leaf.op)) {
            leaf.key = jsonObject.get("key").getAsString();
            leaf.pattern = Pattern.compile(leaf.value.getAsString());
            leaf.expression = null;
            leaf.expressionString = null;
            leaf.expressionVariables = new HashSet<>();
        } else {
            leaf.key = jsonObject.get("key").getAsString();
            leaf.pattern = null;
            leaf.expression = null;
            leaf.expressionString = null;
            leaf.expressionVariables = new HashSet<>();
        }
        return leaf;
    }

    public static boolean filter(final ConditionNode condition, final Schema schema, final MElement element) {
        return filter(condition, element.asStandardMap(schema, condition.variables));
    }

    public static boolean filter(final ConditionNode condition, final Map<String, Object> standardValues) {
        final List<Boolean> bits = new ArrayList<>();

        if(condition.getLeaves() != null && !condition.getLeaves().isEmpty()) {
            for(ConditionLeaf leaf : condition.getLeaves()) {
                final Object value;
                if(leaf.expression != null) {
                    if(!standardValues.keySet().containsAll(leaf.expressionVariables)) {
                        throw new IllegalArgumentException("filter conditions expression variables[" + leaf.expressionVariables + "] are not included all in values keys: " + standardValues.keySet());
                    }
                    final Map<String, Double> variables = standardValues.entrySet()
                            .stream()
                            .filter(e -> leaf.expressionVariables.contains(e.getKey()))
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> ExpressionUtil.getAsDouble(e.getValue(), Double.NaN)));
                    try {
                        final double evaluatedValue = leaf.expression.setVariables(variables).evaluate();
                        if(Double.isNaN(evaluatedValue)) {
                            value = null;
                        } else {
                            value = evaluatedValue;
                        }
                    } catch (IllegalArgumentException e) {
                        return false;
                    }
                } else {
                    value = ElementSchemaUtil.getValue(standardValues, leaf.getKey());
                }
                bits.add(is(value, leaf));
            }
        }
        if(condition.getNodes() != null && !condition.getNodes().isEmpty()) {
            for(ConditionNode node : condition.getNodes()) {
                bits.add(filter(node, standardValues));
            }
        }

        if(bits.isEmpty()) {
            return false;
        }

        return is(condition.getType(), bits);
    }

    private static boolean is(final Type type, final Collection<Boolean> bits) {
        if(type.equals(Type.AND)) {
            return bits.stream().allMatch(v -> v);
        } else if(type.equals(Type.OR)) {
            return bits.stream().anyMatch(v -> v);
        } else {
            return type.equals(Type.TRUE);
        }
    }

    static boolean is(final Object value, final ConditionLeaf leaf) {
        if(value == null) {
            if(leaf.getValue() == null || leaf.getValue().isJsonNull()) {
                return leaf.getOp().equals(Op.EQUAL);
            }
            return false;
        } else if(leaf.getValue() == null || leaf.getValue().isJsonNull()) {
            return leaf.getOp().equals(Op.NOT_EQUAL);
        }

        if(leaf.getOp().equals(Op.IN) || leaf.getOp().equals(Op.NOT_IN)) {
            if(!leaf.getValue().isJsonArray()) {
                throw new IllegalArgumentException("Condition `in` or `not in` value must be array. json: " + leaf.getValue().toString());
            }
            for(final JsonElement e : leaf.getValue().getAsJsonArray()) {
                if(value.toString().equals(e.getAsString())) {
                    return leaf.getOp().equals(Op.IN);
                }
            }
            return leaf.getOp().equals(Op.NOT_IN);
        } else if(leaf.getOp().equals(Op.MATCH)) {
            return leaf.pattern.matcher(value.toString()).find();
        } else {
            final int c = switch (value) {
                case Byte b -> new BigDecimal(b.toString()).compareTo(leaf.getValue().getAsBigDecimal());
                case BigInteger b -> new BigDecimal(b).compareTo(leaf.getValue().getAsBigDecimal());
                case BigDecimal b -> b.compareTo(leaf.getValue().getAsBigDecimal());
                case Boolean b -> b.compareTo(leaf.getValue().getAsBoolean());
                case Short s -> s.compareTo(leaf.getValue().getAsShort());
                case Integer i -> i.compareTo(leaf.getValue().getAsInt());
                case Long l -> l.compareTo(leaf.getValue().getAsLong());
                case Float f when Float.isNaN(f) || Float.isInfinite(f) -> -2;
                case Float f-> f.compareTo(leaf.getValue().getAsFloat());
                case Double d when Double.isNaN(d) || Double.isInfinite(d) -> -2;
                case Double d -> d.compareTo(leaf.getValue().getAsDouble());
                case String s -> s.compareTo(leaf.getValue().getAsString());
                case java.time.Instant i -> i.compareTo(DateTimeUtil.toInstant(leaf.getValue().getAsString()));
                case Instant i -> i.compareTo(DateTimeUtil.toJodaInstant(leaf.getValue().getAsString()));
                case LocalDate l -> l.compareTo(DateTimeUtil.toLocalDate(leaf.getValue().getAsString()));
                case LocalTime l -> l.compareTo(DateTimeUtil.toLocalTime(leaf.getValue().getAsString()));
                case Utf8 u -> u.toString().compareTo(leaf.getValue().getAsString());
                default -> {
                    LOG.warn("not matched value: {} to leaf: {}", value, leaf.getValue().getAsString());
                    yield (value).toString().compareTo(leaf.getValue().getAsString());
                }
            };

            if(Math.abs(c) > 1) {
                return c > 0;
            }

            return switch (leaf.getOp()) {
                case EQUAL -> c == 0;
                case NOT_EQUAL -> c != 0;
                case GREATER -> c > 0;
                case GREATER_OR_EQUAL -> c >= 0;
                case LESSER -> c < 0;
                case LESSER_OR_EQUAL -> c <= 0;
                case TRUE -> true;
                case FALSE -> false;
                default -> throw new IllegalArgumentException("");
            };
        }
    }

    // PTransform
    public static Transform of(
            final String jobName,
            final String name,
            final String filterJson,
            final Schema inputSchema,
            final boolean failFast) {

        return new Transform(jobName, name, filterJson, inputSchema, failFast);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final JsonElement filterJson,
            final Schema inputSchema,
            final boolean failFast) {

        final String filterText = Optional.ofNullable(filterJson).map(JsonElement::toString).orElse(null);
        return new Transform(jobName, name, filterText, inputSchema, failFast);
    }


    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        final String jobName;
        final String name;
        final String filterJson;
        final Schema inputSchema;
        final boolean failFast;

        public final TupleTag<MElement> outputTag;
        public final TupleTag<MElement> failuresTag;

        Transform(
                final String jobName,
                final String name,
                final String filterJson,
                final Schema inputSchema,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.filterJson = filterJson;
            this.inputSchema = inputSchema;
            this.failFast = failFast;
            this.outputTag = new TupleTag<>() {};
            this.failuresTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PCollection<MElement> input) {
            final PCollectionTuple outputs = input
                    .apply("Filter", ParDo
                            .of(new FilterDoFn(jobName, name, filterJson, inputSchema, failuresTag, failFast))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

            return PCollectionTuple
                    .of(outputTag, outputs.get(outputTag)
                            .setCoder(input.getCoder()))
                    .and(failuresTag, outputs.get(failuresTag)
                            .apply("WithDefaultWindow", Strategy.createDefaultWindow())
                            .setCoder(ElementCoder.of(MFailure.schema())));
        }

        private static class FilterDoFn extends DoFn<MElement, MElement> {

            private final String jobName;
            private final String name;
            private final String conditionJsons;
            private final Schema inputSchema;
            private final TupleTag<MElement> failureTag;
            private final boolean failFast;

            private final Counter errorCounter;

            private transient Filter.ConditionNode conditions;

            private FilterDoFn(
                    final String jobName,
                    final String name,
                    final String conditionJsons,
                    final Schema inputSchema,
                    final TupleTag<MElement> failureTag,
                    final boolean failFast) {

                this.jobName = jobName;
                this.name = name;
                this.conditionJsons = conditionJsons;
                this.inputSchema = inputSchema;
                this.failFast = failFast;
                this.failureTag = failureTag;

                this.errorCounter = Metrics.counter(name, "filter_error");
            }

            @Setup
            public void setup() {
                this.conditions = Filter.parse(conditionJsons);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement element = c.element();
                if(element == null) {
                    return;
                }

                try {
                    if (Filter.filter(conditions, inputSchema, element)) {
                        c.output(element);
                    } else {
                        LOG.info("not matched element: " + element);
                    }
                } catch (final Throwable e) {
                    errorCounter.inc();
                    final MFailure failure = MFailure
                            .of(jobName, name, element.toString(), e, c.timestamp());
                    final String errorMessage = String.format("Failed to filter for input: %s, for condition: %s, error: %s", failure.getInput(), conditionJsons, failure.getError());
                    LOG.error(errorMessage);
                    if(failFast) {
                        throw new RuntimeException("Failed to filter for input: " + element + ", for condition: " + conditionJsons, e);
                    }
                    c.output(failureTag, failure.toElement(c.timestamp()));
                }
            }
        }

    }

}
