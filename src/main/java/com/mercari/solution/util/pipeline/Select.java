package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class Select implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Select.class);

    public static Map<String, Object> apply(
            List<SelectFunction> selectFunctions,
            Map<String, Object> primitiveValues,
            Instant timestamp) {

        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Object primitiveValue = selectFunction.apply(primitiveValues, timestamp);
            primitiveValues.put(selectFunction.getName(), primitiveValue);
        }
        return primitiveValues;
    }

    public static Select of(
            final Schema outputSchema,
            final JsonElement filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final DataType outputType) {

        return new Select(outputSchema, filterJson, selectFunctions, flattenField, outputType);
    }

    protected final Schema outputSchema;
    private final String filter;
    private final List<SelectFunction> selectFunctions;
    private final String flattenField;
    protected final DataType outputType;

    private transient Filter.ConditionNode conditionNode;

    public Select(
            final Schema outputSchema,
            final JsonElement filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final DataType outputType) {

        this(outputSchema, Optional.ofNullable(filterJson).map(JsonElement::toString).orElse(null), selectFunctions, flattenField, outputType);
    }

    public Select(
            final Schema outputSchema,
            final String filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final DataType outputType) {

        this.outputSchema = outputSchema;
        this.filter = filterJson;
        this.selectFunctions = Optional.ofNullable(selectFunctions).orElseGet(ArrayList::new);
        this.flattenField = flattenField;
        this.outputType = outputType;
    }

    public void setup() {
        if(filter != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(filter, JsonElement.class));
        }
        selectFunctions.forEach(SelectFunction::setup);
    }

    public boolean filter(final MElement element) {
        return filter(element.asPrimitiveMap());
    }

    public boolean filter(final Map<String, Object> primitiveValues) {
        return Filter.filter(conditionNode, primitiveValues);
    }

    public Map<String, Object> select(final MElement element, final Instant timestamp) {
        if(selectFunctions.isEmpty()) {
            return element.asPrimitiveMap();
        }
        return SelectFunction.apply(selectFunctions, element, timestamp);
    }

    public Map<String, Object> select(final Map<String, Object> primitiveValues, final Instant timestamp) {
        if(selectFunctions.isEmpty()) {
            return primitiveValues;
        }
        return SelectFunction.apply(selectFunctions, primitiveValues, timestamp);
    }

    public List<MElement> flatten(final Map<String, Object> primitiveValues, final Instant timestamp) {
        final List<MElement> outputs = new ArrayList<>();
        if (flattenField == null) {
            final MElement output = MElement
                    .of(outputSchema, primitiveValues, timestamp)
                    .convert(outputSchema, outputType);
            outputs.add(output);
            return outputs;
        }

        final List<?> flattenList = Optional
                .ofNullable((List<?>) primitiveValues.get(flattenField))
                .orElseGet(ArrayList::new);
        if (flattenList.isEmpty()) {
            final Map<String, Object> flattenValues = new HashMap<>(primitiveValues);
            flattenValues.put(flattenField, null);
            final MElement output = MElement
                    .of(outputSchema, flattenValues, timestamp)
                    .convert(outputSchema, outputType);
            outputs.add(output);
        } else {
            for (final Object value : flattenList) {
                final Map<String, Object> flattenValues = new HashMap<>(primitiveValues);
                flattenValues.put(flattenField, value);
                final MElement output = MElement
                        .of(outputSchema, flattenValues, timestamp)
                        .convert(outputSchema, outputType);
                outputs.add(output);
            }
        }
        return outputs;
    }

    public MElement convert(final MElement element) {
        if(element == null) {
            return null;
        }
        return element.convert(outputSchema, outputType);
    }

    public boolean useSelect() {
        return !selectFunctions.isEmpty();
    }

    public boolean useFilter() {
        return filter != null;
    }

    public boolean useFlatten() {
        return flattenField != null;
    }

    /*
    public Schema createSchema(
            final List<Schema.Field> inputFields,
            final JsonArray select,
            final String flattenField,
            final DataType outputType) {

        final Schema outputSchema;
        if(select != null && select.isJsonArray()) {
            final List<SelectFunction> selectFunctions = SelectFunction.of(select, inputFields);
            outputSchema = SelectFunction.createSchema(selectFunctions, flattenField);
        } else {
            outputSchema = Schema.of(inputFields);
        }

        final DataType type = Optional
                .ofNullable(outputType)
                .orElse(DataType.ELEMENT);

        return outputSchema.withType(type);
    }
     */

    /*
    public Map<String, Object> sql(final List<MElement> elements) {
        final MemorySchema memorySchema = MemorySchema.create("schema", List.of(
            MemorySchema.createTable("INPUT", inputSchema, elements)
        ));
        //this.planner = Query.createPlanner(memorySchema);
        try {
            //this.statement = Query.createStatement(planner, sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try(final ResultSet resultSet = statement.executeQuery()) {
            final List<MElement> outputs = new ArrayList<>();
            final List<Map<String, Object>> results = CalciteSchemaUtil.convert(resultSet);
            for(final Map<String, Object> result : results) {
                final MElement output = MElement.of(result, timestamp);
                outputs.add(output);
            }
            return outputs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
      */

    public static Transform of(
            final String jobName,
            final String name,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final List<Logging> loggings,
            final boolean failFast) {

        return new Transform(jobName, name, null, selectFunctions, flattenField, loggings, failFast, DataType.ELEMENT);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final List<Logging> loggings,
            final boolean failFast,
            final DataType outputType) {

        return new Transform(jobName, name, null, selectFunctions, flattenField, loggings, failFast, outputType);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final String filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final List<Logging> loggings,
            final boolean failFast,
            final DataType outputType) {

        return new Transform(jobName, name, filterJson, selectFunctions, flattenField, loggings, failFast, outputType);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final JsonElement filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final List<Logging> loggings,
            final boolean failFast,
            final DataType outputType) {

        return new Transform(jobName, name, Optional.ofNullable(filterJson).map(JsonElement::toString).orElse(null), selectFunctions, flattenField, loggings, failFast, outputType);
    }

    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        final String jobName;
        final String name;
        final String filterJson;
        final List<SelectFunction> selectFunctions;
        final String flattenField;
        final boolean failFast;
        final Map<String, Logging> loggings;
        final DataType outputType;

        public final Schema outputSchema;
        public final TupleTag<MElement> outputTag;
        public final TupleTag<MElement> failuresTag;

        Transform(
                final String jobName,
                final String name,
                final String filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final List<Logging> loggings,
                final boolean failFast,
                final DataType outputType) {

            this.jobName = jobName;
            this.name = name;
            this.filterJson = filterJson;
            this.selectFunctions = selectFunctions;
            this.flattenField = flattenField;
            this.failFast = failFast;
            this.loggings = Logging.map(loggings);
            this.outputType = outputType;

            this.outputSchema = SelectFunction
                    .createSchema(selectFunctions, flattenField)
                    .withType(outputType);

            this.outputTag = new TupleTag<>() {};
            this.failuresTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PCollection<MElement> input) {
            final Schema outputSchema = SelectFunction.createSchema(selectFunctions, flattenField);
            final PCollection<MElement> output = input
                    .apply("Select", ParDo.of(new SelectFilterDoFn(
                            jobName, name, outputSchema, filterJson, selectFunctions, flattenField,
                            loggings, failFast, failuresTag, outputType)));
            return PCollectionTuple
                    .of(outputTag, output.setCoder(ElementCoder.of(outputSchema)));
        }

        private static class SelectFilterDoFn extends DoFn<MElement, MElement> {

            private final String jobName;
            private final String name;
            private final Schema schema;
            private final String filter;
            private final List<SelectFunction> selectFunctions;
            private final String flattenField;

            private final Map<String, Logging> loggings;
            private final boolean failFast;
            private final DataType outputType;

            private final TupleTag<MElement> failuresTag;
            private final Counter errorCounter;

            private transient Filter.ConditionNode conditionNode;

            private SelectFilterDoFn(
                    final String jobName,
                    final String name,
                    final Schema schema,
                    final String filterJson,
                    final List<SelectFunction> selectFunctions,
                    final String flattenField,
                    final Map<String, Logging> loggings,
                    final boolean failFast,
                    final TupleTag<MElement> failuresTag,
                    final DataType outputType) {

                this.jobName = jobName;
                this.name = name;
                this.schema = schema;
                this.filter = filterJson;
                this.selectFunctions = selectFunctions;
                this.flattenField = flattenField;

                this.failFast = failFast;
                this.failuresTag = failuresTag;
                this.loggings = loggings;
                this.outputType = outputType;

                this.errorCounter = Metrics.counter(name, "select_error");
            }

            @Setup
            public void setup() {
                //schema.setup();
                if(filter != null) {
                    this.conditionNode = Filter.parse(new Gson().fromJson(filter, JsonElement.class));
                }
                for(final SelectFunction selectFunction: selectFunctions) {
                    selectFunction.setup();
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                if(input == null) {
                    return;
                }

                try {
                    Logging.log(LOG, loggings, "input", input);
                    // filter
                    if(conditionNode != null) {
                        final Map<String, Object> values = input.asPrimitiveMap();
                        if(!Filter.filter(conditionNode, values)) {
                            Logging.log(LOG, loggings, "not_matched", input);
                            return;
                        } else {
                            Logging.log(LOG, loggings, "matched", input);
                        }
                    }

                    // select
                    final List<MElement> outputs = new ArrayList<>();
                    final Map<String, Object> values = SelectFunction.apply(selectFunctions, input, c.timestamp());
                    if(flattenField == null) {
                        final MElement output = MElement.of(schema, values, c.timestamp());
                        outputs.add(output);
                    } else {
                        final List<?> flattenList = Optional.ofNullable((List<?>) values.get(flattenField)).orElseGet(ArrayList::new);
                        if(flattenList.isEmpty()) {
                            final Map<String, Object> flattenValues = new HashMap<>(values);
                            flattenValues.put(flattenField, null);
                            final MElement output = MElement.of(schema, flattenValues, c.timestamp());
                            outputs.add(output);
                        } else {
                            for(final Object value : flattenList) {
                                final Map<String, Object> flattenValues = new HashMap<>(values);
                                flattenValues.put(flattenField, value);
                                final MElement output = MElement.of(schema, flattenValues, c.timestamp());
                                outputs.add(output);
                            }
                        }
                    }

                    for(final MElement output : outputs) {
                        final MElement output_ = output.convert(schema, outputType);
                        c.output(output_);
                        Logging.log(LOG, loggings, "output", output_);
                    }

                } catch (final Throwable e) {
                    errorCounter.inc();
                    final MFailure failure = MFailure
                            .of(jobName, name, input.toString(), e, c.timestamp());
                    final String errorMessage = String.format("Failed to execute select functions for input: %s, error: %s", failure.getInput(), failure.getError());
                    LOG.error(e.toString() + " : " + errorMessage);
                    if(failFast) {
                        throw new RuntimeException(errorMessage, e);
                    }
                    c.output(failuresTag, failure.toElement(c.timestamp()));
                }

            }
        }
    }

}
