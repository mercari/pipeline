package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.select.navigation.NavigationFunction;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import com.mercari.solution.util.schema.AvroSchemaUtil;
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

    private final List<SelectFunction> selectFunctions;

    public List<SelectFunction> getSelectFunctions() {
        return selectFunctions;
    }

    public Select(final List<SelectFunction> selectFunctions) {
        this.selectFunctions = Optional
                .ofNullable(selectFunctions)
                .orElseGet(ArrayList::new);
    }

    public static Select of(final List<SelectFunction> selectFunctions) {
        return new Select(selectFunctions);
    }

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

    public void setup() {
        selectFunctions.forEach(SelectFunction::setup);
    }

    // Stateless select processing
    public Map<String, Object> select(
            final MElement element,
            final Instant timestamp) {

        if(selectFunctions.isEmpty()) {
            return element.asPrimitiveMap();
        }
        return SelectFunction.apply(selectFunctions, element, timestamp);
    }

    // Stateless select processing
    public Map<String, Object> select(
            final Map<String, Object> primitiveValues,
            final Instant timestamp) {

        if(selectFunctions.isEmpty()) {
            return primitiveValues;
        }
        return SelectFunction.apply(selectFunctions, primitiveValues, timestamp);
    }

    // Stateful select processing
    public Map<String, Object> select(
            final MElement input,
            final Collection<TimestampedValue<MElement>> buffer,
            final Instant timestamp) {

        // process state buffer
        int index = buffer.size();
        Accumulator accumulator = Accumulator.of();
        for(final TimestampedValue<MElement> bufferTimestampedValue : buffer) {
            final Instant bufferTimestamp = bufferTimestampedValue.getTimestamp();
            final MElement bufferValue = bufferTimestampedValue.getValue();
            for(final SelectFunction selectFunction : selectFunctions) {
                if(selectFunction.ignore()) {
                    continue;
                }
                if(selectFunction instanceof StatefulFunction statefulFunction) {
                    if(!statefulFunction.getRange().filter(timestamp, bufferTimestamp, index)) {
                        continue;
                    }
                    accumulator = statefulFunction.addInput(accumulator, bufferValue, index, timestamp);
                }
            }
            index = index - 1;
        }

        // process input
        final Map<String, Object> outputs = new HashMap<>();
        if(selectFunctions.isEmpty()) {
            return outputs;
        }
        outputs.putAll(input.asPrimitiveMap());

        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Object output = switch (selectFunction) {
                case StatefulFunction statefulFunction -> {
                    if(statefulFunction.getRange().filter(timestamp, input.getTimestamp(), index)) {
                        accumulator = statefulFunction.addInput(accumulator, input, 0, timestamp);
                    }
                    yield statefulFunction.extractOutput(accumulator, outputs);
                }
                case NavigationFunction navigationFunction -> null; //TODO
                default -> selectFunction.apply(outputs, timestamp);
            };
            outputs.put(selectFunction.getName(), output);
        }
        return outputs;
    }

    public boolean useSelect() {
        return !selectFunctions.isEmpty();
    }

    public static List<String> getInputFieldNames(final List<SelectFunction> selectFunctions) {
        final List<String> fieldNameList = new ArrayList<>();
        final Set<String> fieldNameDuplicateCheckSet = new HashSet<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            for(final Schema.Field selectInputField : selectFunction.getInputFields()) {
                if(!fieldNameDuplicateCheckSet.contains(selectInputField.getName())) {
                    fieldNameList.add(selectInputField.getName());
                    fieldNameDuplicateCheckSet.add(selectInputField.getName());
                }
            }
        }
        return fieldNameList;
    }

    public static org.apache.avro.Schema createStateAvroSchema(
            final Schema inputSchema,
            final List<SelectFunction> selectFunctions) {

        final Schema outputSchema = SelectFunction.createSchema(selectFunctions);
        final Schema unionSchema = Union.createUnionSchema(List.of(outputSchema, inputSchema));
        final org.apache.avro.Schema unionAvroSchema = unionSchema.getAvroSchema();
        final List<String> fieldNameList = getInputFieldNames(selectFunctions);
        return AvroSchemaUtil.selectFields(unionAvroSchema, fieldNameList);
    }

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
