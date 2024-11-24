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

import java.util.*;

public class Select {

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

    public static Transform of(
            final String jobName,
            final String name,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final boolean outputFailure,
            final boolean failFast) {

        return new Transform(jobName, name, null, selectFunctions, flattenField, failFast, outputFailure, DataType.ELEMENT);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final boolean failFast,
            final boolean outputFailure,
            final DataType outputType) {

        return new Transform(jobName, name, null, selectFunctions, flattenField, failFast, outputFailure, outputType);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final String filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final boolean failFast,
            final boolean outputFailure,
            final DataType outputType) {

        return new Transform(jobName, name, filterJson, selectFunctions, flattenField, failFast, outputFailure, outputType);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final JsonElement filterJson,
            final List<SelectFunction> selectFunctions,
            final String flattenField,
            final boolean failFast,
            final boolean outputFailure,
            final DataType outputType) {

        return new Transform(jobName, name, Optional.ofNullable(filterJson).map(JsonElement::toString).orElse(null), selectFunctions, flattenField, failFast, outputFailure, outputType);
    }

    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        final String jobName;
        final String name;
        final String filterJson;
        final List<SelectFunction> selectFunctions;
        final String flattenField;
        final boolean failFast;
        final boolean outputFailure;
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
                final boolean failFast,
                final boolean outputFailure,
                final DataType outputType) {

            this.jobName = jobName;
            this.name = name;
            this.filterJson = filterJson;
            this.selectFunctions = selectFunctions;
            this.flattenField = flattenField;
            this.failFast = failFast;
            this.outputFailure = outputFailure;
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
            if(outputFailure) {
                final PCollectionTuple outputs = input
                        .apply("Select", ParDo.of(new SelectDoFn(
                                        jobName, name, outputSchema, filterJson, selectFunctions, flattenField, failFast, outputFailure, failuresTag, outputType))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
                return PCollectionTuple
                        .of(outputTag, outputs.get(outputTag)
                                .setCoder(ElementCoder.of(outputSchema)))
                        .and(failuresTag, outputs.get(failuresTag)
                                .apply("WithDefaultWindow", Strategy.createDefaultWindow())
                                .setCoder(ElementCoder.of(MFailure.schema())));
            } else {
                final PCollection<MElement> output = input
                        .apply("Select", ParDo.of(new SelectDoFn(
                                jobName, name, outputSchema, filterJson, selectFunctions, flattenField,
                                failFast, outputFailure, failuresTag, outputType)));
                return PCollectionTuple
                        .of(outputTag, output.setCoder(ElementCoder.of(outputSchema)));
            }
        }

        private static class SelectDoFn extends DoFn<MElement, MElement> {

            private final String jobName;
            private final String name;
            private final Schema schema;
            private final String filter;
            private final List<SelectFunction> selectFunctions;
            private final String flattenField;

            private final boolean failFast;
            private final boolean outputFailure;
            private final DataType outputType;

            private final TupleTag<MElement> failuresTag;
            private final Counter errorCounter;

            private transient Filter.ConditionNode conditionNode;

            private SelectDoFn(
                    final String jobName,
                    final String name,
                    final Schema schema,
                    final String filterJson,
                    final List<SelectFunction> selectFunctions,
                    final String flattenField,
                    final boolean failFast,
                    final boolean outputFailure,
                    final TupleTag<MElement> failuresTag,
                    final DataType outputType) {

                this.jobName = jobName;
                this.name = name;
                this.schema = schema;
                this.filter = filterJson;
                this.selectFunctions = selectFunctions;
                this.flattenField = flattenField;

                this.failFast = failFast;
                this.outputFailure = outputFailure;
                this.failuresTag = failuresTag;
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
                final MElement element = c.element();
                if(element == null) {
                    return;
                }

                try {
                    // filter
                    if(conditionNode != null) {
                        final Map<String, Object> values = element.asPrimitiveMap();
                        if(!Filter.filter(conditionNode, values)) {
                            LOG.info("Not matched condition. module: {}, input: {}", name, element);
                            return;
                        }
                    }

                    // select
                    final List<MElement> outputs = new ArrayList<>();
                    final Map<String, Object> values = SelectFunction.apply(selectFunctions, element, c.timestamp());
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
                    }

                } catch (final Throwable e) {
                    errorCounter.inc();
                    final MFailure failure = MFailure
                            .of(jobName, name, element.toString(), e, c.timestamp());
                    final String errorMessage = String.format("Failed to execute select functions for input: %s, error: %s", failure.getInput(), failure.getError());
                    LOG.error(e.toString() + " : " + errorMessage);
                    if(failFast) {
                        throw new RuntimeException(errorMessage, e);
                    }
                    if(outputFailure) {
                        c.output(failuresTag, failure.toElement(c.timestamp()));
                    }
                }

            }
        }
    }

}
