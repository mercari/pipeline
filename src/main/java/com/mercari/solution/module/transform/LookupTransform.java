package com.mercari.solution.module.transform;

import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Transform.Module(name="lookup")
public class LookupTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(LookupTransform.class);

    private static class Parameters implements Serializable {

        private List<LookupParameter> lookups;
        private JsonElement select;
        private String flattenField;

        private void validate(List<MCollection> sideInputs) {
            final List<String> errorMessages = new ArrayList<>();
            if(sideInputs.isEmpty()) {
                errorMessages.add("`sideInputs` must not be empty");
            }
            if(this.lookups == null || lookups.isEmpty()) {
                errorMessages.add("`lookups` parameters is required");
            } else {
                for(int index=0; index<lookups.size(); index++) {
                    final LookupParameter lookup = lookups.get(index);
                    errorMessages.addAll(lookup.validate(index, sideInputs));
                }
            }
            if(select != null && !select.isJsonArray()) {
                errorMessages.add("`select` parameters must be array");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            for(final LookupParameter lookup : lookups) {
                lookup.setDefaults();
            }
        }

        private static class LookupParameter implements Serializable {

            private String name;
            private Views.Type type;
            private String sideInput;
            private String keyField;
            private Boolean flatten;

            private List<String> validate(int index, List<MCollection> sideInputs) {
                final List<String> errorMessages = new ArrayList<>();
                if(sideInput == null) {
                    errorMessages.add("`lookups`[" + index + "].`sideInput` parameter is required");
                } else if(!sideInputs.stream().anyMatch(s -> sideInput.equals(s.getName()))) {
                    errorMessages.add("`lookups`[" + index + "].`sideInput`=" + sideInput + " is not found in sideInputs: " + sideInputs.stream().map(MCollection::getName).toList());
                }
                if(keyField == null) {
                    errorMessages.add("`lookups`[" + index + "].`keyField` parameter is required");
                }
                if(name == null && flatten != null && !flatten) {
                    errorMessages.add("`lookups`[" + index + "].`name` parameter is required if `flatten` parameter is false");
                }
                return errorMessages;
            }

            private void setDefaults() {
                if(type == null) {
                    type = Views.Type.singleton;
                }
                if(flatten == null) {
                    flatten = false;
                }
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getSideInputs());
        if(getSideInputs().isEmpty()) {
            throw new IllegalModuleException("`sideInputs` must not be empty");
        }
        parameters.setDefaults();

        final Map<String, PCollectionView<?>> views = new HashMap<>();
        final Map<String, Schema> sideInputSchemas = new HashMap<>();
        for(final Parameters.LookupParameter lookup : parameters.lookups) {
            final MCollection collection = getSideInputs().stream()
                    .filter(c -> lookup.sideInput.equals(c.getName()))
                    .findAny()
                    .orElseThrow();
            final PCollectionView<Map<String,Object>> sideInputView = collection
                    .apply("AsView_" + collection.getName(), Views.of(lookup.type));
            views.put(collection.getName(), sideInputView);
            sideInputSchemas.put(collection.getName(), collection.getSchema());
        }

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputLookupSchema = createOutputSchema(parameters, inputSchema, sideInputSchemas);

        final Schema outputSchema;
        final List<SelectFunction> selectFunctions;
        if(parameters.select == null || !parameters.select.isJsonArray()) {
            selectFunctions = new ArrayList<>();
            outputSchema = outputLookupSchema;
        } else {
            selectFunctions = SelectFunction.of(parameters.select.getAsJsonArray(), outputLookupSchema.getFields());
            outputSchema = SelectFunction.createSchema(selectFunctions, parameters.flattenField);
        }

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<MElement> failureTag = new TupleTag<>(){};

        PCollection<MElement> input;

        if(OptionUtil.isStreaming(inputs)) {
            /*
            final WindowUtil.WindowParameters window = new WindowUtil.WindowParameters();
            window.setType(WindowUtil.WindowType.fixed);
            window.setSize(1L);
            window.setOffset(0L);
            window.setUnit(DateTimeUtil.TimeUnit.second);
            window.setAllowedLateness(0L);
            window.setTimestampCombiner(TimestampCombiner.LATEST);

            final TriggerUtil.TriggerParameters earlyTrigger = new TriggerUtil.TriggerParameters();
            earlyTrigger.setType(TriggerUtil.TriggerType.afterProcessingTime);
            final TriggerUtil.TriggerParameters trigger = new TriggerUtil.TriggerParameters();
            trigger.setType(TriggerUtil.TriggerType.afterWatermark);
            //trigger.setEarlyFiringTrigger(earlyTrigger);

            input = inputs
                    .apply("Union", Union.flatten()
                            .withWindow(window)
                            .withTrigger(trigger)
                            .withAccumulationMode(WindowUtil.AccumulationMode.discarding)
                    );

             */

            input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
        } else {
            input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
        }

        /*
        final PCollectionTuple withLookup = input
                //.apply("WithKey", WithKeys.of(""))
                .apply("WithKey", WithKeys.of((m) -> UUID.randomUUID().toString()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()))
                .apply("GroupByKey", GroupByKey.create())
                .apply("Lookup", ParDo
                        .of(new LookupDoFn2(jobName, getName(), outputLookupSchema, parameters.lookups, views, failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag))
                        .withSideInputs(views));


         */
        final PCollectionTuple withLookup = input
                .apply("Lookup", ParDo
                        .of(new LookupDoFn(getJobName(), getName(), outputLookupSchema, parameters.lookups, views, failureTag, getFailFast()))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag))
                        .withSideInputs(views));


        final PCollection<MElement> lookup = withLookup.get(outputTag);
        /*
        final PCollection<MElement> lookupFailures = withLookup.get(failureTag)
                .apply("WithDefaultWindow", Strategy.createDefaultWindow())
                .setCoder(ElementCoder.of(MFailure.schema()));
         */

        final PCollection<MElement> output;
        //PCollectionList<MElement> failuresList = PCollectionList.of(lookupFailures);
        if(selectFunctions.isEmpty()) {
            output = lookup;
        } else {
            final Select.Transform selectTransform = Select.of(
                    getJobName(), getName(), selectFunctions, parameters.flattenField, getLoggings(), getFailFast());
            final PCollectionTuple selected = lookup
                    .apply("Select", selectTransform);
            output = selected.get(selectTransform.outputTag);
            //failuresList = failuresList.and(selected.get(selectTransform.failuresTag));
        }

        return MCollectionTuple
                .of(output, outputSchema);
    }

    private static Schema createOutputSchema(
            final Parameters parameters,
            final Schema inputSchema,
            final Map<String, Schema> sideInputSchemas) {

        final Schema.Builder builder = Schema.builder(inputSchema);
        for(final Parameters.LookupParameter lookup : parameters.lookups) {
            final Schema sideInputSchema = sideInputSchemas.get(lookup.sideInput);
            if(lookup.flatten) {
                for(final Schema.Field field : sideInputSchema.getFields()) {
                    if(field.getName().equals(lookup.keyField)) {
                        continue;
                    }
                    builder.withField(field.getName(), field.getFieldType());
                }
            } else {
                builder.withField(lookup.name, Schema.FieldType.element(sideInputSchema).withNullable(true));
            }
        }
        return builder.build();
    }

    private static class LookupDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final Schema schema;
        private final List<Parameters.LookupParameter> lookups;
        private final Map<String, PCollectionView<?>> views;
        private final TupleTag<MElement> failureTag;
        private final boolean failFast;

        private final Counter errorCounter;

        LookupDoFn(
                final String jobName,
                final String name,
                final Schema schema,
                final List<Parameters.LookupParameter> lookups,
                final Map<String, PCollectionView<?>> views,
                final TupleTag<MElement> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.schema = schema;
            this.lookups = lookups;
            this.views = views;
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.errorCounter = Metrics.counter(name, "lookup_error");
        }

        @Setup
        public void setup() {
            schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                LOG.warn("lookup: {} input is null", name);
                return;
            }
            try {
                final Map<String, Object> values = element.asPrimitiveMap();
                for(final Parameters.LookupParameter lookup : lookups) {

                    final PCollectionView<Map<String, Object>> view = (PCollectionView<Map<String, Object>>)views.get(lookup.sideInput);
                    final Map<String, Object> sideInputValues = c.sideInput(view);
                    if(sideInputValues == null) {
                        LOG.error("lookup: {} side input is null", name);
                        errorCounter.inc();
                        continue;
                    }

                    final Object keyValue = element.getPrimitiveValue(lookup.keyField);
                    if(keyValue == null) {
                        LOG.warn("lookup: {} key value is null", name);
                        continue;
                    }

                    final Map<String, Object> sideInputValue = (Map<String, Object>)sideInputValues.get(keyValue.toString());
                    if(sideInputValue == null) {
                        LOG.warn("lookup: {} key lookup value is null", name);
                        continue;
                    }

                    if(lookup.flatten) {
                        values.putAll(sideInputValue);
                    } else {
                        values.put(lookup.name, sideInputValue);
                    }
                }

                final MElement output = MElement.of(schema, values, c.timestamp());
                c.output(output);
            } catch (Throwable e) {
                errorCounter.inc();
                final MFailure failure = MFailure.of(jobName, name, element.toString(), e, c.timestamp());
                c.output(failureTag, failure.toElement(c.timestamp()));
                LOG.error("Failed to lookup for input: {}, cause: {}", failure.getInput(), failure.getError());
                if(failFast) {
                    throw new RuntimeException("Failed to lookup for input: " + element, e);
                }
            }
        }

    }

    private static class LookupDoFn2 extends DoFn<KV<String, Iterable<MElement>>, MElement> {

        private final String jobName;
        private final String name;
        private final Schema schema;
        private final List<Parameters.LookupParameter> lookups;
        private final Map<String, PCollectionView<?>> views;
        private final TupleTag<MElement> failureTag;

        private final Counter errorCounter;

        LookupDoFn2(
                final String jobName,
                final String name,
                final Schema schema,
                final List<Parameters.LookupParameter> lookups,
                final Map<String, PCollectionView<?>> views,
                final TupleTag<MElement> failureTag) {

            this.jobName = jobName;
            this.name = name;
            this.schema = schema;
            this.lookups = lookups;
            this.views = views;
            this.failureTag = failureTag;
            this.errorCounter = Metrics.counter(name, "lookup_error");
        }

        @Setup
        public void setup() {
            schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if(kv == null) {
                LOG.warn("warn null");
                return;
            }
            final Iterable<MElement> elements = kv.getValue();
            if(elements == null) {
                LOG.warn("warn null2");
                return;
            }
            try {
                for(final MElement element : elements) {
                    final Map<String, Object> values = element.asPrimitiveMap();
                    for (final Parameters.LookupParameter lookup : lookups) {

                        final PCollectionView<Map<String, Object>> view = (PCollectionView<Map<String, Object>>) views.get(lookup.sideInput);
                        final Map<String, Object> sideInputValues = c.sideInput(view);
                        if (sideInputValues == null) {
                            LOG.warn("skip sideinput");
                            continue;
                        }

                        final Object keyValue = element.getPrimitiveValue(lookup.keyField);
                        if (keyValue == null) {
                            LOG.warn("skip sideinput 2");
                            continue;
                        }

                        final Map<String, Object> sideInputValue = (Map<String, Object>) sideInputValues.get(keyValue.toString());
                        if (sideInputValue == null) {
                            LOG.warn("skip sideinput 3");
                            continue;
                        }

                        if (lookup.flatten) {
                            values.putAll(sideInputValue);
                        } else {
                            values.put(lookup.name, sideInputValue);
                        }
                    }

                    final MElement output = MElement.of(schema, values, c.timestamp());
                    c.output(output);
                }
            } catch (Throwable e) {
                errorCounter.inc();
                final MFailure failure = MFailure.of(jobName, name, elements.toString(), e, c.timestamp());
                c.output(failureTag, failure.toElement(c.timestamp()));
                LOG.error("Failed to lookup for input: {}, cause: {}", failure.getInput(), failure.getError());
            }
        }

    }

}
