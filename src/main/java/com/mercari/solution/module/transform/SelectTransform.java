package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.util.*;

@Transform.Module(name="select")
public class SelectTransform extends Transform {

    private static class Parameters {

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        private List<String> groupFields;

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }

        public boolean useUnnest() {
            return flattenField != null;
        }

        private void validate(Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if((this.filter == null || this.filter.isJsonNull())
                    && (this.select == null || !this.select.isJsonArray())) {

                errorMessages.add("requires filter or select parameter.");
            } else if(this.select.isJsonArray()) {
                int i=0;
                for(final JsonElement e : this.select.getAsJsonArray()) {
                    if(!e.isJsonObject()) {
                        errorMessages.add("select[" + i + "] must be object but: " + e);
                    }
                    i++;
                }
            }
            if(this.groupFields != null && !this.groupFields.isEmpty()) {
                if(!this.groupFields.stream().allMatch(inputSchema::hasField)) {
                    errorMessages.add("all groupFields[" + groupFields + "] must in input schema: " + inputSchema.getFields());
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);

        final Schema inputSchema = Union.createUnionSchema(inputs);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        final Schema outputSchema = createOutputSchema(inputSchema, parameters);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

        final List<SelectFunction> selectFunctions = SelectFunction
                .of(parameters.select, inputSchema.getFields());

        final PCollectionTuple outputs;
        if(SelectFunction.isGrouping(selectFunctions)) {
            final PCollection<KV<String,MElement>> input = inputs
                    .apply("UnionWithKey", Union.withKeys(parameters.groupFields)
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
            if(SelectFunction.isStateful(selectFunctions)) {
                final KvCoder<String, MElement> inputCoder = (KvCoder<String, MElement>) input.getCoder();
                final StatefulSelectDoFn statefulSelectDoFn;
                if(OptionUtil.isStreaming(inputs)) {
                    statefulSelectDoFn = new StatefulStreamingSelectDoFn(
                            getJobName(), getName(),
                            outputSchema, parameters.filter, selectFunctions, parameters.flattenField,
                            getLoggings(), getFailFast(), failuresTag, inputCoder.getValueCoder());
                } else {
                    statefulSelectDoFn = new StatefulBatchSelectDoFn(
                            getJobName(), getName(),
                            outputSchema, parameters.filter, selectFunctions, parameters.flattenField,
                            getLoggings(), getFailFast(), failuresTag, inputCoder.getValueCoder());
                }
                outputs = input
                        .apply("StatefulSelect", ParDo
                                .of(statefulSelectDoFn)
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            } else {
                outputs = input
                        .apply("GroupByKey", GroupByKey.create())
                        .apply("NavigationSelect", ParDo
                                .of(new NavigationSelectDoFn(
                                        getJobName(), getName(),
                                        outputSchema, parameters.filter, selectFunctions, parameters.flattenField,
                                        getLoggings(), getFailFast(), failuresTag))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
            }
        } else {
            outputs = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()))
                    .apply("StatelessSelect", ParDo
                            .of(new StatelessSelectDoFn(
                                    getJobName(), getName(),
                                    outputSchema, parameters.filter, selectFunctions, parameters.flattenField,
                                    getLoggings(), getFailFast(), failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        if(errorHandler != null) {
            errorHandler.addError(outputs.get(failuresTag));
        }

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    private Schema createOutputSchema(final Schema inputSchema, final Parameters parameters) {
        Schema outputSchema;
        if(parameters.useSelect()) {
            final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, inputSchema.getFields());
            if(selectFunctions.isEmpty()) {
                outputSchema = inputSchema;
            } else {
                outputSchema = SelectFunction.createSchema(selectFunctions);
            }
        } else {
            outputSchema = inputSchema;
        }

        if(parameters.useUnnest()) {
            outputSchema = Unnest.createSchema(outputSchema.getFields(), parameters.flattenField);
        }

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.ELEMENT);
        return outputSchema.withType(outputType);
    }

    private static class SelectDoFn<InputT> extends DoFn<InputT, MElement> {

        protected final String jobName;
        protected final String moduleName;
        protected final Schema outputSchema;

        protected final Map<String, Logging> logging;

        protected final boolean failFast;
        protected final TupleTag<BadRecord> failuresTag;

        protected final Filter filter;
        protected final Select select;
        protected final Unnest unnest;


        public SelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;
            this.outputSchema = outputSchema;

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;

            this.filter = Filter.of(filterJson);
            this.select = Select.of(selectFunctions);
            this.unnest = Unnest.of(flattenField);
        }

    }

    private static class StatelessSelectDoFn extends SelectDoFn<MElement> {

        public StatelessSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, logging, failFast, failuresTag);
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);

                if (!filter.filter(input)) {
                    Logging.log(LOG, logging, "not_matched", input);
                    return;
                }

                final List<MElement> outputs = new ArrayList<>();
                if (!select.useSelect() && !unnest.useUnnest()) {
                    outputs.add(input);
                } else if(select.useSelect()) {
                    final Map<String, Object> primitiveValues = select.select(input, c.timestamp());
                    if(unnest.useUnnest()) {
                        final List<Map<String, Object>> list = unnest.unnest(primitiveValues);
                        outputs.addAll(MElement.ofList(list, c.timestamp()));
                    } else {
                        final MElement output = MElement.of(primitiveValues, c.timestamp());
                        outputs.add(output);
                    }
                } else {
                    final List<Map<String, Object>> list = unnest.unnest(input);
                    outputs.addAll(MElement.ofList(list, c.timestamp()));
                }

                for(final MElement output : outputs) {
                    final MElement output_ = output.convert(outputSchema);
                    c.output(output_);
                    Logging.log(LOG, logging, "output", output_);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateless select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }
    }

    private static class StatefulSelectDoFn extends SelectDoFn<KV<String, MElement>> {

        protected static final String STATE_ID_BUFFER = "statefulSelectBuffer";
        protected static final String STATE_ID_MAX_COUNT_TIME = "statefulSelectMaxCountTime";
        protected static final String TIMER_ID = "statefulSelectBufferTimer";

        private final StatefulFunction.RangeBound maxRange;

        public StatefulSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, logging, failFast, failuresTag);
            this.maxRange = StatefulFunction.calcMaxRange(selectFunctions);
        }

        public Map<String, Object> process(
                final MElement input,
                final OrderedListState<MElement> bufferState,
                final ValueState<Instant> maxCountState,
                final Instant eventTime) {

            final Instant durationMinTimestamp = maxRange.firstTimestamp(eventTime);
            final Instant countMinTimestamp = Optional
                    .ofNullable(maxCountState.read())
                    .orElse(Instant.ofEpochMilli(0L));
            final Integer maxCount = maxRange.maxCount;
            final Instant minTimestamp = durationMinTimestamp.compareTo(countMinTimestamp) > 0 ? countMinTimestamp : durationMinTimestamp;

            //final Iterable<TimestampedValue<MElement>> buffer = bufferState.readRange(minTimestamp, eventTime);
            final List<TimestampedValue<MElement>> buffer = Lists.newArrayList(bufferState.readRange(minTimestamp, eventTime));

            final Map<String, Object> output = select.statefulSelect(input, buffer, eventTime, maxCount);

            //bufferState.clearRange();
            bufferState.add(TimestampedValue.of(input, eventTime));

            return output;
        }

    }

    private static class StatefulBatchSelectDoFn extends StatefulSelectDoFn {

        @StateId(STATE_ID_BUFFER)
        private final StateSpec<OrderedListState<MElement>> bufferStateSpec;
        @StateId(STATE_ID_MAX_COUNT_TIME)
        private final StateSpec<ValueState<Instant>> maxCountTimeStateSpec;

        public StatefulBatchSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag,
                final Coder<MElement> inputCoder) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, logging, failFast, failuresTag);

            this.bufferStateSpec = StateSpecs.orderedList(inputCoder);
            this.maxCountTimeStateSpec = StateSpecs.value(InstantCoder.of());
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
        }

        @ProcessElement
        @RequiresTimeSortedInput
        public void processElement(
                final ProcessContext c,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            final KV<String, MElement> kv = c.element();
            if(kv == null) {
                return;
            }
            final MElement input = kv.getValue();
            if(input == null) {
                return;
            }

            try {
                final Map<String, Object> result = process(input, bufferState, maxCountState, c.timestamp());
                final MElement output = MElement.of(result, c.timestamp());
                c.output(output);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateful batch select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }


        @OnWindowExpiration
        public void onWindowExpiration(
                final OutputReceiver<MElement> receiver,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            LOG.info("onWindowExpiration");

            //flush(receiver, bufferState, bufferSizeState);
        }

    }

    private static class StatefulStreamingSelectDoFn extends StatefulSelectDoFn {

        @StateId(STATE_ID_BUFFER)
        private final StateSpec<OrderedListState<MElement>> bufferStateSpec;
        @StateId(STATE_ID_MAX_COUNT_TIME)
        private final StateSpec<ValueState<Instant>> maxCountTimeStateSpec;

        @TimerId(TIMER_ID)
        private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        public StatefulStreamingSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag,
                final Coder<MElement> inputCoder) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, logging, failFast, failuresTag);

            this.bufferStateSpec = StateSpecs.orderedList(inputCoder);
            this.maxCountTimeStateSpec = StateSpecs.value(InstantCoder.of());
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
        }

        @ProcessElement
        public void processElement(
                ProcessContext c,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState,
                final @TimerId(TIMER_ID) Timer timer) {

            final KV<String, MElement> kv = c.element();
            if(kv == null) {
                return;
            }
            final MElement input = kv.getValue();
            if(input == null) {
                return;
            }

            try {
                final Map<String, Object> result = process(input, bufferState, maxCountState, c.timestamp());
                final MElement output = MElement.of(result, c.timestamp());
                c.output(output);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process stateful streaming select", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        @OnWindowExpiration
        public void onWindowExpiration(
                final OutputReceiver<Row> receiver,
                final @StateId(STATE_ID_BUFFER) OrderedListState<MElement> bufferState,
                final @StateId(STATE_ID_MAX_COUNT_TIME) ValueState<Instant> maxCountState) {

            LOG.info("onWindowExpiration");

            //flush(receiver, bufferState, bufferSizeState);
        }

    }

    private static class NavigationSelectDoFn extends SelectDoFn<KV<String, Iterable<MElement>>> {

        public NavigationSelectDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                //
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            super(jobName, moduleName, outputSchema, filterJson, selectFunctions, flattenField, logging, failFast, failuresTag);
        }

        @Setup
        public void setup() {
            filter.setup();
            select.setup();
            unnest.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            // TODO
        }
    }

}
