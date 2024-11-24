package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Limit {

    private static final Logger LOG = LoggerFactory.getLogger(Limit.class);

    public static Transform of(
            final LimitParameters limit,
            final List<Schema> schemas,
            boolean isStreaming) {

        return new Transform(limit, schemas, isStreaming);
    }


    public static class LimitParameters implements Serializable {

        private Integer count;
        private Instant outputStartAt;

        public Integer getCount() {
            return count;
        }

        public Instant getOutputStartAt() {
            return outputStartAt;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.count == null && this.outputStartAt == null) {
                errorMessages.add("limit.count parameter must not be null.");
            } else if(this.count != null) {
                if(this.count < 1) {
                    errorMessages.add("limit.count parameter must not over zero.");
                }
            }
            return errorMessages;
        }

        public void setDefaults() {

        }

    }

    public static class Transform extends PTransform<PCollection<KV<String,MElement>>, PCollection<MElement>> {

        private final LimitParameters limit;
        private final List<Schema> schemas;
        private final boolean isStreaming;

        Transform(
                final LimitParameters limit,
                final List<Schema> schemas,
                boolean isStreaming) {

            this.limit = limit;
            this.schemas = schemas;
            this.isStreaming = isStreaming;
        }

        @Override
        public PCollection<MElement> expand(PCollection<KV<String, MElement>> input) {
            final DoFn<KV<String,MElement>, MElement> limitDoFn;
            if(isStreaming) {
                limitDoFn = new LimitStreamingDoFn(limit, ElementCoder.of(schemas));
            } else {
                limitDoFn = new LimitBatchDoFn(limit, ElementCoder.of(schemas));
            }
            return input.apply("Limit", ParDo.of(limitDoFn));
        }
    }

    private static class LimitBatchDoFn extends DoFn<KV<String,MElement>, MElement> {

        protected static final String STATEID_COUNT = "aggregationLimitCount";
        protected static final String STATEID_BUFFER = "aggregationLimitBuffer";

        @StateId(STATEID_COUNT)
        private final StateSpec<ValueState<Integer>> countSpec;
        @StateId(STATEID_BUFFER)
        private final StateSpec<ValueState<KV<Instant, MElement>>> bufferSpec;

        private final Integer limitCount;
        private final Instant outputStartAt;


        LimitBatchDoFn(
                final LimitParameters limit,
                final Coder<MElement> coder) {

            this.countSpec = StateSpecs.value(VarIntCoder.of());
            this.bufferSpec = StateSpecs.value(KvCoder.of(InstantCoder.of(), coder));

            this.limitCount = limit.getCount();
            this.outputStartAt = limit.getOutputStartAt();


        }


        @Setup
        public void setup() {

        }

        @ProcessElement
        @RequiresTimeSortedInput
        public void processElement(
                final ProcessContext c,
                final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant, MElement>> bufferValueState) {

            if(c.element() == null) {
                return;
            }

            final MElement input = c.element().getValue();
            final Instant timestamp = c.timestamp();

            if(this.outputStartAt != null) {
                final KV<Instant, MElement> buffer = bufferValueState.read();
                if(this.outputStartAt.isAfter(timestamp)) {
                    if(buffer == null || timestamp.isAfter(buffer.getKey())) {
                        bufferValueState.write(KV.of(timestamp, input));
                    }
                    return;
                } else if(buffer != null) {

                }
            }

            if(this.limitCount != null) {
                final Integer outputCount = Optional
                        .ofNullable(countValueState.read())
                        .orElse(0);
                if(this.limitCount > outputCount) {
                    c.output(input);
                    countValueState.write(outputCount + 1);
                }
            } else {
                c.output(input);
            }

        }

            /*
            @OnWindowExpiration
            public void onWindowExpiration(
                    final OutputReceiver<Row> receiver,
                    final @StateId(STATE_ID_BUFFER) BagState<MatchingEngineUtil.DataPoint> bufferState,
                    final @StateId(STATE_ID_BUFFER_SIZE) CombiningState<Long, long[], Long> bufferSizeState) {

                LOG.info("onWindowExpiration");

                flush(receiver, bufferState, bufferSizeState);
            }

             */


    }

    private static class LimitStreamingDoFn extends DoFn<KV<String,MElement>, MElement> {

        protected static final String STATEID_COUNT = "aggregationLimitCount";
        protected static final String STATEID_BUFFER = "aggregationLimitBuffer";
        protected static final String TIMERID_OUTPUT = "aggregationLimitOutput";

        @StateId(STATEID_COUNT)
        private final StateSpec<ValueState<Integer>> countSpec;
        @StateId(STATEID_BUFFER)
        private final StateSpec<ValueState<KV<Instant, MElement>>> bufferSpec;

        @TimerId(TIMERID_OUTPUT)
        private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        private final Integer limitCount;
        private final Instant outputStartAt;

        LimitStreamingDoFn(
                final LimitParameters limit,
                final Coder<MElement> coder) {

            this.countSpec = StateSpecs.value(VarIntCoder.of());
            this.bufferSpec = StateSpecs.value(KvCoder.of(InstantCoder.of(), coder));
            this.limitCount = limit.getCount();
            this.outputStartAt = limit.getOutputStartAt();
        }

        @Setup
        public void setup() {

        }

        @ProcessElement
        public void processElement(
                final ProcessContext c,
                final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant, MElement>> bufferValueState,
                final @TimerId(TIMERID_OUTPUT) Timer timer) {

            if(c.element() == null) {
                return;
            }

            final MElement input = c.element().getValue();
            final Instant timestamp = c.timestamp();

            if(this.outputStartAt != null && this.outputStartAt.isAfter(timestamp)) {
                final KV<Instant, MElement> buffer = Optional
                        .ofNullable(bufferValueState.read())
                        .orElseGet(() -> KV.of(Instant.ofEpochMilli(0L), input));
                if(timestamp.isAfter(buffer.getKey())) {
                    bufferValueState.write(KV.of(timestamp, input));
                    timer.set(this.outputStartAt);
                }
                LOG.info("buffer timestamp: " + timestamp);
                return;
            }

            if(this.limitCount != null) {
                final Integer outputCount = Optional
                        .ofNullable(countValueState.read())
                        .orElse(0);
                if(this.limitCount > outputCount) {
                    c.output(input);
                    countValueState.write(outputCount + 1);
                    LOG.info("output count: " + outputCount);
                } else {
                    LOG.info("limit count: " + outputCount);
                }
            } else {
                c.output(input);
            }

        }

        @OnTimer(TIMERID_OUTPUT)
        public void onTimer(final OnTimerContext c,
                            final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                            final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant,MElement>> bufferValueState) {

            final KV<Instant, MElement> buffer = bufferValueState.read();
            if(buffer != null) {
                c.output(buffer.getValue());
                final Integer outputCount = Optional
                        .ofNullable(countValueState.read())
                        .orElse(0);
                countValueState.write(outputCount + 1);
            }
        }

    }

}
