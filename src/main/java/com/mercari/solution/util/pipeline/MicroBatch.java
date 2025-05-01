package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.gcp.StorageUtil;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

public class MicroBatch {

    public static class MicroBatchParameter implements Serializable {

        private Integer intervalSecond;
        private Integer gapSecond;
        private Integer maxDurationMinute;
        private Integer catchupIntervalSecond;

        private String startDatetime;
        private String outputCheckpoint;
        private Boolean useCheckpointAsStartDatetime;

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public void setDefaults() {
            if(intervalSecond == null) {
                this.intervalSecond = 60;
            }
            if(gapSecond == null) {
                this.gapSecond = 30;
            }
            if(maxDurationMinute == null) {
                this.maxDurationMinute = 60;
            }
            if(catchupIntervalSecond == null) {
                this.catchupIntervalSecond = this.intervalSecond;
            }
            if(useCheckpointAsStartDatetime == null) {
                this.useCheckpointAsStartDatetime = false;
            }
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatch.class);

    public static MicroBatchQuery query(
            final String query,
            final MicroBatchParameter parameter,
            final DoFn<KV<KV<Long,Instant>, String>, MElement> queryDoFn) {
        return new MicroBatchQuery(query, parameter, queryDoFn);
    }


    public static class MicroBatchQuery extends PTransform<PBegin, PCollection<MElement>> {

        private static final Logger LOG = LoggerFactory.getLogger(MicroBatchQuery.class);

        private final TupleTag<KV<Integer, KV<Long, Instant>>> tagCheckpoint = new TupleTag<>() {};
        private final TupleTag<MElement> tagQueryResult = new TupleTag<>() {};

        private final String query;
        private final MicroBatchParameter parameter;
        private final DoFn<KV<KV<Long,Instant>,String>, MElement> queryDoFn;

        MicroBatchQuery(
                final String query,
                final MicroBatchParameter parameter,
                final DoFn<KV<KV<Long,Instant>, String>, MElement> queryDoFn) {

            this.query = query;
            this.parameter = parameter;
            this.queryDoFn = queryDoFn;
        }

        @Override
        public PCollection<MElement> expand(PBegin begin) {

            final PCollectionView<Instant> startInstantView = begin.getPipeline()
                    .apply("Seed", Create.of(KV.of(true, true)))
                    .apply("ReadCheckpointText", ParDo.of(new ReadStartDatetimeDoFn(parameter)))
                    .apply("ToSingleton", Min.globally())
                    .apply("AsView", View.asSingleton());

            final PCollectionTuple queryResults = begin
                    .apply("GenerateSequence", GenerateSequence
                            .from(0)
                            .withRate(1, Duration.standardSeconds(parameter.intervalSecond)))
                    .apply("GlobalWindow", Window
                            .<Long>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.standardDays(365)))
                    .apply("WithFixedKey", WithKeys.of(true))
                    .apply("GenerateQuery", ParDo.of(new QueryGenerateDoFn(query, parameter, startInstantView))
                            .withSideInputs(startInstantView))
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("MicroBatchQuery", ParDo.of(this.queryDoFn)
                            .withOutputTags(tagQueryResult, TupleTagList.of(tagCheckpoint)));

            if(parameter.outputCheckpoint != null) {
                queryResults.get(tagCheckpoint)
                        .apply("CheckpointCalcTrigger", Window.<KV<Integer, KV<Long, Instant>>>configure()
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .discardingFiredPanes()
                                .withAllowedLateness(Duration.standardDays(2)))
                        .apply("WithStateDummyKey", WithKeys.of(true))
                        .apply("CalcCheckpoint", ParDo.of(new MaxSequenceCalcDoFn(startInstantView))
                                .withSideInputs(startInstantView))
                        .apply("WriteWindow", Window
                                .<Instant>into(FixedWindows.of(Duration.standardSeconds(1L)))
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane())))
                        .apply("ExtractOne", Max.<Instant>globally().withoutDefaults())
                        .apply("WriteCheckpoint", ParDo.of(new CheckpointSaveDoFn(parameter.outputCheckpoint)));
            }

            return queryResults.get(tagQueryResult);
        }

        private static class ReadStartDatetimeDoFn extends DoFn<KV<Boolean, Boolean>, Instant> {

            private static final String STATEID_START_INSTANT = "startInstant";

            private final MicroBatchParameter parameter;

            @StateId(STATEID_START_INSTANT)
            private final StateSpec<ValueState<Instant>> startState = StateSpecs.value(InstantCoder.of());


            ReadStartDatetimeDoFn(final MicroBatchParameter parameter) {
                this.parameter = parameter;
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_START_INSTANT) ValueState<Instant> startState) {

                if(startState.read() != null) {
                    LOG.info(String.format("ReadStartDatetime from state: %s", startState.read()));
                    c.output(startState.read());
                    return;
                }
                final Instant start = getStartDateTimeOrCheckpoint();
                LOG.info(String.format("ReadStartDatetime from startDatetime parameter: %s", start));
                startState.write(start);
                c.output(start);
            }

            private Instant getStartDateTimeOrCheckpoint() {
                if(parameter.useCheckpointAsStartDatetime) {
                    final Instant start = getCheckpointDateTime();
                    if(start != null) {
                        return start;
                    } else {
                        LOG.warn("'useCheckpointAsStartDatetime' is 'True' but checkpoint object doesn't exist. Using 'startDatetime' instead");
                    }
                }
                return getStartDateTime();
            }

            private Instant getCheckpointDateTime() {
                // Returns Instant object from outputCheckpoint or null if the storage object does not exist
                if(parameter.outputCheckpoint == null) {
                    String errorMessage = "'outputCheckpoint' is not specified";
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
                if(!StorageUtil.exists(parameter.outputCheckpoint)) {
                    LOG.warn(String.format("outputCheckpoint object %s does not exist", parameter.outputCheckpoint));
                    return null;
                }
                final String checkpointDatetimeString = StorageUtil.readString(parameter.outputCheckpoint).trim();
                try {
                    LOG.info(String.format("Start from checkpoint: %s", checkpointDatetimeString));
                    return Instant.parse(checkpointDatetimeString);
                } catch (Exception e) {
                    final String errorMessage = String.format("Failed to parse checkpoint text %s,  cause %s",
                            checkpointDatetimeString, e.getMessage());
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }

            private Instant getStartDateTime() {
                LOG.info(String.format("Start from startDatetime: %s", parameter.startDatetime));
                if(parameter.startDatetime == null) {
                    String errorMessage = "'startDatetimeString' is not specified";
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
                try {
                    return Instant.parse(parameter.startDatetime);
                } catch (Exception e) {
                    final String errorMessage = String.format("Failed to parse startDatetime %s, cause %s",
                            parameter.startDatetime, e.getMessage());
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }

        private static class QueryGenerateDoFn extends DoFn<KV<Boolean, Long>, KV<KV<Long,Instant>, String>> {

            private static final String STATEID_INTERVAL_COUNT = "intervalCount";
            private static final String STATEID_LASTEVENT_TIME = "lastEventTime";
            private static final String STATEID_LASTPROCESSING_TIME = "lastProcessingTime";
            private static final String STATEID_CATCHUP = "catchup";
            private static final String STATEID_PRECATCHUP = "preCatchup";

            private final String query;
            private final MicroBatchParameter parameter;
            private final PCollectionView<Instant> startInstantView;

            private transient Template template;
            private transient Duration durationGap;
            private transient Duration durationInterval;
            private transient Duration durationCatchupInterval;
            private transient Duration durationMax;

            private QueryGenerateDoFn(
                    final String query,
                    final MicroBatchParameter parameter,
                    final PCollectionView<Instant> startInstantView) {

                this.query = query;
                this.parameter = parameter;
                this.startInstantView = startInstantView;
            }

            @StateId(STATEID_INTERVAL_COUNT)
            private final StateSpec<ValueState<Long>> queryCountState = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATEID_LASTEVENT_TIME)
            private final StateSpec<ValueState<Instant>> lastEventTimeState = StateSpecs.value(InstantCoder.of());
            @StateId(STATEID_LASTPROCESSING_TIME)
            private final StateSpec<ValueState<Instant>> lastProcessingTimeState = StateSpecs.value(InstantCoder.of());
            @StateId(STATEID_CATCHUP)
            private final StateSpec<ValueState<Boolean>> catchupState = StateSpecs.value(BooleanCoder.of());
            @StateId(STATEID_PRECATCHUP)
            private final StateSpec<ValueState<Boolean>> preCatchupState = StateSpecs.value(BooleanCoder.of());

            @Setup
            public void setup() {
                this.template = createTemplate(query);
                this.durationGap = Duration.standardSeconds(parameter.gapSecond);
                this.durationInterval = Duration.standardSeconds(parameter.intervalSecond);
                this.durationCatchupInterval = Duration.standardSeconds(parameter.catchupIntervalSecond);
                this.durationMax = Duration.standardMinutes(parameter.maxDurationMinute);
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_LASTEVENT_TIME) ValueState<Instant> lastEventTimeState,
                                       final @StateId(STATEID_LASTPROCESSING_TIME) ValueState<Instant> lastProcessingTimeState,
                                       final @StateId(STATEID_INTERVAL_COUNT) ValueState<Long> queryCountState,
                                       final @StateId(STATEID_CATCHUP) ValueState<Boolean> catchupState,
                                       final @StateId(STATEID_PRECATCHUP) ValueState<Boolean> preCatchupState) {

                final Instant currentTime = Instant.now();
                final Instant endEventTime = currentTime.minus(durationGap);
                final Instant lastQueryEventTime = Optional.ofNullable(lastEventTimeState.read()).orElse(c.sideInput(this.startInstantView));

                // Skip if last queried event time(plus interval) is over current time(minus gap duration)
                if(lastQueryEventTime.plus(durationInterval).isAfter(endEventTime)) {
                    return;
                }

                // Skip if pre-query's duration was over maxDurationMinute.
                final Boolean catchup = Optional.ofNullable(catchupState.read()).orElse(false);
                final Instant lastProcessingTime = Optional.ofNullable(lastProcessingTimeState.read()).orElse(new Instant(0L));
                if(lastProcessingTime
                        .plus(catchup ? this.durationCatchupInterval : this.durationInterval)
                        .isAfter(currentTime)) {
                    return;
                }

                // Determine query duration
                final Duration allEventDuration = new Duration(lastQueryEventTime, endEventTime);
                final Instant queryEventTime;
                if(allEventDuration.getStandardMinutes() > parameter.maxDurationMinute) {
                    queryEventTime = lastQueryEventTime.plus(this.durationMax);
                    catchupState.write(true);
                    preCatchupState.write(true);
                } else {
                    queryEventTime = lastQueryEventTime.plus(allEventDuration);
                    final Boolean preCatchup = Optional.ofNullable(preCatchupState.read()).orElse(false);
                    catchupState.write(preCatchup); // To skip pre-pre-query's duration was over maxDurationMinute
                    preCatchupState.write(false);
                }

                // Generate Queries and output
                long queryCount = Optional.ofNullable(queryCountState.read()).orElse(1L);
                final String queryString = createQuery(template, lastQueryEventTime, queryEventTime);
                /*
                final String[] queries = queryString.split(SQL_SPLITTER);
                for (int queryIdx = 0; queryIdx < queries.length; queryIdx++) {
                    final KV<Integer, KV<Long,Instant>> checkpoint = KV.of(queryIdx, KV.of(queryCount, queryEventTime));
                    c.output(KV.of(checkpoint, queries[queryIdx]));
                }
                 */
                final KV<Long,Instant> checkpoint = KV.of(queryCount, queryEventTime);
                c.output(KV.of(checkpoint, queryString));

                LOG.info(String.format("Query from: %s to: %s count: %d", lastQueryEventTime.toString(), queryEventTime.toString(), queryCount));

                // Update states
                queryCount += 1;
                lastEventTimeState.write(queryEventTime);
                lastProcessingTimeState.write(currentTime);
                queryCountState.write(queryCount);
            }

            @Override
            public org.joda.time.Duration getAllowedTimestampSkew() {
                return org.joda.time.Duration.standardDays(365);
            }

        }

        private static class MaxSequenceCalcDoFn extends DoFn<KV<Boolean, KV<Integer, KV<Long, Instant>>>, Instant> {

            private static final long INITIAL_COUNT = 1L;
            private static final String STATEID_HEAD = "headState";
            private static final String STATEID_VALUES = "valuesState";

            private final PCollectionView<Instant> startInstantView;
            private transient int queryNum;

            @Setup
            public void setup() {
                this.queryNum = 1;//query.split(SQL_SPLITTER).length;
            }

            @StateId(STATEID_HEAD)
            private final StateSpec<ValueState<Map<Integer, KV<Long, Instant>>>> headState = StateSpecs
                    .value(MapCoder.of(
                            BigEndianIntegerCoder.of(),
                            KvCoder.of(BigEndianLongCoder.of(), InstantCoder.of())));

            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<Map<Integer, List<KV<Long, Instant>>>>> valuesState = StateSpecs
                    .value(MapCoder.of(
                            BigEndianIntegerCoder.of(),
                            ListCoder.of(KvCoder.of(BigEndianLongCoder.of(), InstantCoder.of()))));

            private MaxSequenceCalcDoFn(PCollectionView<Instant> startInstantView) {
                this.startInstantView = startInstantView;
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @Element KV<Boolean, KV<Integer, KV<Long, Instant>>> kv,
                                       final @StateId(STATEID_HEAD) ValueState<Map<Integer, KV<Long, Instant>>> headState,
                                       final @StateId(STATEID_VALUES) ValueState<Map<Integer, List<KV<Long, Instant>>>> valuesState,
                                       final OutputReceiver<Instant> out) {

                final KV<Integer, KV<Long, Instant>> input = kv.getValue();
                final Map<Integer, KV<Long, Instant>> head = Optional.ofNullable(headState.read()).orElse(new HashMap<>());
                final Map<Integer, List<KV<Long, Instant>>> values = Optional.ofNullable(valuesState.read()).orElse(new HashMap<>());

                final Instant startInstant = c.sideInput(this.startInstantView);
                addInput(head, values, input, startInstant);
                values.put(input.getKey(), values.get(input.getKey()).stream()
                        .filter(value -> {
                            if(head.containsKey(input.getKey())) {
                                return value.getKey() >= head.get(input.getKey()).getKey();
                            }
                            return value.getKey() >= INITIAL_COUNT - 1;
                        })
                        .distinct()
                        .collect(Collectors.toList()));

                if(head.values().isEmpty() || head.values().size() != this.queryNum) {
                    out.output(startInstant);
                } else {
                    out.output(Collections.min(head.values(), (kv1, kv2) -> (int)(kv1.getKey() - kv2.getKey())).getValue());
                }

                headState.write(head);
                valuesState.write(values);
            }

            private boolean isHead(final Map<Integer, KV<Long, Instant>> head, final Integer index) {
                if(!head.containsKey(index)) {
                    return false;
                } else {
                    return head.get(index).getKey() >= INITIAL_COUNT;
                }
            }

            private void updateHead(final Integer key, final Map<Integer, KV<Long, Instant>> head, final Map<Integer, List<KV<Long, Instant>>> values) {
                if(isHead(head, key)) {
                    Collections.sort(values.get(key), (e1, e2) -> (int)(e1.getKey() - e2.getKey()));
                    Long headCount = head.get(key).getKey();
                    Instant headTime = head.get(key).getValue();
                    for(final KV<Long, Instant> value : values.get(key)) {
                        if(value.getKey() - headCount > 1) {
                            break;
                        }
                        headCount = value.getKey();
                        headTime = value.getValue();
                    }
                    head.put(key, KV.of(headCount, headTime));
                }
            }

            private void addInput(final Map<Integer, KV<Long, Instant>> head,
                                  final Map<Integer, List<KV<Long, Instant>>> values,
                                  final KV<Integer, KV<Long, Instant>> input,
                                  final Instant startInstant) {

                if(!head.containsKey(input.getKey())) {
                    final KV<Long, Instant> initialHead = KV
                            .of(INITIAL_COUNT - 1, startInstant);
                    head.put(input.getKey(), initialHead);
                }
                if(input.getValue().getKey() < head.get(input.getKey()).getKey()) {
                    return;
                }
                if(!isHead(head, input.getKey()) && INITIAL_COUNT == input.getValue().getKey()) {
                    head.put(input.getKey(), input.getValue());
                }
                values.merge(input.getKey(), Arrays.asList(input.getValue()), (list1, list2) -> {
                    List<KV<Long,Instant>> list = new ArrayList<>();
                    list.addAll(list1);
                    list.addAll(list2);
                    return list;
                });
                updateHead(input.getKey(), head, values);
            }

        }

        private static class CheckpointSaveDoFn extends DoFn<Instant, Void> {

            private final Distribution checkpointDistribution = Metrics.distribution("checkpoint", "lag_millis");
            private final String outputCheckpoint;

            private CheckpointSaveDoFn(final String outputCheckpoint) {
                this.outputCheckpoint = outputCheckpoint;
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                final Instant executedMinEventTime = c.element();
                if(executedMinEventTime.getMillis() == 0) {
                    LOG.info("Query not yet executed");
                    return;
                }
                final String checkpointDatetimeString = executedMinEventTime.toString();
                StorageUtil.writeString(outputCheckpoint, checkpointDatetimeString);
                LOG.info(String.format("Checkpoint: %s", checkpointDatetimeString));
                long checkpointLagMillis = Instant.now().getMillis() - executedMinEventTime.getMillis();
                LOG.info(String.format("Checkpoint lag millis: %d", checkpointLagMillis));
                this.checkpointDistribution.update(checkpointLagMillis);
            }

        }

    }

    static Template createTemplate(final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_30);
        templateConfig.setNumberFormat("computer");
        try {
            return new Template("config", new StringReader(template), templateConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String createQuery(final Template template, final Instant lastTime, final Instant eventTime) {

        final Map<String, Object> context = new HashMap<>();
        context.put("__EVENT_EPOCH_SECOND__", eventTime.getMillis() / 1000);
        context.put("__EVENT_EPOCH_SECOND_PRE__", lastTime.getMillis() / 1000);
        context.put("__EVENT_EPOCH_MILLISECOND__", eventTime.getMillis());
        context.put("__EVENT_EPOCH_MILLISECOND_PRE__", lastTime.getMillis());
        context.put("__EVENT_DATETIME_ISO__", eventTime.toString(ISODateTimeFormat.dateTime()));
        context.put("__EVENT_DATETIME_ISO_PRE__", lastTime.toString(ISODateTimeFormat.dateTime()));
        final StringWriter sw = new StringWriter();
        try {
            template.process(context, sw);
            return sw.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TemplateException e) {
            throw new RuntimeException(e);
        }
    }

}
