package com.mercari.solution.module.source;

import com.mercari.solution.util.gcp.StorageUtil;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;
import freemarker.template.Template;


public class MicrobatchQuery {

    public static <T> Transform<T> of(final String query,
                                      final String startDatetime,
                                      final Integer intervalSecond,
                                      final Integer gapSecond,
                                      final Integer maxDurationMinute,
                                      final String outputCheckpoint,
                                      final Integer catchupIntervalSecond,
                                      final Boolean useCheckpointAsStartDatetime,
                                      final DoFn<KV<KV<Integer, KV<Long, Instant>>, String>, T> queryExecuteDoFn) {

        return new Transform<T>(
                query, startDatetime, intervalSecond, gapSecond, maxDurationMinute, outputCheckpoint,
                catchupIntervalSecond, useCheckpointAsStartDatetime, queryExecuteDoFn);
    }


    private static class Transform<OutputT> extends PTransform<PCollection<Long>, PCollection<OutputT>> {

        private static final String SQL_SPLITTER = "--SPLITTER--";
        private static final Logger LOG = LoggerFactory.getLogger(MicrobatchQuery.class);

        private static final TupleTag<KV<Integer, KV<Long, Instant>>> tagCheckpoint = new TupleTag<KV<Integer, KV<Long, Instant>>>("checkpoint") {
            private static final long serialVersionUID = 1L;
        };

        private final TupleTag<OutputT> tagQueryResult = new TupleTag<OutputT>() {
            private static final long serialVersionUID = 1L;
        };

        private final String query;
        private final String startDatetime;
        private final Integer intervalSecond;
        private final Integer gapSecond;
        private final Integer maxDurationMinute;
        private final String outputCheckpoint;
        private final Integer catchupIntervalSecond;
        private final Boolean useCheckpointAsStartDatetime;
        private final DoFn<KV<KV<Integer, KV<Long, Instant>>, String>, OutputT> queryExecuteDoFn;

        private Transform(final String query,
                          final String startDatetime,
                          final Integer intervalSecond,
                          final Integer gapSecond,
                          final Integer maxDurationMinute,
                          final String outputCheckpoint,
                          final Integer catchupIntervalSecond,
                          final Boolean useCheckpointAsStartDatetime,
                          final DoFn<KV<KV<Integer, KV<Long, Instant>>, String>, OutputT> queryExecuteDoFn) {

            this.query = query;
            this.startDatetime = startDatetime;
            this.intervalSecond = intervalSecond;
            this.gapSecond = gapSecond;
            this.maxDurationMinute = maxDurationMinute;
            this.outputCheckpoint = outputCheckpoint;
            this.catchupIntervalSecond = catchupIntervalSecond;
            this.useCheckpointAsStartDatetime = useCheckpointAsStartDatetime;
            this.queryExecuteDoFn = queryExecuteDoFn;
        }

        public PCollection<OutputT> expand(PCollection<Long> begin) {

            final PCollectionView<Instant> startInstantView = begin.getPipeline()
                    .apply("Seed", Create.of(KV.of(true, true)))
                    .apply("ReadCheckpointText", ParDo.of(new ReadStartDatetimeDoFn()))
                    .apply("ToSingleton", Min.globally())
                    .apply("AsView", View.asSingleton());

            final PCollectionTuple queryResults = begin
                    .apply("GlobalWindow", Window
                            .<Long>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.standardDays(365)))
                    .apply("WithFixedKey", WithKeys.of(true))
                    .apply("GenerateQuery", ParDo.of(new QueryGenerateDoFn(
                            this.intervalSecond, this.gapSecond, this.query, this.maxDurationMinute, this.catchupIntervalSecond, startInstantView))
                            .withSideInputs(startInstantView))
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("MicrobatchQuery", ParDo.of(this.queryExecuteDoFn)
                            .withOutputTags(tagQueryResult, TupleTagList.of(tagCheckpoint)));

            if(this.outputCheckpoint != null) {
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
                        .apply("WriteCheckpoint", ParDo.of(new CheckpointSaveDoFn(this.outputCheckpoint)));
            }

            return queryResults.get(tagQueryResult);
        }

        private class ReadStartDatetimeDoFn extends DoFn<KV<Boolean, Boolean>, Instant> {

            private static final String STATEID_START_INSTANT = "startInstant";

            @StateId(STATEID_START_INSTANT)
            private final StateSpec<ValueState<Instant>> startState = StateSpecs.value(InstantCoder.of());

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
                if(useCheckpointAsStartDatetime) {
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
                if(outputCheckpoint == null) {
                    String errorMessage = "'outputCheckpoint' is not specified";
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
                if(!StorageUtil.exists(outputCheckpoint)) {
                    LOG.warn(String.format("outputCheckpoint object %s does not exist", outputCheckpoint));
                    return null;
                }
                final String checkpointDatetimeString = StorageUtil.readString(outputCheckpoint).trim();
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
                LOG.info(String.format("Start from startDatetime: %s", startDatetime));
                if(startDatetime == null) {
                    String errorMessage = "'startDatetimeString' is not specified";
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
                try {
                    return Instant.parse(startDatetime);
                } catch (Exception e) {
                    final String errorMessage = String.format("Failed to parse startDatetime %s, cause %s",
                            startDatetime, e.getMessage());
                    LOG.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }

        private static class QueryGenerateDoFn extends DoFn<KV<Boolean, Long>, KV<KV<Integer, KV<Long,Instant>>, String>> {

            private static final String STATEID_INTERVAL_COUNT = "intervalCount";
            private static final String STATEID_LASTEVENT_TIME = "lastEventTime";
            private static final String STATEID_LASTPROCESSING_TIME = "lastProcessingTime";
            private static final String STATEID_CATCHUP = "catchup";
            private static final String STATEID_PRECATCHUP = "preCatchup";

            private final Integer intervalSecond;
            private final Integer gapSecond;
            private final String query;
            private final Integer maxDurationMinute;
            private final Integer catchupIntervalSecond;
            private final PCollectionView<Instant> startInstantView;

            private transient Template template;
            private transient Duration durationGap;
            private transient Duration durationInterval;
            private transient Duration durationCatchupInterval;
            private transient Duration durationMax;

            private QueryGenerateDoFn(final Integer intervalSecond,
                                      final Integer gapSecond,
                                      final String query,
                                      final Integer maxDurationMinute,
                                      final Integer catchupIntervalSecond,
                                      final PCollectionView<Instant> startInstantView) {

                this.intervalSecond = intervalSecond;
                this.gapSecond = gapSecond;
                this.query = query;
                this.maxDurationMinute = maxDurationMinute;
                this.catchupIntervalSecond = catchupIntervalSecond;
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
                this.template = createTemplate(this.query);
                this.durationGap = Duration.standardSeconds(gapSecond);
                this.durationInterval = Duration.standardSeconds(intervalSecond);
                this.durationCatchupInterval = Duration.standardSeconds(catchupIntervalSecond);
                this.durationMax = Duration.standardMinutes(maxDurationMinute);
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
                if(allEventDuration.getStandardMinutes() > this.maxDurationMinute) {
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
                final String[] queries = queryString.split(SQL_SPLITTER);
                for (int queryIdx = 0; queryIdx < queries.length; queryIdx++) {
                    final KV<Integer, KV<Long,Instant>> checkpoint = KV.of(queryIdx, KV.of(queryCount, queryEventTime));
                    c.output(KV.of(checkpoint, queries[queryIdx]));
                }

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

        private class MaxSequenceCalcDoFn extends DoFn<KV<Boolean, KV<Integer, KV<Long, Instant>>>, Instant> {

            private static final long INITIAL_COUNT = 1L;
            private static final String STATEID_HEAD = "headState";
            private static final String STATEID_VALUES = "valuesState";

            private final PCollectionView<Instant> startInstantView;
            private transient int queryNum;

            @Setup
            public void setup() {
                this.queryNum = query.split(SQL_SPLITTER).length;
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

                if(head.values().size() == 0 || head.values().size() != this.queryNum) {
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

        private class CheckpointSaveDoFn extends DoFn<Instant, Void> {

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
