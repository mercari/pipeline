package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutationCoder;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ChangeStreamTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeStreamTransform.class);

    public static class ChangeStreamTransformParameters implements Serializable {

        private Type type;
        private String startAt;
        private String endAt;

        private List<String> tables;
        private Map<String, String> renameTables;
        private Boolean applyUpsertForInsert;
        private Boolean applyUpsertForUpdate;

        private Boolean debug;

        public Type getType() {
            return type;
        }

        public String getStartAt() {
            return startAt;
        }

        public String getEndAt() {
            return endAt;
        }

        public List<String> getTables() {
            return tables;
        }

        public Map<String, String> getRenameTables() {
            return renameTables;
        }

        public Boolean getApplyUpsertForInsert() {
            return applyUpsertForInsert;
        }

        public Boolean getApplyUpsertForUpdate() {
            return applyUpsertForUpdate;
        }

        public Boolean getDebug() {
            return debug;
        }

        public void validate(final String name, final boolean isStreaming) {
            final List<String> errorMessages = new ArrayList<>();
            if(type == null) {
                errorMessages.add("changeStream transform module[" + name + "].type must not be null");
            } else {
                if(Type.restore.equals(type) && isStreaming) {
                    errorMessages.add("changeStream transform module[" + name + "].type: " + type + " requires batch mode");
                }
            }
            if(startAt != null && DateTimeUtil.toInstant(startAt, true) == null) {
                errorMessages.add("changeStream transform module[" + name + "].startAt value: " + startAt + " is illegal");
            }
            if(endAt != null && DateTimeUtil.toInstant(endAt, true) == null) {
                errorMessages.add("changeStream transform module[" + name + "].endAt value: " + endAt + " is illegal");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            if(this.tables == null) {
                this.tables = new ArrayList<>();
            }
            if(this.renameTables == null) {
                this.renameTables = new HashMap<>();
            }
            if(this.applyUpsertForInsert == null) {
                this.applyUpsertForInsert = false;
            }
            if(this.applyUpsertForUpdate == null) {
                this.applyUpsertForUpdate = false;
            }
            if(this.debug == null) {
                this.debug = false;
            }
        }

        public static ChangeStreamTransformParameters of(final TransformConfig config, final boolean isStreaming) {
            final ChangeStreamTransformParameters parameters = new Gson().fromJson(config.getParameters(), ChangeStreamTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("changeStream transform module[" + config.getName() + "].parameters must not be empty!");
            }
            parameters.validate(config.getName(), isStreaming);
            parameters.setDefaults();
            return parameters;
        }

    }

    private enum Type {
        capture,
        replicate,
        restore
    }

    private enum Service {
        spanner,
        bigquery,
        bigtable,
        datastream
    }


    public String getName() {
        return "changeStream";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, TransformConfig config) {
        final ChangeStreamTransformParameters parameters = ChangeStreamTransformParameters.of(config, OptionUtil.isStreaming(inputs.get(0).getCollection()));

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag inputTag = new TupleTag<>(){};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            tuple = tuple.and(inputTag, input.getCollection());
        }

        switch (parameters.getType()) {
            case capture -> {
                throw new IllegalArgumentException("Not supported type: capture");
            }
            case replicate -> {
                final Service sourceService = detectSourceService(inputs.get(0).getAvroSchema());
                //final Service targetService = parameters.getTargetService();
                if(OptionUtil.isStreaming(tuple)) {
                    // Streaming replicate
                    /*
                    return switch (targetService) {
                        case spanner -> {
                            final Coder<Mutation> coder = SerializableCoder.of(Mutation.class);
                            final Replicate<Mutation> replicate = new Replicate<>(
                                    parameters,
                                    inputTags, inputNames, inputTypes,
                                    StructSchemaUtil::convert,
                                    StructSchemaUtil::getChangeDataCommitTimestampMicros,
                                    StructSchemaUtil::accumulateChangeRecords,
                                    coder);
                            final PCollection<Mutation> mutations = tuple.apply(config.getName(), replicate).setCoder(coder);
                            FCollection<Mutation> output = FCollection.of(config.getName(), mutations, DataType.MUTATION, StructSchemaUtil.createDataChangeRecordRowSchema());
                            yield Collections.singletonMap(config.getName(), output);
                        }
                        case bigquery -> {
                            throw new IllegalArgumentException("Not supported restore service: bigquery");
                        }
                        case bigtable -> {
                            throw new IllegalArgumentException("Not supported restore service: bigtable");
                        }
                        case datastream -> throw new IllegalArgumentException();
                    };

                     */
                    throw new IllegalArgumentException();
                } else {
                    // Batch restore
                    return switch (sourceService) {
                        case spanner -> {
                            final Restore restore = new Restore(
                                    parameters,
                                    inputTags, inputNames, inputTypes,
                                    StructSchemaUtil::convertChangeRecordToMutations,
                                    StructSchemaUtil::getChangeDataCommitTimestampMicros,
                                    StructSchemaUtil::accumulateChangeRecords);
                            final PCollection<UnifiedMutation> mutations = tuple
                                    .apply(config.getName(), restore)
                                    .setCoder(UnifiedMutationCoder.of());
                            yield Collections.singletonMap(
                                    config.getName(), FCollection.of(config.getName(), mutations, DataType.UNIFIEDMUTATION, StructSchemaUtil.createDataChangeRecordRowSchema()));
                        }
                        case bigtable -> {
                            /*
                            switch (targetService) {
                                case spanner -> throw new IllegalArgumentException();
                                case bigtable -> throw new IllegalArgumentException();
                                case bigquery -> throw new IllegalArgumentException();
                                case datastream -> throw new IllegalArgumentException();
                                default -> throw new IllegalArgumentException();
                            }

                             */
                            throw new IllegalArgumentException();
                        }
                        case datastream -> throw new IllegalArgumentException();
                        case bigquery -> throw new IllegalArgumentException();
                    };
                }
            }
            default -> throw new IllegalArgumentException("Not supported type: " + parameters.getType());
        }
    }

    private static Service detectSourceService(final Schema inputSchema) {
        return Service.spanner;
    }

    /*
    public static class Replicate<MutationT> extends PTransform<PCollectionTuple, PCollection<MutationT>> {

        private final ChangeStreamTransformParameters parameters;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        Replicate(final ChangeStreamTransformParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final Converter<UnionValue, ChangeRecordT> changeRecordConverter,
                final TimestampMicrosGetter commitTimestampMicrosGetter,
                final ChangeRecordsAccumulator<ChangeRecordT, MutationT> accumulator,
                final Coder<ChangeRecordT> changeRecordCoder) {

            this.parameters = parameters;

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;

            this.changeRecordConverter = changeRecordConverter;
            this.commitTimestampMicrosGetter = commitTimestampMicrosGetter;
            this.accumulator = accumulator;
            this.changeRecordCoder = changeRecordCoder;
        }

        @Override
        public PCollection<MutationT> expand(PCollectionTuple inputs) {
            return inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames))
                    .apply("ToMutationWithKey", ParDo
                            .of(new ChangeRecordWithKeyDoFn<>(parameters, changeRecordConverter, commitTimestampMicrosGetter)))
                    .setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), KvCoder.of(VarLongCoder.of(), changeRecordCoder)))
                    //.apply(ParDo.of(new ChangeRecordsAccumulateDoFn2<>(parameters, accumulator, changeRecordCoder)));
                    .apply("GroupByKey", GroupByKey.create())
                    .apply("Accumulate", ParDo
                            .of(new Restore.ChangeRecordsAccumulateDoFn<>(parameters, accumulator)));
        }

        private static class ChangeRecordWithKeyDoFn<ChangeRecordT> extends DoFn<UnionValue, KV<KV<String, String>, KV<Long, ChangeRecordT>>> {

            private final Long startCommitTimestampMicros;
            private final Long endCommitTimestampMicros;
            private final List<String> tables;

            private final Converter<UnionValue, ChangeRecordT> changeRecordConverter;
            private final TimestampMicrosGetter commitTimestampMicrosGetter;

            ChangeRecordWithKeyDoFn(final ChangeStreamTransformParameters parameters,
                                    final Converter<UnionValue, ChangeRecordT> changeRecordConverter,
                                    final TimestampMicrosGetter commitTimestampMicrosGetter) {

                this.tables = parameters.getTables();
                this.startCommitTimestampMicros = Optional
                        .ofNullable(parameters.getStartAt())
                        .map(DateTimeUtil::toEpochMicroSecond)
                        .orElse(null);
                this.endCommitTimestampMicros = Optional
                        .ofNullable(parameters.getEndAt())
                        .map(DateTimeUtil::toEpochMicroSecond)
                        .orElse(null);

                this.changeRecordConverter = changeRecordConverter;
                this.commitTimestampMicrosGetter = commitTimestampMicrosGetter;
            }

            @Setup
            public void setup() {

            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue unionValue = c.element();
                final Long commitTimestampMicros = commitTimestampMicrosGetter.getTimestampMicros(unionValue);
                if(commitTimestampMicros == null) {
                    throw new IllegalStateException();
                }
                if(startCommitTimestampMicros != null && commitTimestampMicros <= startCommitTimestampMicros) {
                    return;
                }
                if(endCommitTimestampMicros != null && commitTimestampMicros > endCommitTimestampMicros) {
                    return;
                }

                final List<KV<KV<String, String>, ChangeRecordT>> tableKeyAndValues = changeRecordConverter.convert(unionValue);
                for(final KV<KV<String, String>, ChangeRecordT> tableKeyAndValue : tableKeyAndValues) {
                    if(!this.tables.isEmpty() && !this.tables.contains(tableKeyAndValue.getKey().getKey())) {
                        continue;
                    }
                    c.output(KV.of(tableKeyAndValue.getKey(), KV.of(commitTimestampMicros, tableKeyAndValue.getValue())));
                }
            }
        }

    }
     */


    public static class Restore extends PTransform<PCollectionTuple, PCollection<UnifiedMutation>> {

        private final ChangeStreamTransformParameters parameters;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final Converter<UnionValue, UnifiedMutation> changeRecordConverter;
        private final TimestampMicrosGetter commitTimestampMicrosGetter;
        private final ChangeRecordsAccumulator<UnifiedMutation> accumulator;

        Restore(final ChangeStreamTransformParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final Converter<UnionValue, UnifiedMutation> changeRecordConverter,
                final TimestampMicrosGetter commitTimestampMicrosGetter,
                final ChangeRecordsAccumulator<UnifiedMutation> accumulator) {

            this.parameters = parameters;

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;

            this.changeRecordConverter = changeRecordConverter;
            this.commitTimestampMicrosGetter = commitTimestampMicrosGetter;
            this.accumulator = accumulator;
        }

        @Override
        public PCollection<UnifiedMutation> expand(PCollectionTuple inputs) {
            final Window<UnionValue> window = createWindow(true);

            final Coder<UnifiedMutation> coder = UnifiedMutationCoder.of();

            return inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames))
                    .apply("WithWindow", window)
                    .apply("ToMutationWithKey", ParDo
                            .of(new ChangeRecordWithKeyDoFn(parameters, changeRecordConverter, commitTimestampMicrosGetter)))
                    .setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), coder))
                    .apply("GroupByKey", GroupByKey.create())
                    .apply("Accumulate", ParDo
                            .of(new ChangeRecordsAccumulateDoFn(parameters, accumulator)))
                    .setCoder(coder);
        }

        private static Window<UnionValue> createWindow(final boolean isBounded) {
            if(isBounded) {
                return Window
                        .into(new GlobalWindows());
            } else {
                return Window
                        .into(FixedWindows
                        .of(Duration.standardMinutes(1)));
            }
        }

        private static class ChangeRecordWithKeyDoFn extends DoFn<UnionValue, KV<KV<String, String>, UnifiedMutation>> {

            private final Long startCommitTimestampMicros;
            private final Long endCommitTimestampMicros;
            private final List<String> tables;

            private final Converter<UnionValue, UnifiedMutation> changeRecordConverter;
            private final TimestampMicrosGetter commitTimestampMicrosGetter;

            ChangeRecordWithKeyDoFn(final ChangeStreamTransformParameters parameters,
                                    final Converter<UnionValue, UnifiedMutation> changeRecordConverter,
                                    final TimestampMicrosGetter commitTimestampMicrosGetter) {

                this.tables = parameters.getTables();
                this.startCommitTimestampMicros = Optional
                        .ofNullable(parameters.getStartAt())
                        .map(DateTimeUtil::toEpochMicroSecond)
                        .orElse(null);
                this.endCommitTimestampMicros = Optional
                        .ofNullable(parameters.getEndAt())
                        .map(DateTimeUtil::toEpochMicroSecond)
                        .orElse(null);

                this.changeRecordConverter = changeRecordConverter;
                this.commitTimestampMicrosGetter = commitTimestampMicrosGetter;
            }

            @Setup
            public void setup() {

            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue unionValue = c.element();
                final Long commitTimestampMicros = commitTimestampMicrosGetter.getTimestampMicros(unionValue);
                if(commitTimestampMicros == null) {
                    throw new IllegalStateException("CommitTimestamp must not be null!");
                }
                if(startCommitTimestampMicros != null && commitTimestampMicros <= startCommitTimestampMicros) {
                    LOG.info("out of range startCommitTimestamp: {}", Instant.ofEpochMilli(commitTimestampMicros / 1000L));
                    return;
                }
                if(endCommitTimestampMicros != null && commitTimestampMicros > endCommitTimestampMicros) {
                    LOG.info("out of range endCommitTimestamp: {}", Instant.ofEpochMilli(commitTimestampMicros / 1000L));
                    return;
                }

                final List<KV<KV<String, String>, UnifiedMutation>> tableKeyAndValues = changeRecordConverter.convert(unionValue);
                for(final KV<KV<String, String>, UnifiedMutation> tableKeyAndValue : tableKeyAndValues) {
                    if(!this.tables.isEmpty() && !this.tables.contains(tableKeyAndValue.getKey().getKey())) {
                        continue;
                    }
                    c.output(tableKeyAndValue);
                }
            }
        }

        private static class ChangeRecordsAccumulateDoFn extends DoFn<KV<KV<String, String>, Iterable<UnifiedMutation>>, UnifiedMutation> {

            private final Map<String, String> renameTables;
            private final boolean applyUpsertForInsert;
            private final boolean applyUpsertForUpdate;

            private final ChangeRecordsAccumulator<UnifiedMutation> accumulator;

            ChangeRecordsAccumulateDoFn(final ChangeStreamTransformParameters parameters,
                                        final ChangeRecordsAccumulator<UnifiedMutation> accumulator) {

                this.renameTables = parameters.getRenameTables();
                this.applyUpsertForInsert = parameters.getApplyUpsertForInsert();
                this.applyUpsertForUpdate = parameters.getApplyUpsertForUpdate();
                this.accumulator = accumulator;
            }

            @Setup
            public void setup() {

            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String table = c.element().getKey().getKey();
                final List<UnifiedMutation> elements = Lists.newArrayList(c.element().getValue());
                final List<UnifiedMutation> changeRecords = elements
                        .stream()
                        .sorted(Comparator
                                .comparingLong(UnifiedMutation::getCommitTimestampMicros)
                                .thenComparing(UnifiedMutation::getSequence))
                        .collect(Collectors.toList());

                final long maxCommitTimestampMicros = elements.stream()
                        .map(UnifiedMutation::getCommitTimestampMicros)
                        .filter(Objects::nonNull)
                        .max(Long::compare)
                        .orElse(0L);

                try {
                    final List<UnifiedMutation> mutations = accumulator.accumulate(
                            changeRecords, renameTables, applyUpsertForInsert, applyUpsertForUpdate);
                    for(final UnifiedMutation mutation : mutations) {
                        final UnifiedMutation unifiedMutation = UnifiedMutation.copy(mutation, maxCommitTimestampMicros);//mutationConverter.convert(mutation, table, maxCommitTimestampMicros, 0);
                        c.output(unifiedMutation);
                    }
                } catch (final Throwable e) {
                    final String message = "Failed to accumulate mutations for table: " + table + ", changeRecords: " + changeRecords + ", cause: " + e.getMessage();
                    LOG.error(message);
                    throw new IllegalStateException(message, e);
                }
            }

        }

    }

    private interface Converter<InputT, OutputT> extends Serializable {
        List<KV<KV<String, String>, OutputT>> convert(InputT input);
    }

    private interface TimestampMicrosGetter extends Serializable {
        Long getTimestampMicros(UnionValue input);
    }

    private interface ChangeRecordsAccumulator<MutationT> extends Serializable {
        List<MutationT> accumulate(
                List<MutationT> changeRecords,
                Map<String,String> renameTables,
                boolean applyUpsertForInsert,
                boolean applyUpsertForUpdate);
    }

}
