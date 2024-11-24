package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.Filter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Source.Module(name="files")
public class FilesSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(FilesSource.class);

    private static class FilesSourceParameters implements Serializable {

        private String pattern;
        private List<String> patterns;
        private ContinuouslyParameters continuously;
        private EmptyMatchTreatment emptyMatchTreatment;
        private JsonElement filter;
        private Boolean withContent;

        private void validate() {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(pattern == null && (patterns == null || patterns.isEmpty())) {
                errorMessages.add("parameters.pattern must not be null");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(patterns == null) {
                patterns = new ArrayList<>();
            }
            if(patterns.isEmpty() && pattern != null) {
                patterns.add(pattern);
            }
            if(emptyMatchTreatment == null) {
                emptyMatchTreatment = EmptyMatchTreatment.ALLOW;
            }
            if(withContent == null) {
                withContent = false;
            }
        }

        private static class ContinuouslyParameters implements Serializable {

            private Duration interval;
            private Boolean matchUpdatedFiles;

        }
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {

        final FilesSourceParameters parameters = getParameters(FilesSourceParameters.class);
        parameters.validate();
        parameters.setDefaults();

        begin.getPipeline().getCoderRegistry().registerCoderForClass(MatchResult.Metadata.class, MetadataCoderV2.of());

        final PCollection<MatchResult.Metadata> metadata;
        if(parameters.patterns.size() > 1) {
            PCollectionList<MatchResult.Metadata> list = PCollectionList.empty(begin.getPipeline());
            int i = 0;
            for(final String pattern : parameters.patterns) {
                final PCollection<MatchResult.Metadata> metadata_ = begin
                        .apply("MatchFiles" + i, FileIO
                                .match()
                                .filepattern(pattern)
                                .withEmptyMatchTreatment(parameters.emptyMatchTreatment));
                list = list.and(metadata_);
                i++;
            }
            metadata = list.apply("Flatten", Flatten.pCollections());
        } else {
            metadata = begin
                    .apply("MatchFiles", FileIO
                            .match()
                            .filepattern(parameters.patterns.getFirst())
                            .withEmptyMatchTreatment(parameters.emptyMatchTreatment));
        }

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<MElement> failureTag = new TupleTag<>(){};

        final String filterText = Optional
                .ofNullable(parameters.filter)
                .map(Object::toString)
                .orElse(null);

        final Schema outputSchema = createFileSchema();
        final PCollectionTuple outputs;
        if(parameters.withContent) {
            outputs = metadata
                    .apply("Filter", ParDo.of(new FilterDoFn(filterText)))
                    .apply("ReadMatches", FileIO.readMatches())
                    .apply("Format", ParDo
                            .of(new FileWithStorageDoFn(getJobName(), getName(), getFailFast(), failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            outputs = metadata
                    .apply("Format", ParDo
                            .of(new FileDoFn(filterText, getFailFast(), failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema)
                .failure(outputs.get(failureTag));
    }

    private static class FileDoFn extends DoFn<MatchResult.Metadata, MElement> {

        private final Schema schema = createFileSchema();
        private final String filter;
        private final Boolean failFast;
        private final TupleTag<MElement> failureTag;

        private transient Filter.ConditionNode conditionNode;

        FileDoFn(
                final String filterText,
                final Boolean failFast,
                final TupleTag<MElement> failureTag) {

            this.filter = filterText;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            if(filter != null) {
                this.conditionNode = Filter.parse(filter);
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MatchResult.Metadata metadata = c.element();
            if(metadata == null) {
                return;
            }
            final MElement file = builder(metadata).build();
            if(conditionNode == null || Filter.filter(conditionNode, schema, file)) {
                c.output(file);
            }
        }

    }

    private static class FilterDoFn extends DoFn<MatchResult.Metadata, MatchResult.Metadata> {

        private final Schema schema = createFileSchema();
        private final String filter;
        private transient Filter.ConditionNode conditionNode;

        FilterDoFn(final String filterText) {
            this.filter = filterText;
        }

        @Setup
        public void setup() {
            if(filter != null) {
                this.conditionNode = Filter.parse(filter);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MatchResult.Metadata metadata = c.element();
            if(metadata == null) {
                return;
            }
            if(filter == null) {
                c.output(metadata);
                return;
            }
            final MElement file = builder(metadata).build();
            if(Filter.filter(conditionNode, schema, file)) {
                c.output(metadata);
            }
        }

    }

    private static class FileWithStorageDoFn extends DoFn<FileIO.ReadableFile, MElement> {

        private final String jobName;
        private final String module;
        private final Boolean failFast;
        private final TupleTag<MElement> failureTag;

        FileWithStorageDoFn(
                final String jobName,
                final String module,
                final Boolean failFast,
                final TupleTag<MElement> failureTag) {

            this.jobName = jobName;
            this.module = module;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FileIO.ReadableFile readableFile = c.element();
            if(readableFile == null) {
                return;
            }

            final MatchResult.Metadata metadata = readableFile.getMetadata();
            try {
                final MElement file = builder(metadata)
                        .withBytes("content", readableFile.readFullyAsBytes())
                        .withString("compression", readableFile.getCompression().name())
                        .build();
                c.output(file);
            } catch (final Throwable e) {
                if(failFast) {
                    throw new RuntimeException(e);
                }
                final MFailure failure = MFailure.of(jobName, module, readableFile.toString(), e, c.timestamp());
                c.output(failure.toElement(c.timestamp()));
            }

        }

    }

    private static MElement.Builder builder(final MatchResult.Metadata metadata) {
        return MElement.builder()
                .withString("checksum", metadata.checksum())
                .withInt64("sizeBytes", metadata.sizeBytes())
                .withBool("isReadSeekEfficient", metadata.isReadSeekEfficient())
                .withTimestamp("lastModified", Instant.ofEpochMilli(metadata.lastModifiedMillis()))
                .withString("filename", metadata.resourceId().getFilename())
                .withString("directory", metadata.resourceId().getCurrentDirectory().getFilename())
                .withString("resource", metadata.resourceId().toString())
                .withBool("isDirectory", metadata.resourceId().isDirectory())
                .withString("schema", metadata.resourceId().getScheme());
    }

    private static Schema createFileSchema() {
        return Schema.builder()
                .withField("filename", Schema.FieldType.STRING.withNullable(true))
                .withField("directory", Schema.FieldType.STRING.withNullable(true))
                .withField("resource", Schema.FieldType.STRING.withNullable(true))
                .withField("sizeBytes", Schema.FieldType.INT64.withNullable(true))
                .withField("isDirectory", Schema.FieldType.BOOLEAN.withNullable(true))
                .withField("lastModified", Schema.FieldType.TIMESTAMP.withNullable(true))
                .withField("schema", Schema.FieldType.STRING.withNullable(true))
                .withField("isReadSeekEfficient", Schema.FieldType.BOOLEAN.withNullable(true))
                .withField("checksum", Schema.FieldType.STRING.withNullable(true))
                .withField("content", Schema.FieldType.BYTES.withNullable(true))
                .withField("compression", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

}
