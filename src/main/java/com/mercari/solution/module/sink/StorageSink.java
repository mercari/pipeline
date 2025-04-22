package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.fileio.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

@Sink.Module(name="storage")
public class StorageSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSink.class);

    private static class Parameters implements Serializable {

        private String output;
        private WriteFormat format;
        private String suffix;
        private String tempDirectory;
        private Integer numShards;
        private Compression compression;
        private CodecName codec;
        private Boolean noSpilling;

        // csv
        private Boolean header;
        private Boolean bom;
        private Boolean outputEmpty;

        // inner use
        private List<String> outputTemplateArgs;


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("parameters.output must not be null");
            }
            if(this.format == null) {
                errorMessages.add("parameters.format must not be null");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(this.suffix == null) {
                this.suffix = "";
            }
            if(this.codec == null) {
                this.codec = CodecName.SNAPPY;
            }
            if(this.noSpilling == null) {
                this.noSpilling = false;
            }
            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }

            // For CSV format
            if(this.header == null) {
                this.header = false;
            }
            if(this.bom == null) {
                this.bom = false;
            }
        }
    }

    private enum WriteFormat {
        csv,
        json,
        avro,
        parquet
    }

    public enum CodecName {
        // common
        SNAPPY,
        ZSTD,
        UNCOMPRESSED,
        // avro only
        BZIP2,
        DEFLATE,
        XZ,
        // parquet only
        LZO,
        LZ4,
        LZ4_RAW,
        BROTLI,
        GZIP
    }


    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        if(hasFailures()) {
            try(final ErrorHandler.BadRecordErrorHandler<?> errorHandler = registerErrorHandler(inputs)) {
                return expand(inputs, errorHandler);
            }
        } else {
            return expand(inputs, null);
        }
    }

    private MCollectionTuple expand(
            final MCollectionTuple inputs,
            final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema = Optional.ofNullable(getSchema()).orElse(inputSchema);

        final String object = getObject(parameters.output);

        final List<String> outputTemplateArgs = Optional
                .ofNullable(parameters.outputTemplateArgs)
                .orElseGet(() -> TemplateUtil.extractTemplateArgs(object, inputSchema));

        final PCollection<KV<String, MElement>> withKey = input
                .apply("WithObjectName", ParDo.of(new ObjectNameDoFn(
                        this.getName(), object, inputSchema, outputTemplateArgs)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));

        final FileIO.Sink<KV<String, MElement>> sink = switch (parameters.format) {
            case csv -> {
                final List<String> fields = outputSchema.getFields().stream()
                        .map(Schema.Field::getName)
                        .toList();
                yield CsvSink.of(outputSchema, fields, parameters.header, null, parameters.bom);
            }
            case json -> JsonSink.of(outputSchema, false, parameters.bom);
            case avro, parquet -> {
                final boolean fitSchema = false;// Optional.ofNullable(schema.getUseDestinationSchema()).orElse(false);
                yield switch (parameters.format) {
                    case avro -> AvroSink.of(outputSchema, parameters.codec, fitSchema);
                    case parquet -> ParquetSink.of(outputSchema, parameters.codec, fitSchema);
                    default -> throw new IllegalArgumentException();
                };
            }
        };

        final String label = switch (parameters.format) {
            case csv -> "WriteCSV";
            case json -> "WriteJSON";
            case avro -> "WriteAvro";
            case parquet -> "WriteParquet";
        };

        final WriteFilesResult writeFilesResult;
        if(TemplateUtil.isTemplateText(object)) {
            final FileIO.Write<String, KV<String, MElement>> write = createDynamicWrite(parameters, errorHandler);
            writeFilesResult = withKey.apply(label + "Dynamic", write.via(sink));
        } else {
            final FileIO.Write<Void, KV<String, MElement>> write = createWrite(parameters, errorHandler);
            writeFilesResult = withKey.apply(label, write.via(sink));
        }

        final PCollection<KV> rows = writeFilesResult.getPerDestinationOutputFilenames();
        PCollection<MElement> outputFiles =  rows
                .apply("Output", ParDo.of(new OutputDoFn(this.getName())))
                .setCoder(ElementCoder.of(OutputDoFn.schema));

        return MCollectionTuple
                .done(PDone.in(inputs.getPipeline()));
    }

    private static FileIO.Write<Void, KV<String, MElement>> createWrite(
            final Parameters parameters,
            final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {

        final String bucket = getBucket(parameters.output);
        final String object = getObject(parameters.output);
        final String suffix = parameters.suffix;
        final Integer numShards = parameters.numShards;

        FileIO.Write<Void, KV<String, MElement>> write = FileIO
                .<KV<String, MElement>>write()
                .to(bucket)
                .withNaming(getFileNaming(object, suffix, numShards));

        if(parameters.numShards == null || parameters.numShards < 1) {
            write = write.withAutoSharding();
        } else {
            write = write.withNumShards(parameters.numShards);
        }

        if(parameters.tempDirectory != null) {
            write = write.withTempDirectory(parameters.tempDirectory);
        }
        if(parameters.compression != null) {
            write = write.withCompression(parameters.compression);
        }
        if(parameters.noSpilling) {
            write = write.withNoSpilling();
        }

        if(errorHandler != null) {
            write = write.withBadRecordErrorHandler(errorHandler);
        }

        return write;
    }

    private static FileIO.Write<String, KV<String, MElement>> createDynamicWrite(
            final Parameters parameters,
            final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {

        final String bucket = getBucket(parameters.output);
        final String suffix = parameters.suffix;
        final Integer numShards = parameters.numShards;

        FileIO.Write<String, KV<String, MElement>> write = FileIO
                .<String, KV<String, MElement>>writeDynamic()
                .to(bucket)
                .by(KV::getKey)
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(key -> getFileNaming(key, suffix, numShards));

        if(parameters.numShards == null || parameters.numShards < 1) {
            write = write.withAutoSharding();
        } else {
            write = write.withNumShards(parameters.numShards);
        }

        if(parameters.tempDirectory != null) {
            write = write.withTempDirectory(parameters.tempDirectory);
        }
        if(parameters.compression != null) {
            write = write.withCompression(parameters.compression);
        }

        if(errorHandler != null) {
            write = write.withBadRecordErrorHandler(errorHandler);
        }

        return write;
    }

    private static class ObjectNameDoFn extends DoFn<MElement, KV<String, MElement>> {

        private final String name;
        private final String path;
        private final Schema inputSchema;
        private final List<String> templateArgs;
        private final boolean useTemplate;

        private transient Template pathTemplate;

        public ObjectNameDoFn(
                final String name,
                final String path,
                final Schema inputSchema,
                final List<String> templateArgs) {

            this.name = name;
            this.path = path;
            this.inputSchema = inputSchema;
            this.templateArgs = templateArgs;
            this.useTemplate = TemplateUtil.isTemplateText(this.path);
        }

        @Setup
        public void setup() {
            if(useTemplate) {
                this.pathTemplate = TemplateUtil.createSafeTemplate("pathTemplate" + name, path);
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            if(useTemplate) {
                final Map<String, Object> values = element.asStandardMap(inputSchema, templateArgs);
                TemplateUtil.setFunctions(values);
                values.put("__timestamp", Instant.ofEpochMilli(c.timestamp().getMillis()));
                final String key = TemplateUtil.executeStrictTemplate(pathTemplate, values);
                c.output(KV.of(key, element));
            } else {
                c.output(KV.of(this.path, element));
            }
        }

    }

    private static class OutputDoFn extends DoFn<KV, MElement> {

        private static final Schema schema = Schema.builder()
                .withField(Schema.Field.of("sink", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("path", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();

        private final String name;

        OutputDoFn(String name) {
            this.name = name;
        }


        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement row = MElement.builder()
                    .withString("sink", this.name)
                    .withString("path", c.element().getValue().toString())
                    .withTimestamp("timestamp", c.timestamp())
                    .build();
            c.output(row);
        }

    }

    private static FileIO.Write.FileNaming getFileNaming(
            final String key,
            final String suffix,
            final Integer numShards) {

        if(TemplateUtil.isTemplateText(suffix)) {
            return new TemplateFileNaming(key, suffix);
        } else if(numShards != null && numShards == 1) {
            return new SingleFileNaming(key, suffix);
        }

        return FileIO.Write.defaultNaming(key, suffix);
    }


    private static class TemplateFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;
        private transient Template suffixTemplate;

        private TemplateFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            if(suffixTemplate == null) {
                this.suffixTemplate = TemplateUtil.createStrictTemplate("TemplateFileNaming", suffix);
            }

            final Map<String,Object> values = new HashMap<>();
            if (window != GlobalWindow.INSTANCE) {
                final IntervalWindow iw = (IntervalWindow)window;
                final Instant start = Instant.ofEpochMilli(iw.start().getMillis());
                final Instant end   = Instant.ofEpochMilli(iw.end().getMillis());
                values.put("windowStart", start);
                values.put("windowEnd",   end);
            }

            values.put("paneIndex", pane.getIndex());
            values.put("paneIsFirst", pane.isFirst());
            values.put("paneIsLast", pane.isLast());
            values.put("paneTiming", pane.getTiming().name());
            values.put("paneIsOnlyFiring", pane.isFirst() && pane.isLast());
            values.put("numShards", numShards);
            values.put("shardIndex", shardIndex);
            values.put("suggestedSuffix", compression.getSuggestedSuffix());

            TemplateUtil.setFunctions(values);

            final String filename = TemplateUtil.executeStrictTemplate(suffixTemplate, values);
            final String fullPath = this.path + filename;
            LOG.info("templateFilename: {}", fullPath);
            return fullPath;
        }

    }

    private static class SingleFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;

        private SingleFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            final String fullPath = this.path + this.suffix;
            return fullPath;
        }

    }

    private static String getBucket(String output) {
        final String prefix = getPrefix(output);
        final String[] paths = output.replaceFirst(prefix, "").split("/", -1);
        return prefix + paths[0] + "/";
    }

    private static String getObject(String output) {
        final String prefix = getPrefix(output);
        final String[] paths = output.replaceFirst(prefix, "").split("/", 2);
        return paths[1];
    }

    private static String getPrefix(final String output) {
        if(output.startsWith("gs://")) {
            return "gs://";
        } else if(output.startsWith("s3://")) {
            return "s3://";
        } else {
            return "";
        }
    }

}
