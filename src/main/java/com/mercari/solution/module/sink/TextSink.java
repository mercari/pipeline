package com.mercari.solution.module.sink;

import com.google.api.services.storage.Storage;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.aws.S3Util;
import com.mercari.solution.util.schema.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TextSink {

    private static final Logger LOG = LoggerFactory.getLogger(TextSink.class);

    private static class TextSinkParameters implements Serializable {

        private String output;
        private String template;
        private String contentType;
        private String charset;
        private Boolean bom;

        // GCS storage object fields
        private String cacheControl;
        private String contentDisposition;
        private String contentEncoding;
        private String contentLanguage;
        private String customTimeField;
        private Map<String, String> metadata;

        // AWS S3 fields
        private String s3AccessKey;
        private String s3SecretKey;
        private String s3Region;

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getTemplate() {
            return template;
        }

        public void setTemplate(String template) {
            this.template = template;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = charset;
        }

        public Boolean getBom() {
            return bom;
        }

        public void setBom(Boolean bom) {
            this.bom = bom;
        }

        public String getCacheControl() {
            return cacheControl;
        }

        public String getContentDisposition() {
            return contentDisposition;
        }

        public void setContentDisposition(String contentDisposition) {
            this.contentDisposition = contentDisposition;
        }

        public void setCacheControl(String cacheControl) {
            this.cacheControl = cacheControl;
        }

        public String getContentEncoding() {
            return contentEncoding;
        }

        public void setContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
        }

        public String getContentLanguage() {
            return contentLanguage;
        }

        public void setContentLanguage(String contentLanguage) {
            this.contentLanguage = contentLanguage;
        }

        public String getCustomTimeField() {
            return customTimeField;
        }

        public void setCustomTimeField(String customTimeField) {
            this.customTimeField = customTimeField;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
        }

        public String getS3AccessKey() {
            return s3AccessKey;
        }

        public String getS3SecretKey() {
            return s3SecretKey;
        }

        public String getS3Region() {
            return s3Region;
        }
    }

    public String getName() { return "text"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("text sink module requires input parameter");
        }
        final FCollection<?> input = inputs.get(0);
        return Collections.singletonMap(config.getName(), TextSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final TextSinkParameters parameters = new Gson().fromJson(config.getParameters(), TextSinkParameters.class);
        final TextWrite write = new TextWrite(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        final FCollection<?> fcollection = FCollection.update(collection, output);
        /*
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

         */
        return fcollection;
    }

    public static class TextWrite extends PTransform<PCollection<?>, PCollection<String>> {

        private FCollection<?> collection;
        private final TextSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private TextWrite(final FCollection<?> collection,
                          final TextSinkParameters parameters,
                          final List<FCollection<?>> waits) {
            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<String> expand(final PCollection<?> inputP) {

            boolean isIntervalWindow = inputP.getWindowingStrategy()
                    .getWindowFn()
                    .getWindowTypeDescriptor()
                    .getType()
                    .getTypeName()
                    .endsWith("IntervalWindow");

            validateParameters(isIntervalWindow);
            setDefaultParameters();

            final PCollection<?> input;
            if(waits == null || waits.size() == 0) {
                input = inputP;
            } else {
                final List<PCollection<?>> wait = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = inputP
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder)inputP.getCoder());
            }

            final String template;
            if(parameters.getTemplate().startsWith("gs://")) {
                template = StorageUtil.readString(parameters.getTemplate());
            } else if(parameters.getTemplate().startsWith("s3://")) {
                template = S3Util.readString(parameters.getTemplate(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region());
            } else {
                throw new IllegalArgumentException("TextSink module parameter `template` must be GCS path.");
            }

            switch (collection.getDataType()) {
                case AVRO -> {
                    final RecordFormatter<GenericRecord> formatter = AvroToMapConverter::convert;
                    return input
                            .apply("WriteText", ParDo.of(new TemplateDoFn<>(
                                    parameters.getOutput(), template,
                                    parameters.getContentType(), parameters.getCharset(), parameters.getBom(),
                                    parameters.getCacheControl(), parameters.getContentDisposition(),
                                    parameters.getContentEncoding(), parameters.getContentLanguage(), parameters.getCustomTimeField(),
                                    parameters.getMetadata(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region(),
                                    isIntervalWindow,
                                    formatter)));
                }
                case ROW -> {
                    final RecordFormatter<Row> formatter = RowToMapConverter::convert;
                    return input
                            .apply("WriteText", ParDo.of(new TemplateDoFn<>(
                                    parameters.getOutput(), template,
                                    parameters.getContentType(), parameters.getCharset(), parameters.getBom(),
                                    parameters.getCacheControl(), parameters.getContentDisposition(),
                                    parameters.getContentEncoding(), parameters.getContentLanguage(), parameters.getCustomTimeField(),
                                    parameters.getMetadata(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region(),
                                    isIntervalWindow,
                                    formatter)));
                }
                case STRUCT -> {
                    final RecordFormatter<Struct> formatter = StructToMapConverter::convert;
                    return input
                            .apply("WriteText", ParDo.of(new TemplateDoFn<>(
                                    parameters.getOutput(), template,
                                    parameters.getContentType(), parameters.getCharset(), parameters.getBom(),
                                    parameters.getCacheControl(), parameters.getContentDisposition(),
                                    parameters.getContentEncoding(), parameters.getContentLanguage(), parameters.getCustomTimeField(),
                                    parameters.getMetadata(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region(),
                                    isIntervalWindow,
                                    formatter)));
                }
                case DOCUMENT -> {
                    final RecordFormatter<Document> formatter = DocumentToMapConverter::convert;
                    return input
                            .apply("WriteText", ParDo.of(new TemplateDoFn<>(
                                    parameters.getOutput(), template,
                                    parameters.getContentType(), parameters.getCharset(), parameters.getBom(),
                                    parameters.getCacheControl(), parameters.getContentDisposition(),
                                    parameters.getContentEncoding(), parameters.getContentLanguage(), parameters.getCustomTimeField(),
                                    parameters.getMetadata(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region(),
                                    isIntervalWindow,
                                    formatter)));
                }
                case ENTITY -> {
                    final RecordFormatter<Entity> formatter = EntityToMapConverter::convert;
                    return input
                            .apply("WriteText", ParDo.of(new TemplateDoFn<>(
                                    parameters.getOutput(), template,
                                    parameters.getContentType(), parameters.getCharset(), parameters.getBom(),
                                    parameters.getCacheControl(), parameters.getContentDisposition(),
                                    parameters.getContentEncoding(), parameters.getContentLanguage(), parameters.getCustomTimeField(),
                                    parameters.getMetadata(), parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region(),
                                    isIntervalWindow,
                                    formatter)));
                }
                default -> throw new IllegalArgumentException("Not supported csv input type: " + collection.getDataType());
            }
        }

        private void validateParameters(boolean inIntervalWindow) {
            final String output = parameters.getOutput();
            if(inIntervalWindow) {
                if(!output.contains("__WINDOW_START__") || !output.contains("__WINDOW_END__")) {
                    throw new IllegalArgumentException("TextSink in IntervalWindow output must be set variables '__WINDOW_START__' and '__WINDOW_END__'");
                }
            }
        }

        private void setDefaultParameters() {
            if(parameters.getBom() == null) {
                this.parameters.setBom(false);
            }
            if(parameters.getContentType() == null) {
                parameters.setContentType("application/octet-stream");
            }
            if(parameters.getCharset() == null) {
                parameters.setCharset(StandardCharsets.UTF_8.name());
            }
            if(parameters.getMetadata() == null) {
                parameters.setMetadata(new HashMap<>());
            }
        }

    }

    public static class TemplateDoFn<InputT> extends DoFn<InputT, String> {

        private final String templatePathString;
        private final String templateTextString;
        private final String contentType;
        private final String charset;
        private final RecordFormatter<InputT> formatter;
        private final Boolean bom;

        private final String cacheControl;
        private final String contentDisposition;
        private final String contentEncoding;
        private final String contentLanguage;
        private final String customTimeField;
        private final Map<String, String> metadata;
        private final String s3AccessKey;
        private final String s3SecretKey;
        private final String s3Region;

        private final boolean inIntervalWindow;

        private transient Template templatePath;
        private transient Template templateText;
        private transient Charset _charset;

        private transient Storage storage;
        private transient S3Client s3;

        private TemplateDoFn(final String templatePathString,
                             final String templateTextString,
                             final String contentType,
                             final String charset,
                             final Boolean bom,
                             final String cacheControl,
                             final String contentDisposition,
                             final String contentEncoding,
                             final String contentLanguage,
                             final String customTimeField,
                             final Map<String, String> metadata,
                             final String s3AccessKey,
                             final String s3SecretKey,
                             final String s3Region,
                             final boolean inIntervalWindow,
                             final RecordFormatter formatter) {

            this.templatePathString = templatePathString;
            this.templateTextString = templateTextString;
            this.contentType = contentType;
            this.charset = charset;
            this.bom = bom;

            this.cacheControl = cacheControl;
            this.contentDisposition = contentDisposition;
            this.contentEncoding = contentEncoding;
            this.contentLanguage = contentLanguage;
            this.customTimeField = customTimeField;
            this.metadata = metadata;
            this.s3AccessKey = s3AccessKey;
            this.s3SecretKey = s3SecretKey;
            this.s3Region = s3Region;

            this.inIntervalWindow = inIntervalWindow;
            this.formatter = formatter;
        }

        @Setup
        public void setup() throws IOException {
            this.templatePath = TemplateUtil.createStrictTemplate("textPath", templatePathString);
            this.templateText = TemplateUtil.createStrictTemplate("textContent", templateTextString);
            this._charset = getCharset(charset);
        }

        @StartBundle
        public void startBundle(StartBundleContext c) {
            if(this.templatePathString.startsWith("gs://")) {
                this.storage = StorageUtil.storage();
                this.s3 = null;
            } else if(this.templatePathString.startsWith("s3://")) {
                this.storage = null;
                this.s3 = S3Util.storage(s3AccessKey, s3SecretKey, s3Region);
            } else {
                this.storage = StorageUtil.storage();
                //this.s3 = S3Util.storage(s3AccessKey, s3SecretKey, s3Region);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow bw) throws Exception {

            final Map<String, Object> map = this.formatter.formatMap(c.element());
            if(inIntervalWindow) {
                map.putAll(createContextParams(bw, c.pane()));
            }
            map.put("_CSVPrinter", new MapToCsvConverter());

            try(final StringWriter pathWriter = new StringWriter();
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                final OutputStreamWriter osw = new OutputStreamWriter(bos, _charset)) {

                // path
                templatePath.process(map, pathWriter);
                final String path = pathWriter.toString();

                // content
                //// BOM(UTF)
                if(this.bom) {
                    if(_charset.equals(StandardCharsets.UTF_8)) {
                        bos.write(0xEF);
                        bos.write(0xBB);
                        bos.write(0xBF);
                    } else if(_charset.equals(StandardCharsets.UTF_16)) {
                        bos.write(0xFF);
                        bos.write(0xFE);
                    }
                }

                templateText.process(map, osw);
                osw.flush();

                final byte[] bytes = bos.toByteArray();

                final Map<String, Object> fields = new HashMap<>();
                fields.put("size", BigInteger.valueOf(bytes.length));
                if(cacheControl != null) {
                    fields.put("cacheControl", cacheControl);
                }
                if(contentDisposition != null) {
                    fields.put("contentDisposition", contentDisposition);
                }
                if(contentEncoding != null) {
                    fields.put("contentEncoding", contentEncoding);
                }
                if(contentLanguage != null) {
                    fields.put("contentLanguage", contentLanguage);
                }
                if(customTimeField != null) {
                    if(map.containsKey(customTimeField)) {
                        final Object customTime = map.get(customTimeField);
                        if(customTime instanceof Instant) {
                            fields.put("customTime", customTime.toString());
                        } else {
                            LOG.warn("Specified customTimeField: " + customTimeField + " value is not Instant but "
                                    + customTimeField.getClass().getSimpleName());
                        }
                    } else {
                        LOG.warn("Specified customTimeField: " + customTimeField + " value is missing.");
                    }
                }

                if(path.startsWith("gs://")) {
                    StorageUtil.writeBytes(storage, path, bytes, contentType, fields, metadata);
                } else if(path.startsWith("s3://")) {
                    S3Util.writeBytes(s3, path, bytes, contentType, fields, metadata);
                }

                final JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("path", path);

                c.output(jsonObject.toString());
            }
        }

        private Charset getCharset(final String charset) {
            switch (charset.trim().toLowerCase()) {
                case "utf8":
                case "utf-8":
                    return StandardCharsets.UTF_8;
                case "ascii":
                    return StandardCharsets.US_ASCII;
                case "iso":
                case "iso-8859":
                case "iso-8859-1":
                    return StandardCharsets.ISO_8859_1;
                case "utf16":
                case "utf-16":
                    return StandardCharsets.UTF_16;
                case "utf16be":
                case "utf-16-be":
                    return StandardCharsets.UTF_16BE;
                case "utf16le":
                case "utf-16-le":
                    return StandardCharsets.UTF_16LE;
                default:
                    return Charset.forName(charset);
            }
        }

        public static Map<String, Object> createContextParams(final BoundedWindow window,
                                                              final PaneInfo pane) {

            final Map<String, Object> map = new HashMap<>();

            // Window Path
            if(window != GlobalWindow.INSTANCE) {
                if(!(window instanceof IntervalWindow)) {
                    throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                            + window.getClass().getSimpleName());
                }
                final IntervalWindow iw = ((IntervalWindow)window);
                map.put("__WINDOW_START__", iw.start());
                map.put("__WINDOW_END__", iw.end());
            } else {
                map.put("__WINDOW_START__", Instant.ofEpochSecond(0L));
                map.put("__WINDOW_END__", Instant.ofEpochSecond(4102444800L));
            }

            // Pane Path
            if(!pane.isFirst() || !pane.isLast()) {
                map.put("__PANE_INDEX__", pane.getIndex());
            }

            return map;
        }


    }

    public interface RecordFormatter<ElementT> extends Serializable {
        Map<String, Object> formatMap(ElementT element);
    }

}
