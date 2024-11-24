package com.mercari.solution.module.transform;

import com.google.api.services.storage.Storage;
import com.google.gson.JsonArray;
import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.text.HtmlUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;


@Transform.Module(name="pdfextract")
public class PDFExtractTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(PDFExtractTransform.class);

    private static final String FIELD_NAME_CONTENT = "Content";
    private static final String FIELD_NAME_FILESIZE = "FileByteSize";
    private static final String FIELD_NAME_PAGE = "Page";
    private static final String FIELD_NAME_VERSION = "Version";
    private static final String FIELD_NAME_ENCRYPTED = "Encrypted";
    private static final String FIELD_NAME_TITLE = "Title";
    private static final String FIELD_NAME_AUTHOR = "Author";
    private static final String FIELD_NAME_SUBJECT = "Subject";
    private static final String FIELD_NAME_KEYWORDS = "Keywords";
    private static final String FIELD_NAME_CREATOR = "Creator";
    private static final String FIELD_NAME_PRODUCER = "Producer";
    private static final String FIELD_NAME_CREATIONDATE = "CreationDate";
    private static final String FIELD_NAME_MODIFICATIONDATE = "ModificationDate";
    private static final String FIELD_NAME_TRAPPED = "Trapped";
    private static final String FIELD_NAME_FAILED = "Failed";
    private static final String FIELD_NAME_ERROR_PAGE = "ErrorPageCount";
    private static final String FIELD_NAME_ERROR_MESSAGE = "ErrorMessage";

    private static class PDFExtractTransformParameters implements Serializable {

        private String field;
        private String prefix;
        private JsonArray select;

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }

        private void validate() {
            if(field == null) {
                throw new IllegalModuleException("parameters.field must not be null");
            }
        }

        private void setDefaults() {
            if(this.prefix == null) {
                this.prefix = "";
            }
        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final PDFExtractTransformParameters parameters = getParameters(PDFExtractTransformParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final Schema inputSchema = Union.createUnionSchema(inputs);
        Schema outputSchema = createPdfSchema(parameters.prefix);
        outputSchema = Schema.builder(outputSchema).withFields(inputSchema.getFields()).build();

        final boolean isContentFieldString = Schema.Type.string.equals(inputSchema.getField(parameters.field).getFieldType().getType());

        PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        if(!OptionUtil.isStreaming(input)) {
            input = input
                    .apply("Reshuffle", Reshuffle.viaRandomKey());
        }

        final List<SelectFunction> selectFunctions;
        if(parameters.useSelect()) {
            selectFunctions = SelectFunction.of(parameters.select, outputSchema.getFields());
            outputSchema = SelectFunction.createSchema(selectFunctions);
        } else {
            selectFunctions = new ArrayList<>();
        }

        final PCollection<MElement> extracted = input
                .apply(ParDo.of(new PDFExtractDoFn(parameters.field, parameters.prefix, selectFunctions, isContentFieldString)));

        return MCollectionTuple
                .of(extracted, outputSchema);
    }

    private static Schema createPdfSchema(String prefix) {
        return Schema.builder()
                .withField(prefix + FIELD_NAME_CONTENT, Schema.FieldType.STRING.withNullable(false))
                .withField(prefix + FIELD_NAME_FILESIZE, Schema.FieldType.INT64.withNullable(true))
                .withField(prefix + FIELD_NAME_PAGE, Schema.FieldType.INT64.withNullable(true))
                .withField(prefix + FIELD_NAME_VERSION, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_ENCRYPTED, Schema.FieldType.BOOLEAN.withNullable(true))
                .withField(prefix + FIELD_NAME_TITLE, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_AUTHOR, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_SUBJECT, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_KEYWORDS, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_CREATOR, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_PRODUCER, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_CREATIONDATE, Schema.FieldType.TIMESTAMP.withNullable(true))
                .withField(prefix + FIELD_NAME_MODIFICATIONDATE, Schema.FieldType.TIMESTAMP.withNullable(true))
                .withField(prefix + FIELD_NAME_TRAPPED, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_FAILED, Schema.FieldType.BOOLEAN.withNullable(false))
                .withField(prefix + FIELD_NAME_ERROR_PAGE, Schema.FieldType.INT64.withNullable(false))
                .withField(prefix + FIELD_NAME_ERROR_MESSAGE, Schema.FieldType.STRING.withNullable(true))
                .build();
    }

    private static class PDFExtractDoFn extends DoFn<MElement, MElement> {

        private final String field;
        private final String prefix;
        private final List<SelectFunction> selectFunctions;
        private final boolean isContentFieldString;

        private transient PDFTextStripper stripper;
        private transient Storage storage;

        PDFExtractDoFn(final String field,
                       final String prefix,
                       final List<SelectFunction> selectFunctions,
                       final boolean isContentFieldString) {

            this.field = field;
            this.prefix = prefix;
            this.selectFunctions = selectFunctions;
            this.isContentFieldString = isContentFieldString;
        }

        @Setup
        public void setup() throws IOException {
            this.stripper = new PDFTextStripper();
            if(isContentFieldString) {
                this.storage = StorageUtil.storage();
            }
            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            final byte[] bytes;
            String errorMessage = null;
            if(isContentFieldString) {
                String stringFieldValue = input.getAsString(field);
                if(stringFieldValue == null) {
                    errorMessage = "pdf content field: " + field + " value is null";
                    LOG.warn(errorMessage);
                    bytes = null;
                } else {
                    if(stringFieldValue.startsWith("/gs/")) { // modify app engine style gcs path
                        stringFieldValue = stringFieldValue.replaceFirst("/gs/", "gs://");
                    }
                    if(stringFieldValue.startsWith("gs://")) {
                        LOG.info("Read pdf content from gcs path: {}", stringFieldValue);
                        bytes = StorageUtil.readBytes(storage, stringFieldValue);
                    } else if(stringFieldValue.startsWith("https://") || stringFieldValue.startsWith("http://")) {
                        LOG.info("Read pdf content from url: {}", stringFieldValue);
                        bytes = null;
                    } else {
                        errorMessage = "Not supported pdf content uri: " + stringFieldValue;
                        LOG.warn(errorMessage);
                        bytes = null;
                    }
                }
            } else {
                bytes = Optional.ofNullable(input.getAsBytes(field)).map(ByteBuffer::array).orElse(null);
                if(bytes == null) {
                    errorMessage = "PDF content field: " + field + " value is null";
                }
            }

            final Map<String, Object> pdfContent = extractPDF(bytes);
            if(errorMessage != null) {
                pdfContent.put(prefix + FIELD_NAME_ERROR_MESSAGE, errorMessage);
            }

            pdfContent.putAll(input.asPrimitiveMap());

            final MElement output;
            if(selectFunctions.isEmpty()) {
                output = MElement.of(pdfContent, c.timestamp());
            } else {
                final Map<String, Object> selectedValues = SelectFunction.apply(selectFunctions, pdfContent, c.timestamp());
                output = MElement.of(selectedValues, c.timestamp());
            }

            c.output(output);
        }

        private Map<String, Object> extractPDF(final byte[] bytes) {
            if(bytes == null) {
                return createEmpty(bytes, "content is null");
            }

            final Map<String, Object> values = new HashMap<>();
            final List<String> pageErrorMessages = new ArrayList<>();
            try(final PDDocument document = Loader.loadPDF(bytes)) {
                final List<String> textContents = new ArrayList<>();
                int pageCount = document.getPages().getCount();
                long errorPageCount = 0;
                for (int page = 0; page <= pageCount; page++) {
                    stripper.setStartPage(page);
                    stripper.setEndPage(page);
                    stripper.setSortByPosition(true);
                    stripper.setLineSeparator("");
                    stripper.setAddMoreFormatting(true);
                    stripper.setSuppressDuplicateOverlappingText(true);
                    stripper.setShouldSeparateByBeads(true);
                    try {
                        final String text = stripper.getText(document);
                        textContents.add(text);
                    } catch (Exception e) {
                        final String errorMessage = "page: " + page + ", error: " + e.getMessage();
                        pageErrorMessages.add(errorMessage);
                        LOG.error(errorMessage);
                        textContents.add(" ");
                        errorPageCount += 1;
                    }
                }

                final String content = String.join("", textContents);

                values.put(prefix + FIELD_NAME_CONTENT, content);
                values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
                values.put(prefix + FIELD_NAME_PAGE, Integer.valueOf(document.getNumberOfPages()).longValue());
                values.put(prefix + FIELD_NAME_VERSION, Float.valueOf(document.getVersion()).toString());
                values.put(prefix + FIELD_NAME_ENCRYPTED, document.isEncrypted());
                values.put(prefix + FIELD_NAME_TITLE, document.getDocumentInformation().getTitle());
                values.put(prefix + FIELD_NAME_AUTHOR, document.getDocumentInformation().getAuthor());
                values.put(prefix + FIELD_NAME_SUBJECT, document.getDocumentInformation().getSubject());
                values.put(prefix + FIELD_NAME_KEYWORDS, document.getDocumentInformation().getKeywords());
                values.put(prefix + FIELD_NAME_CREATOR, document.getDocumentInformation().getCreator());
                values.put(prefix + FIELD_NAME_PRODUCER, document.getDocumentInformation().getProducer());

                final Calendar creationDate = document.getDocumentInformation().getCreationDate();
                if(creationDate != null) {
                    values.put(prefix + FIELD_NAME_CREATIONDATE, creationDate.toInstant().toEpochMilli() * 1000L);
                } else  {
                    values.put(prefix + FIELD_NAME_CREATIONDATE, null);
                }

                final Calendar modificationDate = document.getDocumentInformation().getModificationDate();
                if(modificationDate != null) {
                    values.put(prefix + FIELD_NAME_MODIFICATIONDATE, modificationDate.toInstant().toEpochMilli() * 1000L);
                } else  {
                    values.put(prefix + FIELD_NAME_MODIFICATIONDATE, null);
                }

                values.put(prefix + FIELD_NAME_TRAPPED, document.getDocumentInformation().getTrapped());
                values.put(prefix + FIELD_NAME_FAILED, false);
                values.put(prefix + FIELD_NAME_ERROR_PAGE, errorPageCount);

                if(!pageErrorMessages.isEmpty()) {
                    values.put(prefix + FIELD_NAME_ERROR_MESSAGE, String.join(", ", pageErrorMessages));
                }

                return values;
            } catch (final Exception e) {
                if(HtmlUtil.isZip(bytes)) {
                    try {
                        HtmlUtil.EPUBDocument document = HtmlUtil.readEPUB(bytes);
                        values.put(prefix + FIELD_NAME_CONTENT, document.getContent());
                        values.put(prefix + FIELD_NAME_PAGE, document.getPage());
                        values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
                        values.put(prefix + FIELD_NAME_FAILED, false);
                        values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
                        values.put(prefix + FIELD_NAME_ERROR_MESSAGE, e.getMessage());
                        return values;
                    } catch (Exception ee) {
                        LOG.error("Failed to parse epub cause: {}", ee.getMessage());
                        return createEmpty(bytes, ee.getMessage());
                    }
                } else {
                    LOG.error("Failed to parse pdf cause: {}", e.getMessage());
                    return createEmpty(bytes, e.getMessage());
                }
            }
        }

        private Map<String, Object> createEmpty(byte[] bytes, String message) {
            final Map<String, Object> values = new HashMap<>();
            values.put(prefix + FIELD_NAME_CONTENT, "");
            values.put(prefix + FIELD_NAME_PAGE, 0L);
            values.put(prefix + FIELD_NAME_FILESIZE, bytes == null ? 0L : Integer.valueOf(bytes.length).longValue());
            values.put(prefix + FIELD_NAME_FAILED, true);
            values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
            values.put(prefix + FIELD_NAME_ERROR_MESSAGE, message);
            return values;
        }

    }

}
