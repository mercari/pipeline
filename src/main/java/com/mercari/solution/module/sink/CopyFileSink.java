package com.mercari.solution.module.sink;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.aws.S3Util;
import com.mercari.solution.util.gcp.DriveUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


@Sink.Module(name="copyfile")
public class CopyFileSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFileSink.class);

    private static final Counter ERROR_COUNTER = Metrics.counter("copyfile", "error");

    private static class CopyFileSinkParameters implements Serializable {

        private StorageService sourceService;
        private StorageService destinationService;

        private String source;
        private String destination;
        private Map<String, String> attributes;

        private S3Parameters s3;
        private DriveParameters drive;

        private static class DriveParameters implements Serializable {
            private String user;
        }

        private static class S3Parameters implements Serializable {
            private String accessKey;
            private String secretKey;
            private String region;
        }


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(sourceService == null) {
                errorMessages.add("sourceService parameter is required for CopyFileSink config.");
            }
            if(destinationService == null) {
                errorMessages.add("destinationService parameter is required for CopyFileSink config.");
            }
            if(destination == null) {
                errorMessages.add("destination parameter is required for CopyFileSink config.");
            }

            if(StorageService.s3.equals(sourceService) || StorageService.s3.equals(destinationService)) {
                if(s3 == null) {
                    errorMessages.add("s3 parameter is required for CopyFileSink config when using s3.");
                } else {
                    if(s3.accessKey == null) {
                        errorMessages.add("s3.accessKey parameter is required for CopyFileSink config when using s3.");
                    }
                    if(s3.secretKey == null) {
                        errorMessages.add("s3.secretKey parameter is required for CopyFileSink config when using s3.");
                    }
                    if(s3.region == null) {
                        errorMessages.add("s3.region parameter is required for CopyFileSink config when using s3.");
                    }
                }
            }

            if(StorageService.drive.equals(sourceService) || StorageService.drive.equals(destinationService)) {
                if(drive == null) {
                    errorMessages.add("drive parameter is required for CopyFileSink config when using drive.");
                } else {
                    if(drive.user == null) {
                        errorMessages.add("drive.user parameter is required for CopyFileSink config when using drive.");
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        private void setDefaults() {
            if(attributes == null) {
                attributes = new HashMap<>();
            }
        }
    }

    private enum StorageService implements Serializable {
        s3,
        gcs,
        drive,
        field
    }


    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs) {
        final CopyFileSinkParameters parameters = getParameters(CopyFileSinkParameters.class);
        parameters.validate();
        parameters.setDefaults();

        if (inputs.size() == 0) {
            throw new IllegalArgumentException("copyfile sink module requires inputs parameter");
        }

        final Schema inputSchema = Union.createUnionSchema(inputs);

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<MElement> failureTag = new TupleTag<>(){};

        final PCollectionTuple input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("Reshuffle", Reshuffle.viaRandomKey())
                .apply("CopyFile", ParDo
                        .of(new CopyDoFn(getJobName(), getName(), inputSchema, parameters, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        return MCollectionTuple
                .of(input.get(outputTag), createOutputSchema())
                .failure(input.get(failureTag));
    }

    private static class CopyDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final Schema inputSchema;

        private final StorageService sourceService;
        private final StorageService destinationService;

        private final String source;
        private final String destination;
        private final Map<String, String> attributes;

        private final CopyFileSinkParameters.DriveParameters driveParameters;
        private final CopyFileSinkParameters.S3Parameters s3Parameters;

        private final TupleTag<MElement> failureTag;
        private final Boolean failFast;

        private transient Template templateSource;
        private transient Template templateDestination;
        private transient Map<String,Template> templateAttributes;

        private transient S3Client s3;
        private transient Storage storage;
        private transient Drive drive;

        private transient Set<String> templateArgs;

        CopyDoFn(
                final String jobName,
                final String name,
                final Schema inputSchema,
                final CopyFileSinkParameters parameters,
                final Boolean failFast,
                final TupleTag<MElement> failureTag) {

            this.jobName = jobName;
            this.name = name;
            this.inputSchema = inputSchema;
            this.sourceService = parameters.sourceService;
            this.destinationService = parameters.destinationService;

            this.source = parameters.source;
            this.destination = parameters.destination;
            this.attributes = parameters.attributes;

            this.driveParameters = parameters.drive;
            this.s3Parameters = parameters.s3;

            this.failureTag = failureTag;
            this.failFast = failFast;
        }

        @Setup
        public void setup() {

            inputSchema.setup();

            // Setup template engine
            this.templateSource = TemplateUtil.createStrictTemplate("source", source);
            this.templateDestination = TemplateUtil.createStrictTemplate("destination", destination);
            this.templateAttributes = new HashMap<>();
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.templateAttributes.put(entry.getKey(), TemplateUtil.createStrictTemplate(entry.getKey(), entry.getValue()));
            }

            // Setup storage service client
            if(StorageService.gcs.equals(sourceService) || StorageService.gcs.equals(destinationService)) {
                this.storage = StorageUtil.storage();
            }
            if(StorageService.s3.equals(sourceService) || StorageService.s3.equals(destinationService)) {
                this.s3 = S3Util.storage(this.s3Parameters.accessKey, this.s3Parameters.secretKey, this.s3Parameters.region);
            }
            if(StorageService.drive.equals(destinationService)) {
                this.drive = DriveUtil.drive(driveParameters.user, DriveScopes.DRIVE_FILE, DriveScopes.DRIVE_READONLY);
            } else if(StorageService.drive.equals(sourceService)) {
                this.drive = DriveUtil.drive(driveParameters.user, DriveScopes.DRIVE_READONLY);
            }

            // Set template args
            this.templateArgs = new HashSet<>();
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(source, inputSchema));
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(destination, inputSchema));
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema));
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }

            try {
                final Map<String, Object> templateValues = element.asStandardMap(inputSchema, null);
                TemplateUtil.setFunctions(templateValues);
                final String destinationPath = TemplateUtil.executeStrictTemplate(this.templateDestination, templateValues);

                if (StorageService.field.equals(sourceService)) {
                    LOG.info("Copy field value to {}: {}", destinationService, destinationPath);
                    final ByteBuffer bytes = element.getAsBytes(source);
                    if(bytes == null) {
                        return;
                    }
                    switch (destinationService) {
                        case s3 -> writeS3(destinationPath, bytes.array(), templateValues);
                        case gcs -> writeGcs(destinationPath, bytes.array(), templateValues);
                        case drive -> writeDrive(destinationPath, bytes.array(), templateValues);
                    }
                } else {
                    final String sourcePath = TemplateUtil.executeStrictTemplate(this.templateSource, templateValues);
                    LOG.info("Copy file from {}: {} to {}: {}", sourceService, sourcePath, destinationService, destinationPath);
                    switch (sourceService) {
                        case s3 -> {
                            switch (destinationService) {
                                case s3 -> {
                                    final Map<String, Object> attributes = new HashMap<>();
                                    for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                        final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                                        attributes.put(entry.getKey(), value);
                                    }
                                    S3Util.copy(s3, sourcePath, destinationPath, attributes);
                                }
                                case gcs -> {
                                    final byte[] bytes = S3Util.readBytes(s3, sourcePath);
                                    writeGcs(destinationPath, bytes, templateValues);
                                }
                                case drive -> {
                                    final byte[] bytes = S3Util.readBytes(s3, sourcePath);
                                    writeDrive(destinationPath, bytes, templateValues);
                                }
                            }
                        }
                        case gcs -> {
                            switch (destinationService) {
                                case s3 -> {
                                    final byte[] bytes = StorageUtil.readBytes(storage, sourcePath);
                                    writeS3(destinationPath, bytes, templateValues);
                                }
                                case gcs -> {
                                    final Map<String, Object> attributes = new HashMap<>();
                                    for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                        final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                                        attributes.put(entry.getKey(), value);
                                    }
                                    StorageUtil.copy(storage, sourcePath, destinationPath, attributes);
                                }
                                case drive -> {
                                    final byte[] bytes = StorageUtil.readBytes(storage, sourcePath);
                                    writeDrive(destinationPath, bytes, templateValues);
                                }
                            }
                        }
                        case drive -> {
                            switch (destinationService) {
                                case s3 -> {
                                    final byte[] bytes = DriveUtil.download(drive, sourcePath);
                                    writeS3(destinationPath, bytes, templateValues);
                                }
                                case gcs -> {
                                    final byte[] bytes = DriveUtil.download(drive, sourcePath);
                                    writeGcs(destinationPath, bytes, templateValues);
                                }
                                case drive -> {
                                    final Map<String, Object> attributes = new HashMap<>();
                                    for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                        final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                                        attributes.put(entry.getKey(), value);
                                    }
                                    DriveUtil.copy(drive, sourcePath, destinationPath, attributes);
                                }
                            }
                        }
                    }
                }

                //c.output();

            } catch (final Throwable e) {
                ERROR_COUNTER.inc();
                final MFailure failure = MFailure
                        .of(jobName, name, element.toString(), e, c.timestamp());
                final String errorMessage = String.format("Failed to copy file for input: %s, error: %s", failure.getInput(), failure.getError());
                LOG.error(errorMessage);

                if(failFast) {
                    throw new IllegalStateException(errorMessage, e);
                }
                c.output(failureTag, failure.toElement(c.timestamp()));
            }

        }

        private void writeGcs(final String gcsDestinationPath, final byte[] bytes, final Map<String, Object> templateValues) {
            final StorageObject object = new StorageObject();
            final String[] gcsPaths = StorageUtil.parseGcsPath(gcsDestinationPath);
            object.setBucket(gcsPaths[0]);
            object.setName(gcsPaths[1]);
            for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                object.set(entry.getKey(), value);
            }
            if(object.getContentType() == null) {
                object.setContentType("application/octet-stream");
            }
            StorageUtil.writeObject(storage, object, bytes);
        }

        private void writeS3(final String s3DestinationPath, final byte[] bytes, final Map<String, Object> templateValues) {
            final Map<String, Object> attributes = new HashMap<>();
            for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                attributes.put(entry.getKey(), value);
            }
            final String contentType;
            if(templateAttributes.containsKey("contentType")) {
                contentType = TemplateUtil.executeStrictTemplate(templateAttributes.get("contentType"), templateValues);
            } else {
                contentType = "application/octet-stream";
            }
            S3Util.writeBytes(s3, s3DestinationPath, bytes, contentType, attributes, new HashMap<>());
        }

        private void writeDrive(final String parent, final byte[] bytes, final Map<String, Object> record) {
            final File file = new File();
            file.setParents(Arrays.asList(parent));
            for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), record);
                file.set(entry.getKey(), value);
            }
            if(file.getMimeType() == null) {
                file.setMimeType("application/octet-stream");
            }
            DriveUtil.createFile(drive, file, bytes);
        }

    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("source", Schema.FieldType.STRING.withNullable(true))
                .withField("destination", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

}