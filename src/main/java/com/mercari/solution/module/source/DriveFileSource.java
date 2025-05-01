package com.mercari.solution.module.source;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.DriveUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;


@Source.Module(name="drivefile")
public class DriveFileSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(DriveFileSource.class);

    private static final String DEFAULT_FIELDS = "files(id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime),nextPageToken";

    private static class Parameters implements Serializable {

        private String query;
        private String user;
        private String driveId;
        private String folderId;
        private Boolean recursive;
        private String fields;

        private void validate() {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(query == null) {
                errorMessages.add("parameters.query must not be null");
            }

            if(fields != null) {
                if(fields.equals("*")) {
                    errorMessages.add("parameters.fields does not support '*' value");
                } else if(!fields.contains("files(")) {
                    errorMessages.add("parameters.fields must be format such as 'files(id,kind,...)'.");
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final PipelineOptions options) {
            if(user == null) {
                user = DataflowOptions.getServiceAccount(options);
            }
            if(recursive == null) {
                recursive = true;
            }
            if(fields == null) {
                fields = DEFAULT_FIELDS;
            } else if(!fields.contains("nextPageToken")) {
                fields = fields + ",nextPageToken";
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(begin.getPipeline().getOptions());

        final Schema outputSchema = DriveUtil.createFileSchema2(parameters.fields);

        final PCollection<MElement> output = begin
                .apply("InjectQuery", Create.of(parameters.query))
                .apply("ReadDriveFiles", ParDo.of(new DriveFileReadDoFn(parameters, outputSchema)));

        return MCollectionTuple.of(output, outputSchema);
    }

    private static class DriveFileReadDoFn extends DoFn<String, MElement> {

        private final String user;
        private final String driveId;
        private final String folderId;
        protected final String fields;
        private final boolean recursive;
        private final Schema schema;

        private transient Drive service;

        DriveFileReadDoFn(final Parameters parameters, Schema schema) {
            this.user = parameters.user;
            this.driveId = parameters.driveId;
            this.folderId = parameters.folderId;
            this.fields = parameters.fields;
            this.recursive = parameters.recursive;
            this.schema = schema;
        }

        public void setup() {
            this.schema.setup();
            this.service = DriveUtil.drive(user, DriveScopes.DRIVE_READONLY);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws IOException {

            final String query = c.element();
            final List<String> path = new ArrayList<>();
            search(query, folderId, path, c);
        }

        private void search(final String query,
                            final String parentFolderId,
                            final List<String> path,
                            final ProcessContext c) throws IOException {

            final String q;
            if(folderId == null) {
                q = query;
            } else {
                q = "'" + parentFolderId + "' in parents and ((" + query + ") or " + "mimeType='" + DriveUtil.MIMETYPE_APPS_FOLDER + "')";
            }

            Drive.Files.List list = this.service.files().list()
                    .setPageSize(1000)
                    .setQ(q)
                    .setFields(fields);

            if(driveId != null) {
                list = list.setDriveId(driveId);
            }

            FileList fileList = list.execute();
            for(final File file : fileList.getFiles()) {
                if(DriveUtil.isFolder(file)) {
                    if(recursive) {
                        final List<String> childPath = new ArrayList<>(path);
                        childPath.add(file.getName());
                        search(query, file.getId(), childPath, c);
                    }
                } else {
                    final MElement output = createOutput(file, c.timestamp());
                    c.output(output);
                }
            }

            while(fileList.getNextPageToken() != null) {
                fileList = list.setPageToken(fileList.getNextPageToken()).execute();
                for(final File file : fileList.getFiles()) {
                    if(DriveUtil.isFolder(file)) {
                        if(recursive) {
                            final List<String> childPath = new ArrayList<>(path);
                            childPath.add(file.getName());
                            search(query, file.getId(), childPath, c);
                        }
                    } else {
                        final MElement output = createOutput(file, c.timestamp());
                        c.output(output);
                    }
                }
            }

        }

        private MElement createOutput(final File file, final Instant instant) {
            final Map<String, Object> values = DriveUtil.convertPrimitives(schema, file);
            return MElement.of(values, instant);
        }

    }

}
