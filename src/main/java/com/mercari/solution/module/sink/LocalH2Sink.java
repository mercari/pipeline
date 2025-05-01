package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.fileio.H2Sink;
import com.mercari.solution.util.domain.search.H2Util;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Sink.Module(name="localH2")
public class LocalH2Sink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(LocalH2Sink.class);

    private static class Parameters implements Serializable {

        private String output;
        private String input;
        private String database;
        private List<H2Util.Config> configs;
        private Integer batchSize;

        private List<String> groupFields;
        private String tempDirectory;


        public String getOutput() {
            return output;
        }

        public String getInput() {
            return input;
        }

        public String getDatabase() {
            return database;
        }

        public List<H2Util.Config> getConfigs() {
            return configs;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("parameters.output must not be null");
            } else if(!this.output.startsWith("gs://")) {
                errorMessages.add("parameters.input must be gcs path.");
            }
            if(this.input != null) {
                if(!this.input.startsWith("gs://")) {
                    errorMessages.add("parameters.input must be gcs path (must start with gs://).");
                }
            }

            if(configs == null || configs.isEmpty()) {
                errorMessages.add("parameters.configs must not be empty");
            } else {
                for(int i=0; i<configs.size(); i++) {
                    errorMessages.addAll(configs.get(i).validate(i));
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            for(final H2Util.Config config : configs) {
                config.setDefaults();
            }
            if(this.batchSize == null) {
                this.batchSize = 1000;
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", com.mercari.solution.util.pipeline.Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final FileIO.Write<String, MElement> write = ZipFileUtil.createSingleFileWrite(
                parameters.getOutput(),
                parameters.getGroupFields(),
                parameters.getTempDirectory(),
                SchemaUtil.createGroupKeysFunction(MElement::getAsString, parameters.getGroupFields()));
        final WriteFilesResult writeResult = input
                .apply("Write", write.via(H2Sink.of(
                        getName(), parameters.getDatabase(), parameters.getInput(), parameters.getConfigs(), parameters.getBatchSize(), inputs.getAllInputs(), inputs.getAllSchema())));

        final PCollection<KV> files = writeResult.getPerDestinationOutputFilenames();
        final PCollection<MElement> output = files.apply("Convert", ParDo.of(new ElementDoFn()));

        return MCollectionTuple.of(output, Schema.builder().withField("dummy", Schema.FieldType.STRING).build());
    }

    private static class ElementDoFn extends DoFn<KV, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {

        }
    }

}