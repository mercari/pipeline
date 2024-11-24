package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.sink.fileio.H2Sink;
import com.mercari.solution.util.domain.search.H2Util;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalH2Sink {

    private static final Logger LOG = LoggerFactory.getLogger(LocalH2Sink.class);

    private static class LocalH2SinkParameters implements Serializable {

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


    public String getName() { return "localH2"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {

        if(inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("localH2 sink module requires inputs");
        }

        final LocalH2SinkParameters parameters = new Gson().fromJson(config.getParameters(), LocalH2SinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("localH2 sink parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final List<String> inputSchemas = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.add(input.getAvroSchema().toString());

            tuple = tuple.and(tag, input.getCollection());
        }

        final PCollection output = tuple.apply(config.getName(), new H2Write(config.getName(), parameters, tags, inputNames, inputTypes, inputSchemas, waits));
        final FCollection<?> fcollection = FCollection.of(config.getName(), output, DataType.AVRO, inputs.get(0).getAvroSchema());
        return Collections.singletonMap(config.getName(), fcollection);
    }

    public static class H2Write extends PTransform<PCollectionTuple, PCollection<KV>> {

        private final String name;
        private final LocalH2SinkParameters parameters;

        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final List<String> inputSchemas;

        private final List<FCollection<?>> waits;

        private H2Write(
                final String name,
                final LocalH2SinkParameters parameters,
                final List<TupleTag<?>> tags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final List<String> inputSchemas,
                final List<FCollection<?>> waits) {

            this.name = name;
            this.parameters = parameters;
            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.inputSchemas = inputSchemas;
            this.waits = waits;
        }

        public PCollection<KV> expand(final PCollectionTuple inputs) {

            final PCollection<UnionValue> union = inputs
                    .apply("Union", Union.flatten(tags, inputTypes, inputNames));

            final PCollection<UnionValue> input;
            if((waits == null || waits.size() == 0)) {
                input = union;
            } else {
                final List<PCollection<?>> waitsList = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = union
                        .apply("Wait", Wait.on(waitsList))
                        .setCoder(union.getCoder());
            }

            final FileIO.Write<String, UnionValue> write = ZipFileUtil.createSingleFileWrite(
                    parameters.getOutput(),
                    parameters.getGroupFields(),
                    parameters.getTempDirectory(),
                    SchemaUtil.createGroupKeysFunction(UnionValue::getAsString, parameters.getGroupFields()));
            final WriteFilesResult writeResult = input
                    .apply("Write", write.via(H2Sink.of(
                            name, parameters.getDatabase(), parameters.getInput(), parameters.getConfigs(), parameters.getBatchSize(), inputNames, inputSchemas)));

            return writeResult.getPerDestinationOutputFilenames();
        }

    }

}