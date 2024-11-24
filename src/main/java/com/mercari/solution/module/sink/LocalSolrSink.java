package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.fileio.SolrSink;
import com.mercari.solution.util.domain.text.XmlUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.schema.converter.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Sink.Module(name="localSolr")
public class LocalSolrSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSolrSink.class);

    private static class LocalSolrSinkParameters implements Serializable {

        private String input;
        private String output;
        private List<CoreParameter> cores;
        private List<String> groupFields;
        private String tempDirectory;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("parameters.output must not be null");
            }
            if(this.cores == null || this.cores.isEmpty()) {
                errorMessages.add("parameters.cores must not be empty");
            } else {
                for(int i=0; i<this.cores.size(); i++) {
                    errorMessages.addAll(this.cores.get(i).validate(i));
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(this.input == null) {
                this.input = output;
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            for(final CoreParameter coreParameter : this.cores) {
                coreParameter.setDefaults();
            }
        }

    }

    public static class CoreParameter implements Serializable {

        private String name;
        private String input;
        private JsonElement schema;
        private JsonElement config;
        private List<CustomConfigFileParameter> customConfigFiles;

        public List<String> validate(int i) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("parameters.cores[" + i + "].name must not be null");
            }
            if(input == null) {
                errorMessages.add("parameters.cores[" + i + "].input must not be null");
            }
            if(customConfigFiles != null) {
                for(int j=0; j<customConfigFiles.size(); j++) {
                    errorMessages.addAll(customConfigFiles.get(j).validate(i, j));
                }
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(this.customConfigFiles == null) {
                this.customConfigFiles = new ArrayList<>();
            } else {
                for(final CustomConfigFileParameter customConfigFile : customConfigFiles) {
                    customConfigFile.setDefaults();
                }
            }
        }

        public Core toCore(Map<String, Schema> inputSchemas) {
            final Core core = new Core();
            core.name = name;
            core.input = input;

            // solr schema
            if(schema == null || schema.isJsonNull()) {
                final Schema inputSchema = inputSchemas.get(input);
                core.schema = AvroToSolrDocumentConverter.convertSchema(inputSchema.getAvroSchema());
            } else if(schema.isJsonPrimitive()) {
                if(schema.getAsString().replaceAll("\"", "").startsWith("gs://")) {
                    core.schema = StorageUtil.readString(schema.getAsString().replaceAll("\"", ""));
                } else {
                    core.schema = schema.getAsString();
                }
            } else if(schema.isJsonObject()) {
                core.schema = XmlUtil.toString(SolrSchemaUtil.convertIndexSchemaJsonToXml(schema.getAsJsonObject()));
            } else {
                throw new IllegalArgumentException();
            }

            // solr config
            if(config == null || config.isJsonNull()) {
                core.config = XmlUtil.toString(SolrSchemaUtil.createSolrConfig().createDocument());
            } else if(config.isJsonPrimitive()) {
                if(config.getAsString().replaceAll("\"", "").startsWith("gs://")) {
                    core.config = StorageUtil.readString(config.getAsString().replaceAll("\"", ""));
                } else {
                    throw new IllegalArgumentException();
                }
            } else if(config.isJsonObject()) {
                final SolrSchemaUtil.SolrConfig solrConfig = new Gson().fromJson(config.getAsJsonObject(), SolrSchemaUtil.SolrConfig.class);
                core.config = XmlUtil.toString(solrConfig.createDocument());
            } else {
                throw new IllegalArgumentException();
            }

            // custom config files
            final List<KV<String,String>> customConfigFilePaths = new ArrayList<>();
            if(!customConfigFiles.isEmpty()) {
                for(final CustomConfigFileParameter customConfigFile : customConfigFiles) {
                    customConfigFilePaths.add(KV.of(customConfigFile.getFilename(), customConfigFile.getInput()));
                }
            }
            core.customConfigFiles = customConfigFilePaths;

            return core;
        }
    }

    private static class CustomConfigFileParameter implements Serializable {

        private String input;
        private String filename;

        public String getInput() {
            return input;
        }

        public String getFilename() {
            return filename;
        }

        public List<String> validate(int coreIndex, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("localSolr.cores[" + coreIndex + "].customConfigFiles[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.filename == null) {
                String[] paths = this.input.split("/");
                this.filename = paths[paths.length - 1];
            }
        }
    }

    public static class Core implements Serializable {

        private String name;
        private String input;
        private String schema;
        private String config;
        private List<String> fields;
        private List<KV<String,String>> customConfigFiles;


        public String getName() {
            return name;
        }

        public String getInput() {
            return input;
        }

        public String getSchema() {
            return schema;
        }

        public String getConfig() {
            return config;
        }

        public List<String> getFields() {
            return fields;
        }

        public List<KV<String,String>> getCustomConfigFiles() {
            return customConfigFiles;
        }
    }


    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        if(inputs == null || inputs.size() == 0) {
            throw new IllegalModuleException("localSolr sink module requires input parameter");
        }

        final LocalSolrSinkParameters parameters = getParameters(LocalSolrSinkParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final List<String> inputNames = new ArrayList<>();
        final Map<String, Schema> inputSchemas = new HashMap<>();
        for(final String inputName : inputs.getAllInputs()){
            inputNames.add(inputName);
            inputSchemas.put(inputName, inputs.getSchema(inputName));
        }

        final SolrIndexWrite write = new SolrIndexWrite(getName(), parameters, inputNames, inputSchemas);
        final Schema outputSchema = SolrIndexWrite.createOutputSchema();
        final PCollection<MElement> output = input.apply(getName(), write);

        return MCollectionTuple
                .of(output, outputSchema);
    }

    public static class SolrIndexWrite extends PTransform<PCollection<MElement>, PCollection<MElement>> {

        private String name;
        private String output;
        private List<String> groupFields;
        private String tempDirectory;
        private List<Core> cores;

        private final List<String> inputNames;
        private final List<Schema> inputSchemas;

        private SolrIndexWrite(
                final String name,
                final LocalSolrSinkParameters parameters,
                final List<String> inputNames,
                final Map<String, Schema> inputSchemaMap) {

            this.name = name;
            this.output = parameters.output;
            this.groupFields = parameters.groupFields;
            this.tempDirectory = parameters.tempDirectory;
            this.cores = parameters.cores.stream()
                    .map(c -> c.toCore(inputSchemaMap))
                    .collect(Collectors.toList());
            this.inputNames = inputNames;
            this.inputSchemas = new ArrayList<>();
            for(final String inputName : inputNames) {
                inputSchemas.add(inputSchemaMap.get(inputName));
            }
        }

        public PCollection<MElement> expand(final PCollection<MElement> input) {

            final FileIO.Write<String, MElement> write = ZipFileUtil.createSingleFileWrite(
                    output,
                    groupFields,
                    tempDirectory,
                    SchemaUtil.createGroupKeysFunction(MElement::getAsString, groupFields));
            final WriteFilesResult<String> writeResult = input
                    .apply("Write", write.via(SolrSink.of(
                            cores,
                            inputNames,
                            inputSchemas)));

            return writeResult.getPerDestinationOutputFilenames()
                    .apply("Format", ParDo.of(new FormatDoFn()))
                    .setCoder(ElementCoder.of(createOutputSchema()));
        }

        private static class FormatDoFn extends DoFn<KV<String,String>, MElement> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement element = MElement.builder()
                        .withString("key", c.element().getKey())
                        .withString("value", c.element().getValue())
                        .build();
                c.output(element);
            }

        }

        public static Schema createOutputSchema() {
            return Schema.builder()
                    .withField("key", Schema.FieldType.STRING.withNullable(true))
                    .withField("value", Schema.FieldType.STRING.withNullable(true))
                    .build();
        }

    }

}
