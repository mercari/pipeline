package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.fileio.Neo4jSink;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.search.Neo4jUtil;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class LocalNeo4jSink {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNeo4jSink.class);

    private static class LocalNeo4jSinkParameters implements Serializable {

        private String output;
        private String input;
        private String database;
        private String conf;
        private List<Neo4jUtil.NodeConfig> nodes;
        private List<Neo4jUtil.RelationshipConfig> relationships;
        private List<String> setupCyphers;
        private List<String> teardownCyphers;
        private Integer bufferSize;
        private Neo4jUtil.Format format;
        private Boolean useGDS;

        private List<String> groupFields;
        private String tempDirectory;

        public static LocalNeo4jSinkParameters of(final JsonElement jsonElement) {
            final LocalNeo4jSinkParameters parameters = new Gson().fromJson(jsonElement, LocalNeo4jSinkParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("localNeo4j sink parameters must not be empty!");
            }
            parameters.validate();
            parameters.setDefaults();
            return parameters;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("localNeo4j sink module requires `output` parameter.");
            }
            if(this.input != null) {
                if(!this.input.startsWith("gs://")) {
                    errorMessages.add("localNeo4j sink module `input` parameter must be gcs path (must start with gs://).");
                }
            }
            if(this.conf != null) {
                if(!this.conf.startsWith("gs://")) {
                    errorMessages.add("localNeo4j sink module `conf` parameter must be gcs path (must start with gs://).");
                }
            }
            if((nodes == null || nodes.isEmpty()) && (relationships == null || relationships.isEmpty())) {
                errorMessages.add("localNeo4j sink module requires `nodes` or `relationships` parameter.");
            } else {
                if(nodes != null) {
                    for(int i=0; i<nodes.size(); i++) {
                        errorMessages.addAll(nodes.get(i).validate(i));
                    }
                }
                if(relationships != null) {
                    for(int i=0; i<relationships.size(); i++) {
                        errorMessages.addAll(relationships.get(i).validate(i));
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {

            if(this.database == null) {
                this.database = Neo4jUtil.DEFAULT_DATABASE_NAME;
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            if(this.nodes == null) {
                this.nodes = new ArrayList<>();
            } else {
                for(final Neo4jUtil.NodeConfig node : nodes) {
                    node.setDefaults();
                }
            }
            if(this.relationships == null) {
                this.relationships = new ArrayList<>();
            } else {
                for(final Neo4jUtil.RelationshipConfig relationship : relationships) {
                    relationship.setDefaults();
                }
            }
            if(this.setupCyphers == null) {
                this.setupCyphers = new ArrayList<>();
            }
            if(this.teardownCyphers == null) {
                this.teardownCyphers = new ArrayList<>();
            }
            if(this.bufferSize == null) {
                this.bufferSize = 1000;
            }
            if(this.format == null) {
                this.format = Neo4jUtil.Format.dump;
            }
            if(this.useGDS == null) {
                this.useGDS = false;
            }
        }

    }


    /*
    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {

        if(inputs == null || inputs.size() == 0) {
            throw new IllegalArgumentException("localNeo4j sink module requires inputs");
        }

        final LocalNeo4jSinkParameters parameters = getParameters(LocalNeo4jSinkParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> output = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("WriteNeo4j", new Neo4jIndexWrite(getName(), parameters, inputs.getAllInputs()));
        return MCollectionTuple
                .of(output, Neo4jIndexWrite.createOutputSchema());
    }

    public static class Neo4jIndexWrite extends PTransform<PCollection<MElement>, PCollection<MElement>> {

        private final String name;
        private final LocalNeo4jSinkParameters parameters;

        private final List<String> inputNames;

        private Neo4jIndexWrite(final String name,
                                final LocalNeo4jSinkParameters parameters,
                                final List<String> inputNames) {

            this.name = name;
            this.parameters = parameters;
            this.inputNames = inputNames;
        }

        public PCollection<MElement> expand(final PCollection<MElement> input) {

            final FileIO.Write<String, MElement> write = ZipFileUtil.createSingleFileWrite(
                    parameters.output,
                    parameters.groupFields,
                    parameters.tempDirectory,
                    SchemaUtil.createGroupKeysFunction(MElement::getAsString, parameters.groupFields));
            final WriteFilesResult<String> writeResult = input
                            .apply("Write", write.via(Neo4jSink.of(
                                    name,
                                    parameters.input, parameters.database, parameters.conf,
                                    parameters.nodes, parameters.relationships,
                                    parameters.setupCyphers, parameters.teardownCyphers,
                                    parameters.bufferSize, parameters.format, parameters.useGDS,
                                    inputNames)));

            return writeResult
                    .getPerDestinationOutputFilenames()
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

     */

}
