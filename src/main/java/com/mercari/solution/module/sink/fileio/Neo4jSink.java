package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.search.Neo4jUtil;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
//import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class Neo4jSink implements FileIO.Sink<MElement> {

    private static final String NEO4J_HOME = "/neo4j/";
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jSink.class);

    private final String name;
    private final String input;
    private final String database;
    private final List<Neo4jUtil.NodeConfig> nodes;
    private final List<Neo4jUtil.RelationshipConfig> relationships;
    private final List<String> setupCyphers;
    private final List<String> teardownCyphers;
    private final Integer bufferSize;
    private final String conf;
    private final Neo4jUtil.Format format;
    private final Boolean useGDS;
    private final List<String> inputNames;

    private final Counter counter;

    //private transient GraphDatabaseService graphDB;
    private transient List<MElement> buffer;
    private transient OutputStream outputStream;

    private Neo4jSink(final String name,
                      final String input,
                      final String database,
                      final String conf,
                      final List<Neo4jUtil.NodeConfig> nodes,
                      final List<Neo4jUtil.RelationshipConfig> relationships,
                      final List<String> setupCyphers,
                      final List<String> teardownCyphers,
                      final Integer bufferSize,
                      final Neo4jUtil.Format format,
                      final Boolean useGDS,
                      final List<String> inputNames) {

        this.name = name;
        this.input = input;
        this.database = database;
        this.conf = conf;
        this.nodes = nodes;
        this.relationships = relationships;
        this.setupCyphers = setupCyphers;
        this.teardownCyphers = teardownCyphers;
        this.bufferSize = bufferSize;
        this.format = format;
        this.useGDS = useGDS;
        this.inputNames = inputNames;

        this.counter = Metrics.counter(name, "processedCount");
    }

    public static Neo4jSink of(
            final String name,
            final String input,
            final String database,
            final String conf,
            final List<Neo4jUtil.NodeConfig> nodes,
            final List<Neo4jUtil.RelationshipConfig> relationships,
            final List<String> setupCyphers,
            final List<String> teardownCyphers,
            final Integer bufferSize,
            final Neo4jUtil.Format format,
            final Boolean useGDS,
            final List<String> inputNames) {

        return new Neo4jSink(name, input, database, conf, nodes, relationships, setupCyphers, teardownCyphers, bufferSize, format, useGDS, inputNames);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        final Path neo4jPath = Paths.get(NEO4J_HOME);
        neo4jPath.toFile().mkdir();

        if(input != null) {
            if(StorageUtil.exists(input)) {
                ZipFileUtil.downloadZipFiles(input, NEO4J_HOME);
                LOG.warn("Downloaded Neo4j initial database file from: {}", input);
            } else {
                LOG.warn("Not found Neo4j initial database file: {}", input);
            }
        }

        if(conf != null) {
            if(StorageUtil.exists(conf)) {
                final String confText = StorageUtil.readString(this.conf);
                LOG.info("Downloaded neo4j.conf from: {}", conf);
                LOG.info("Content neo4j.conf: {}", confText);
                final Path confPath = Paths.get(NEO4J_HOME + "conf");
                confPath.toFile().mkdir();
                try (final FileWriter filewriter = new FileWriter(NEO4J_HOME + "conf/neo4j.conf")) {
                    filewriter.write(confText);
                }
            } else {
                LOG.warn("Not found neo4j.conf: " + conf);
            }
        }

        /*
        if(graphDB == null) {
            final DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jPath).build();
            this.graphDB = service.database(database);
            if(this.useGDS) {
                Neo4jUtil.setupGds(this.graphDB);
            }
            Neo4jUtil.registerShutdownHook(service);
        }
         */

        setup();

        this.buffer = new ArrayList<>();
        this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(MElement unionValue) throws IOException {
        buffer.add(unionValue);
        if(buffer.size() >= bufferSize) {
            flushBuffer();
        }
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        teardown();
        /*
        switch (format) {
            case dump -> Neo4jUtil.dump(NEO4J_HOME, database, outputStream);
            case zip -> ZipFileUtil.writeZipFile(outputStream, NEO4J_HOME);
            default -> throw new IllegalArgumentException("Not supported neo4j format: " + format);
        }

         */
        LOG.info("Finished to upload documents!");
    }

    private void setup() {
        if(setupCyphers == null || setupCyphers.isEmpty()) {
            return;
        }
        /*
        try(final Transaction tx = graphDB.beginTx()) {
            for(final String setupCypher : setupCyphers) {
                final Result result = tx.execute(setupCypher);
                LOG.info("setup cypher query: {}. result: {}", setupCypher, result.resultAsString());
            }
            tx.commit();
        }
         */
    }

    private void teardown() {
        if(teardownCyphers == null || teardownCyphers.size() == 0) {
            return;
        }
        /*
        try(final Transaction tx = graphDB.beginTx()) {
            for(final String teardownCypher : teardownCyphers) {
                final Result result = tx.execute(teardownCypher);
                LOG.info("teardown cypher query: {}. result: {}", teardownCypher, result.resultAsString());
            }
            tx.commit();
        }
         */
    }

    private void flushBuffer() {
        //Neo4jUtil.index(graphDB, buffer, nodes, relationships, inputNames);
        counter.inc(buffer.size());
        buffer.clear();
    }

}
