package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.converter.ToStatementConverter;
import com.mercari.solution.util.domain.search.H2Util;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.sql.stmt.PreparedStatementTemplate;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class H2Sink implements FileIO.Sink<MElement> {

    private static final String H2_HOME = "/h2/";
    private static final Logger LOG = LoggerFactory.getLogger(H2Sink.class);

    private final String name;
    private final String database;
    private final String input;
    private final List<H2Util.Config> configs;
    private final Integer batchSize;
    private final List<String> inputNames;
    private final Map<String, Schema> inputSchemas;
    private final Map<String, H2Util.Config> configMap;
    private final String databasePath;

    private final Counter counter;

    private transient List<MElement> buffer;
    private transient Connection connection;
    private transient Map<String, PreparedStatementWithTemplate> preparedStatements;
    private transient OutputStream outputStream;

    private static class PreparedStatementWithTemplate {
        final PreparedStatementTemplate template;
        final PreparedStatement preparedStatement;

        PreparedStatementWithTemplate(PreparedStatementTemplate template, PreparedStatement preparedStatement) {
            this.template = template;
            this.preparedStatement = preparedStatement;
        }

        PreparedStatementTemplate.PlaceholderSetterProxy createSetterProxy() {
            return this.template.createPlaceholderSetterProxy(this.preparedStatement);
        }
    }

    private H2Sink(
            final String name,
            final String database,
            final String input,
            final List<H2Util.Config> configs,
            final Integer batchSize,
            final List<String> inputNames,
            final List<Schema> inputSchemas) {

        this.name = name;
        this.database = database;
        this.input = input;
        this.configs = configs;
        this.batchSize = batchSize;
        this.inputNames = inputNames;
        this.inputSchemas = new HashMap<>();
        for(int i=0; i<inputNames.size(); i++) {
            this.inputSchemas.put(inputNames.get(i), inputSchemas.get(i));
        }
        this.configMap = configs.stream().collect(Collectors.toMap(H2Util.Config::getInput, c -> c));
        this.databasePath = String.format("%s%s/%s", H2_HOME, name, database);

        this.counter = Metrics.counter(name, "processedCount");
    }

    public static H2Sink of(
            final String name,
            final String database,
            final String input,
            final List<H2Util.Config> configs,
            final Integer bufferSize,
            final List<String> inputNames,
            final List<Schema> inputSchemas) {

        return new H2Sink(name, database, input, configs, bufferSize, inputNames, inputSchemas);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        LOG.info("open h2 sink database: " + database);
        final Path h2Path = Paths.get(databasePath);
        if(h2Path.toFile().mkdirs()) {
            LOG.info("create databasePath: " + databasePath);
        } else {
            LOG.info("Already exists databasePath: " + databasePath);
        }

        if(input != null) {
            if(StorageUtil.exists(input)) {
                ZipFileUtil.downloadZipFiles(input, databasePath);
                LOG.warn("Downloaded H2 initial database file from: " + input);
            } else {
                LOG.warn("Not found H2 initial database file: " + input);
            }
        }

        if(this.preparedStatements == null) {
            this.preparedStatements = new HashMap<>();
        }

        try {
            if(connection == null || connection.isClosed()) {
                this.connection = DriverManager.getConnection("jdbc:h2:file:" + databasePath, "sa", "");
                this.connection.setAutoCommit(false);

                for(final PreparedStatementWithTemplate ps : this.preparedStatements.values()) {
                    ps.preparedStatement.close();
                }

                for(final H2Util.Config config : configs) {
                    for(final String ddl : config.getDdls()) {
                        try(final Statement statement = connection.createStatement()) {
                            final int result = statement.executeUpdate(ddl);
                            LOG.info("setup table: " + config.getTable() + " ddl: " + ddl + ". result: " + result);
                        } catch (final SQLException e) {
                            throw new IllegalArgumentException("Failed to execute DDL: " + ddl, e);
                        }
                    }

                    //final Schema tableSchema = JdbcUtil.createAvroSchemaFromTable(connection, config.getTable());
                    final org.apache.avro.Schema inputSchema = inputSchemas.get(config.getInput()).getAvroSchema();

                    final PreparedStatementTemplate statementTemplate = JdbcUtil.createStatement(config.getTable(), inputSchema, config.getOp(), JdbcUtil.DB.H2, config.getKeyFields());
                    LOG.info("setup preparedStatement: " + statementTemplate);
                    final PreparedStatement preparedStatement = connection.prepareStatement(statementTemplate.getStatementString());
                    this.preparedStatements.put(config.getInput(), new PreparedStatementWithTemplate(statementTemplate, preparedStatement));
                }
            }
        } catch (final SQLException e) {
            throw new IllegalStateException("Failed to open", e);
        }

        this.buffer = new ArrayList<>();
        this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(MElement element) throws IOException {
        buffer.add(element);
        if(buffer.size() >= batchSize) {
            flushBuffer();
            buffer.clear();
        }
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        teardown();
        final String localPath = String.format("%s%s", H2_HOME, name);
        ZipFileUtil.writeZipFile(outputStream, localPath);
        LOG.info("Finished to upload: " + localPath);
    }

    private void teardown() {
        if(this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to close connection", e);
            }
        }
    }

    private void flushBuffer() {
        try {
            for(final PreparedStatementWithTemplate ps : preparedStatements.values()) {
                //ps.preparedStatement.clearParameters();
            }

            for(final MElement unionValue : buffer) {
                final String inputName = inputNames.get(unionValue.getIndex());
                final PreparedStatementWithTemplate ps = preparedStatements.get(inputName);
                final H2Util.Config config = configMap.get(inputName);
                if(ps == null || config == null) {
                    LOG.warn("Skip statement");
                    continue;
                }
                ToStatementConverter.convertElement(unionValue, ps.createSetterProxy(), config.getKeyFields());
                ps.preparedStatement.addBatch();
            }

            for(final PreparedStatementWithTemplate ps : preparedStatements.values()) {
                final int[] result = ps.preparedStatement.executeBatch();
                LOG.info("Execute statement : " + Arrays.stream(result).sum());
            }

            connection.commit();

            counter.inc(buffer.size());
            buffer.clear();
        } catch (SQLException e) {
            //preparedStatement.clearBatch();
            //connection.rollback();
            throw new RuntimeException(e);
        }
    }

}