package com.mercari.solution.module.sink;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.module.*;
import com.mercari.solution.util.schema.converter.ToStatementConverter;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.gcp.SecretManagerUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.sql.stmt.PreparedStatementTemplate;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Sink.Module(name="jdbc")
public class JdbcSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    private static class Parameters implements Serializable {

        private String table;
        private String url;
        private String driver;
        private String user;
        private String password;
        private String kmsKey;
        private Boolean createTable;
        private Boolean emptyTable;
        private List<String> keyFields;
        private Integer batchSize;
        private String op;


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(table == null) {
                errorMessages.add("Parameter must contain table");
            }
            if(url == null) {
                errorMessages.add("Parameter must contain connection url");
            }
            if(driver == null) {
                errorMessages.add("Parameter must contain driverClassName");
            }
            if(user == null) {
                errorMessages.add("Parameter must contain user");
            }
            if(password == null) {
                errorMessages.add("Parameter must contain password");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(createTable == null) {
                this.createTable = false;
            }
            if(emptyTable == null) {
                emptyTable = false;
            }
            if(op == null) {
                op = JdbcUtil.OP.INSERT.name();
            }
            if(batchSize == null) {
                batchSize = 1000;
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
        }

        public void replaceParameters() {
            if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
                try(final SecretManagerServiceClient secretClient = SecretManagerUtil.createClient()) {
                    if(SecretManagerUtil.isSecretName(user)) {
                        user = SecretManagerUtil.getSecret(secretClient, user).toStringUtf8();
                    }
                    if(SecretManagerUtil.isSecretName(password)) {
                        password = SecretManagerUtil.getSecret(secretClient, password).toStringUtf8();
                    }
                }
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
        parameters.replaceParameters();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final JdbcUtil.DB db = getDB(parameters.driver);
        final List<List<String>> ddls;
        if (parameters.createTable) {
            ddls = new ArrayList<>();
            final String ddl = JdbcUtil.buildCreateTableSQL(
                    inputSchema.getAvroSchema(), parameters.table, db, parameters.keyFields);
            ddls.add(Arrays.asList(ddl));
        } else {
            ddls = new ArrayList<>();
        }
        if (parameters.emptyTable) {
            ddls.add(Arrays.asList("DELETE FROM " + parameters.table));
        }

        final PCollection<MElement> tableReady;
        if(ddls.isEmpty()) {
            tableReady = input;
        } else {
            final PCollection<String> wait = input.getPipeline()
                    .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                    .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(
                            parameters.driver, parameters.url, parameters.user, parameters.password)));
            tableReady = input
                    .apply("WaitToTableCreation", Wait.on(wait))
                    .setCoder(input.getCoder());
        }

        final PreparedStatementTemplate statementTemplate = JdbcUtil.createStatement(
                parameters.table, inputSchema.getAvroSchema(),
                JdbcUtil.OP.valueOf(parameters.op), db,
                parameters.keyFields);

        final PCollection<MElement> results = tableReady
                .apply("WriteJdbc", ParDo.of(new WriteDoFn(
                        parameters.driver, parameters.url, parameters.user, parameters.password,
                        statementTemplate, parameters.batchSize)));

        return MCollectionTuple
                .of(results, Schema.builder().withField("dummy", Schema.FieldType.STRING).build());
    }

    private JdbcUtil.DB getDB(final String driver) {
        if(driver.contains("mysql")) {
            return JdbcUtil.DB.MYSQL;
        } else if(driver.contains("postgresql")) {
            return JdbcUtil.DB.POSTGRESQL;
        } else {
            throw new IllegalStateException("Not supported JDBC driver: " + driver);
        }
    }

    private static class WriteDoFn extends DoFn<MElement, MElement> {

        private final String driver;
        private final String url;
        private final String user;
        private final String password;
        private final PreparedStatementTemplate statementTemplate;
        private final int batchSize;

        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient Connection connection = null;
        private transient PreparedStatement preparedStatement;

        private transient int bufferSize;

        public WriteDoFn(
                final String driver,
                final String url,
                final String user,
                final String password,
                final PreparedStatementTemplate statementTemplate,
                final int batchSize) {

            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
            this.statementTemplate = statementTemplate;
            this.batchSize = batchSize;
        }


        @Setup
        public void setup() {
            this.dataSource = JdbcUtil.createDataSource(driver, url, user, password);
        }

        @Teardown
        public void teardown() throws Exception {
            cleanUpDataSource();
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(statementTemplate.getStatementString());
            }
            bufferSize = 0;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            try {
                preparedStatement.clearParameters();
                ToStatementConverter.convertElement(c.element(), this.statementTemplate.createPlaceholderSetterProxy(preparedStatement));
                preparedStatement.addBatch();
                bufferSize += 1;

                if (bufferSize >= batchSize) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    bufferSize = 0;
                }
            } catch (SQLException e) {
                preparedStatement.clearBatch();
                connection.rollback();
                throw new RuntimeException(e);
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            try {
                if (bufferSize > 0) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
                cleanUpStatementAndConnection();
            } catch (SQLException e) {
                preparedStatement.clearBatch();
                connection.rollback();
                cleanUpStatementAndConnection();
                throw new RuntimeException(e);
            }
        }

        private void cleanUpStatementAndConnection() throws Exception {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } finally {
                    preparedStatement = null;
                }
            }

            if(connection != null) {
                try {
                    connection.close();
                } finally {
                    connection = null;
                }
            }
        }

        private void cleanUpDataSource() throws Exception {
            cleanUpStatementAndConnection();

            if(dataSource != null) {
                try {
                    dataSource.close();
                } catch (IOException e) {
                } finally {
                    dataSource = null;
                }
            }
        }
    }

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TablePrepareDoFn.class);

        private final String driver;
        private final String url;
        private final String user;
        private final String password;

        TablePrepareDoFn(final String driver, final String url, final String user, final String password) {
            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            final List<String> ddl = c.element();
            if(ddl == null) {
                return;
            }
            if(ddl.isEmpty()) {
                c.output("ok");
                return;
            }
            try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil.createDataSource(driver, url, user, password)) {
                try(final Connection connection = dataSource.getConnection()) {
                    for(final String sql : ddl) {
                        LOG.info("ExecuteDDL: " + sql);
                        connection.createStatement().executeUpdate(sql);
                        connection.commit();
                        LOG.info("ExecutedDDL: " + sql);
                    }
                }
            }
            c.output("ok");
        }
    }

}
