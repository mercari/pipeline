package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.ToStatementConverter;
import com.mercari.solution.util.gcp.JdbcUtil;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class JdbcSink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    private static class JdbcSinkParameters implements Serializable {

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

        public String getTable() {
            return table;
        }

        public String getUrl() {
            return url;
        }

        public String getDriver() {
            return driver;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public Boolean getCreateTable() {
            return createTable;
        }

        public Boolean getEmptyTable() {
            return emptyTable;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public String getOp() {
            return op;
        }


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

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
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
    }

    public String getName() { return "jdbc"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("jdbc sink module requires input parameter");
        }
        return Collections.singletonMap(config.getName(), JdbcSink.write(inputs.get(0), config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        if(config.getParameters() == null) {
            throw new IllegalArgumentException("jdbc sink module parameters must not be null");
        }
        final JdbcSinkParameters parameters = new Gson().fromJson(config.getParameters(), JdbcSinkParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final JdbcWrite write = switch (collection.getDataType()) {
            case AVRO -> new JdbcWrite<>(collection, parameters, ToStatementConverter::convertRecord);
            case ROW -> new JdbcWrite<>(collection, parameters, ToStatementConverter::convertRow);
            case STRUCT -> new JdbcWrite<>(collection, parameters, ToStatementConverter::convertStruct);
            case DOCUMENT -> new JdbcWrite<>(collection, parameters, ToStatementConverter::convertDocument);
            case ENTITY -> new JdbcWrite<>(collection, parameters, ToStatementConverter::convertEntity);
            default -> throw new IllegalArgumentException("Not supported input type: " + collection.getDataType());
        };
        PCollection output = (PCollection) (collection.getCollection().apply(config.getName(), write));
        /*
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

         */
        return FCollection.update(collection, output);
    }

    public static class JdbcWrite<InputT> extends PTransform<PCollection<InputT>, PCollection<Void>> {

        private FCollection<?> inputCollection;
        private final JdbcSinkParameters parameters;

        private final JdbcIO.PreparedStatementSetter<InputT> formatter;

        private JdbcWrite(
                final FCollection<?> inputCollection,
                final JdbcSinkParameters parameters,
                final JdbcIO.PreparedStatementSetter<InputT> formatter) {

            this.inputCollection = inputCollection;
            this.parameters = parameters;
            this.formatter = formatter;
        }

        public PCollection<Void> expand(final PCollection<InputT> input) {
            final JdbcUtil.DB db = getDB(parameters.getDriver());
            final List<List<String>> ddls;
            if (this.parameters.getCreateTable()) {
                ddls = new ArrayList<>();
                final String ddl = JdbcUtil.buildCreateTableSQL(
                        inputCollection.getAvroSchema(), parameters.getTable(), db, parameters.getKeyFields());
                ddls.add(Arrays.asList(ddl));
            } else {
                ddls = new ArrayList<>();
            }
            if (parameters.getEmptyTable()) {
                ddls.add(Arrays.asList("DELETE FROM " + parameters.getTable()));
            }

            final PCollection<InputT> tableReady;
            if(ddls.size() == 0) {
                tableReady = input;
            } else {
                final PCollection<String> wait = input.getPipeline()
                        .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                        .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(
                                parameters.getDriver(), parameters.getUrl(), parameters.getUser(), parameters.getPassword())));
                tableReady = input
                        .apply("WaitToTableCreation", Wait.on(wait))
                        .setCoder(input.getCoder());
            }

            final String statementString = JdbcUtil.createStatement(
                    parameters.getTable(), inputCollection.getAvroSchema(),
                    JdbcUtil.OP.valueOf(parameters.getOp()), db,
                    parameters.getKeyFields());

            return tableReady.apply("WriteJdbc", ParDo.of(new WriteDoFn<>(
                    parameters.getDriver(), parameters.getUrl(), parameters.getUser(), parameters.getPassword(),
                    statementString, parameters.getBatchSize(), formatter)));
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

    }

    private static class WriteDoFn<T> extends DoFn<T, Void> {

        private final String driver;
        private final String url;
        private final String user;
        private final String password;
        private final String statement;
        private final int batchSize;
        private final JdbcIO.PreparedStatementSetter<T> setter;

        private transient DataSource dataSource;
        private transient Connection connection = null;
        private transient PreparedStatement preparedStatement;

        private transient int bufferSize;

        public WriteDoFn(final String driver, final String url, final String user, final String password,
                         final String statement, final int batchSize, final JdbcIO.PreparedStatementSetter<T> setter) {
            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
            this.statement = statement;
            this.batchSize = batchSize;
            this.setter = setter;
        }


        @Setup
        public void setup() {
            this.dataSource = JdbcUtil.createDataSource(driver, url, user, password);
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(statement);
            }
            bufferSize = 0;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            try {
                preparedStatement.clearParameters();
                setter.setParameters(c.element(), preparedStatement);
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

        protected void finalize() throws Throwable {
            cleanUpStatementAndConnection();
        }

        private void cleanUpStatementAndConnection() throws Exception {
            try {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } finally {
                        preparedStatement = null;
                    }
                }
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } finally {
                        connection = null;
                    }
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
            if(ddl.size() == 0) {
                c.output("ok");
                return;
            }
            try(final Connection connection = JdbcUtil.createDataSource(driver, url, user, password).getConnection()) {
                for(final String sql : ddl) {
                    LOG.info("ExecuteDDL: " + sql);
                    connection.createStatement().executeUpdate(sql);
                    connection.commit();
                    LOG.info("ExecutedDDL: " + sql);
                }
            }
            c.output("ok");
        }
    }

}
