package com.mercari.solution.module.source;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.gcp.SecretManagerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


@Source.Module(name="jdbc")
public class JdbcSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private static class Parameters implements Serializable {

        private String url;
        private String driver;
        private String user;
        private String password;

        // For query extraction
        private String query;
        private List<String> prepareCalls;
        private List<PrepareParameterQuery> prepareParameterQueries;

        // For table extraction
        private String table;
        private List<String> keyFields;
        private String fields;
        private List<String> excludeFields;
        private Integer fetchSize;
        private Integer splitSize;
        private Boolean enableSplit;

        public void validate() {

            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            }
            if(driver == null) {
                errorMessages.add("parameters.driver must not be null");
            }
            if(user != null && password == null) {
                errorMessages.add("parameters.password must not be null");
            }

            if(query == null && table == null) {
                errorMessages.add("parameters.query or parameters.table must not be null");
            } else if(query != null && table != null) {
                errorMessages.add("parameters.query or parameters.table only one of " + query + " : " + table + " must be specified");
            }

            if(prepareParameterQueries != null) {
                for(final PrepareParameterQuery preprocessQuery : prepareParameterQueries) {
                    errorMessages.addAll(preprocessQuery.validate());
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(prepareCalls == null) {
                prepareCalls = new ArrayList<>();
            }
            if(prepareParameterQueries == null) {
                prepareParameterQueries = new ArrayList<>();
            } else {
                for(final PrepareParameterQuery prepareParameterQuery : prepareParameterQueries) {
                    prepareParameterQuery.setDefaults();
                }
            }
            if(fields == null) {
                fields = "*";
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(excludeFields == null) {
                excludeFields = new ArrayList<>();
            }
            if(fetchSize == null) {
                fetchSize = 50_000;
            }
            if(splitSize == null) {
                splitSize = 10;
            }
            if(enableSplit == null) {
                enableSplit = false;
            }
        }

        public void replaceParameters(final Pipeline pipeline) {

            if(query != null && query.startsWith("gs://")) {
                query = StorageUtil.readString(query);
            }

            if(user == null) {
                final String serviceAccount = DataflowOptions.getServiceAccount(pipeline.getOptions());//.as(DataflowPipelineOptions.class).getServiceAccount();
                LOG.info("Using worker service account: '{}' for database user", serviceAccount);
                user = serviceAccount.replace(".gserviceaccount.com", "");
                password = "dummy";
                if(!url.contains("enableIamAuth")) {
                    url = url + "&enableIamAuth=true";
                }
            } else if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
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

    public static class PrepareParameterQuery implements Serializable {

        private String query;
        private List<String> prepareCalls;

        public String getQuery() {
            return query;
        }

        public List<String> getPrepareCalls() {
            return prepareCalls;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(getQuery() == null) {
                errorMessages.add("Jdbc source module preprocessQuery requires query parameter");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.prepareCalls == null) {
                this.prepareCalls = new ArrayList<>();
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters(begin.getPipeline());

        try {
            final Schema outputSchema;
            final PCollection<GenericRecord> output;
            if(parameters.query != null) {
                outputSchema = Optional
                        .ofNullable(getSchema())
                        .map(com.mercari.solution.module.Schema::getAvroSchema)
                        .orElseGet(() -> getQuerySchema(parameters));
                output = begin
                        .apply("Query", new JdbcBatchQuerySource(
                                parameters, getTimestampAttribute(), getTimestampDefault(), outputSchema))
                        .setCoder(AvroCoder.of(outputSchema));
            } else if(parameters.table != null) {
                outputSchema = Optional
                        .ofNullable(getSchema())
                        .map(com.mercari.solution.module.Schema::getAvroSchema)
                        .orElseGet(() -> getTableSchema(parameters));
                output = begin
                        .apply("Read", new JdbcBatchTableSource(
                                parameters, getTimestampAttribute(), getTimestampDefault(), outputSchema))
                        .setCoder(AvroCoder.of(outputSchema));
            } else {
                throw new IllegalArgumentException("Jdbc source module: " + getName() + " does not contain parameter both query and table");
            }
            LOG.info("{} outputSchema: {}", getName(), outputSchema);

            final PCollection<MElement> element = output
                    .apply("Convert", ParDo.of(new ConvertDoFn(getTimestampAttribute(), getTimestampDefault())));

            return MCollectionTuple
                    .of(element, com.mercari.solution.module.Schema.of(outputSchema));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Schema getTableSchema(final Parameters parameters) {
        try {
            return JdbcUtil.createAvroSchemaFromTable(
                    parameters.driver, parameters.url,
                    parameters.user, parameters.password,
                    parameters.table);
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Schema getQuerySchema(final Parameters parameters) {
        try {
            final Schema queryOutputSchema = JdbcUtil.createAvroSchemaFromQuery(
                    parameters.driver, parameters.url,
                    parameters.user, parameters.password,
                    parameters.query, parameters.prepareCalls);
            if (!parameters.excludeFields.isEmpty()) {
                return AvroSchemaUtil
                        .toSchemaBuilder(queryOutputSchema, null, parameters.excludeFields)
                        .endRecord();
            } else {
                return queryOutputSchema;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get query schema", e);
        }
    }

    private static class ConvertDoFn extends DoFn<GenericRecord, MElement> {

        private final String timestampAttribute;
        private final String timestampDefault;

        ConvertDoFn(
                final String timestampAttribute,
                final String timestampDefault) {

            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final GenericRecord record = c.element();
            final MElement element = MElement.of(record, c.timestamp());
            if(timestampAttribute == null) {
                c.output(element);
            } else {
                final Instant timestamp = Optional
                        .ofNullable(element.getAsJodaInstant(timestampAttribute))
                        .orElseGet(() -> DateTimeUtil.toJodaInstant(timestampDefault));
                c.outputWithTimestamp(element, timestamp);
            }
        }

    }

    public static class JdbcBatchQuerySource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private static final String DUMMY_FIELD = "Dummy_String_Field_";

        private final Parameters parameters;

        private final String timestampAttribute;
        private final String timestampDefault;

        private final String outputSchemaString;

        private JdbcBatchQuerySource(
                final Parameters parameters,
                final String timestampAttribute,
                final String timestampDefault,
                final Schema outputSchema) {

            this.parameters = parameters;
            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
            this.outputSchemaString = outputSchema.toString();
        }

        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {

            PCollection<GenericRecord> records = begin
                    .apply("Seed", Create
                            .of(createDummyRecord()).withCoder(AvroCoder.of(createDummySchema())));
            int num = 0;
            for(final PrepareParameterQuery preprocessQuery : parameters.prepareParameterQueries) {
                try {
                    final Schema outputPreprocessSchema = JdbcUtil.createAvroSchemaFromQuery(
                            parameters.driver, parameters.url,
                            parameters.user, parameters.password,
                            preprocessQuery.getQuery(), preprocessQuery.getPrepareCalls());

                    records = records
                            .apply("PrepQuery" + num, ParDo
                                    .of(new QueryDoFn(parameters, preprocessQuery.getQuery(), preprocessQuery.getPrepareCalls(), outputPreprocessSchema.toString())))
                            .setCoder(AvroCoder.of(outputPreprocessSchema))
                            .apply("Reshuffle" + num, Reshuffle.viaRandomKey());
                    num = num + 1;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return records
                    .apply("ExecuteQuery", ParDo
                            .of(new QueryDoFn(parameters, parameters.query, parameters.prepareCalls, outputSchemaString)));
        }

        public static class QueryDoFn extends DoFn<GenericRecord, GenericRecord> {

            private static final int DEFAULT_FETCH_SIZE = 50_000;

            private final String driver;
            private final String url;
            private final String user;
            private final String password;

            private final String query;
            private final List<String> prepareCalls;

            private final String outputSchemaString;

            private transient Schema outputSchema;
            private transient JdbcUtil.CloseableDataSource dataSource;
            private transient Connection connection;

            QueryDoFn(final Parameters parameters,
                      final String query,
                      final List<String> prepareCalls,
                      final String outputSchemaString) {

                this.driver = parameters.driver;
                this.url = parameters.url;
                this.user = parameters.user;
                this.password = parameters.password;

                this.query = query;
                this.prepareCalls = prepareCalls;

                this.outputSchemaString = outputSchemaString;
            }

            @Setup
            public void setup() throws SQLException {
                this.dataSource = JdbcUtil.createDataSource(this.driver, this.url, user, password, true);
                this.connection = dataSource.getConnection();
                this.outputSchema = AvroSchemaUtil.convertSchema(outputSchemaString);
            }

            @Teardown
            public void teardown() throws SQLException {
                if(this.connection != null) {
                    try {
                        this.connection.close();
                    } finally {
                        this.connection = null;
                    }
                }

                if(this.dataSource != null) {
                    try {
                        this.dataSource.close();
                    } catch (IOException e) {
                    } finally {
                        this.dataSource = null;
                    }
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws SQLException, IOException {
                LOG.info(String.format("Start Query [%s]", query));
                if(!prepareCalls.isEmpty()) {
                    for(final String prepareCall : prepareCalls) {
                        try (final CallableStatement statement = connection
                                .prepareCall(prepareCall, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                            final boolean result = statement.execute();
                            if(result) {
                                LOG.info("Executed prepareCall: " + prepareCall);
                            } else {
                                LOG.error("Failed to execute prepareCall: " + prepareCall);
                            }
                        }
                    }
                }

                int count = 0;
                final Instant start = Instant.now();
                final GenericRecord params = c.element();
                if(isDummy(params.getSchema())) {
                    // Received dummy record means not use prepare statement.

                    try (final Statement statement = connection
                            .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                        statement.setFetchSize(DEFAULT_FETCH_SIZE);

                        try (final ResultSet resultSet = statement.executeQuery(query)) {
                            while (resultSet.next()) {
                                final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                                c.output(record);
                                count++;

                                if(count % 1000 == 0) {
                                    LOG.info(String.format("PreparedQuery [%s] reading record count [%d]", query, count));
                                }
                            }
                        }
                        LOG.info(String.format("Finished to read query [%s], total count: [%d]", query, count));
                    }
                } else {
                    // Received not dummy record means received record as prepare statement parameter.

                    try (final PreparedStatement statement = connection
                            .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                        statement.setFetchSize(DEFAULT_FETCH_SIZE);

                        final ParameterMetaData meta = statement.getParameterMetaData();
                        for(int i=0; i<meta.getParameterCount(); i++) {
                            final Schema.Field field = params.getSchema().getFields().get(i);
                            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                            final Object fieldValue = params.get(field.name());

                            JdbcUtil.setStatement(statement, i+1, fieldSchema, fieldValue);
                        }

                        try (final ResultSet resultSet = statement.executeQuery()) {
                            while (resultSet.next()) {
                                final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                                c.output(record);
                                count++;

                                if(count % 1000 == 0) {
                                    LOG.info(String.format("PreparedQuery [%s] reading record count [%d]", statement, count));
                                }
                            }
                        }
                        LOG.info(String.format("Finished to read prepared query [%s], total count: [%d]", statement, count));
                    }

                }

                final long time = Instant.now().getMillis() - start.getMillis();
                LOG.info(String.format("Query took [%d] millisec to execute.", count, time));
            }
        }


        private static Schema createDummySchema() {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
            schemaFields.name(DUMMY_FIELD).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
            return schemaFields.endRecord();
        }

        private static GenericRecord createDummyRecord() {
            final Schema schema = createDummySchema();
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set(DUMMY_FIELD, null);
            return builder.build();
        }

        private static boolean isDummy(final Schema schema) {
            if(schema == null) {
                return false;
            }
            if(schema.getField(DUMMY_FIELD) != null) {
                return true;
            }
            return false;
        }

    }

    public static class JdbcBatchTableSource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private final Parameters parameters;
        private final String timestampAttribute;
        private final String timestampDefault;

        private final String outputSchemaString;
        private final List<String> primaryKeys;

        private JdbcBatchTableSource(
                final Parameters parameters,
                final String timestampAttribute,
                final String timestampDefault,
                final Schema outputSchema) {

            this.parameters = parameters;
            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
            this.outputSchemaString = outputSchema.toString();
            this.primaryKeys = Arrays.asList(outputSchema.getProp("primaryKeys").split(","));
        }

        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {

            final PCollection<String> table = begin
                    .apply("SupplyTable", Create.of(parameters.table).withCoder(StringUtf8Coder.of()));

            final Schema outputSchema = AvroSchemaUtil.convertSchema(outputSchemaString);
            final PCollection<GenericRecord> recordsStartPos = table
                    .apply("ReadTableStartPos", ParDo.of(new TableReadOneDoFn(
                            parameters, outputSchemaString, primaryKeys)))
                    .setCoder(AvroCoder.of(outputSchema));
            final PCollection<GenericRecord> recordsRanges = table
                    .apply("ReadTableRanges", ParDo.of(new TableReadRangeDoFn(
                            parameters, outputSchemaString, primaryKeys)))
                    .setCoder(AvroCoder.of(outputSchema));

            return PCollectionList.of(recordsStartPos).and(recordsRanges)
                    .apply("Union", Flatten.pCollections());
        }

        public abstract static class TableReadDoFn extends DoFn<String, GenericRecord> {

            protected static final int DEFAULT_FETCH_SIZE = 50_000;

            protected final String driver;
            protected final String url;
            protected final String user;
            protected final String password;

            protected final String table;
            protected final List<String> parameterFieldNames;
            protected final String fields;
            protected final List<String> excludeFields;
            protected final Integer fetchSize;

            protected final String outputSchemaString;

            protected transient Schema outputSchema;
            protected transient JdbcUtil.CloseableDataSource dataSource;
            protected transient Connection connection;

            protected transient List<Schema.Field> parameterFields;
            protected transient Map<String, Schema.Field> parameterFieldsMap;

            TableReadDoFn(
                    final Parameters parameters,
                    final String outputSchemaString,
                    final List<String> primaryKeys) {

                this.driver = parameters.driver;
                this.url = parameters.url;
                this.user = parameters.user;
                this.password = parameters.password;

                this.table = parameters.table;
                this.parameterFieldNames = primaryKeys;
                this.fields = parameters.fields;
                this.excludeFields = parameters.excludeFields;
                this.fetchSize = parameters.fetchSize;

                this.outputSchemaString = outputSchemaString;
            }

            protected void setup() throws SQLException {
                this.dataSource = JdbcUtil.createDataSource(this.driver, this.url, user, password, true);
                this.connection = dataSource.getConnection();
                this.outputSchema = AvroSchemaUtil.convertSchema(outputSchemaString);

                this.parameterFields = new ArrayList<>();
                for(final String parameterFieldName : parameterFieldNames) {
                    if(parameterFieldName.contains(":")) {
                        final String[] s = parameterFieldName.split(":");
                        Schema.Field field = outputSchema.getField(s[0]);
                        if(field == null) {
                            field = outputSchema.getField(s[0].toLowerCase());
                            if(field == null) {
                                throw new IllegalStateException("Schema: " + outputSchema.toString() + " does not include field: " + parameterFieldName);
                            }
                        }
                        this.parameterFields.add(field);
                    } else {
                        Schema.Field field = outputSchema.getField(parameterFieldName);
                        if(field == null) {
                            field = outputSchema.getField(parameterFieldName.toLowerCase());
                            if(field == null) {
                                throw new IllegalStateException("Schema: " + outputSchema.toString() + " does not include field: " + parameterFieldName);
                            }
                        }
                        this.parameterFields.add(field);
                    }
                }
                this.parameterFieldsMap = this.parameterFields.stream()
                        .collect(Collectors.toMap(Schema.Field::name, f -> f));
            }

            protected void teardown() throws SQLException {
                if(this.connection != null) {
                    try {
                        this.connection.close();
                    } finally {
                        this.connection = null;
                    }
                }

                if(this.dataSource != null) {
                    try {
                        this.dataSource.close();
                    } catch (IOException e) {
                    } finally {
                        this.dataSource = null;
                    }
                }
            }

            protected void process(
                    final ProcessContext c,
                    final JdbcUtil.IndexPosition startPosition,
                    final JdbcUtil.IndexPosition stopPosition,
                    final RestrictionTracker<JdbcUtil.IndexRange, JdbcUtil.IndexPosition> tracker) throws IOException {

                int lastFetchCount = fetchSize;
                while(lastFetchCount == fetchSize) {

                    if(tracker != null) {
                        if (!tracker.tryClaim(startPosition)) {
                            return;
                        }
                    }

                    final String preparedQuery = JdbcUtil.createSeekPreparedQuery(
                            startPosition,
                            stopPosition,
                            fields,
                            table,
                            parameterFieldNames,
                            fetchSize);
                    try (final PreparedStatement statement = connection.prepareStatement(
                            preparedQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                        statement.setFetchSize(Math.min(DEFAULT_FETCH_SIZE, fetchSize));
                        int paramIndexOffset = JdbcUtil.setStatementParameters(
                                statement, startPosition.getOffsets(), parameterFieldsMap, 1);
                        JdbcUtil.setStatementParameters(
                                statement, stopPosition.getOffsets(), parameterFieldsMap, paramIndexOffset);

                        int count = 0;
                        final Instant start = Instant.now();
                        try (final ResultSet resultSet = statement.executeQuery()) {
                            while (resultSet.next()) {
                                final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                                c.output(record);
                                count++;

                                if(resultSet.isLast()) {
                                    final List<JdbcUtil.IndexOffset> latestOffsets = new ArrayList<>();
                                    for (final Schema.Field field : parameterFields) {
                                        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                                        final Object fieldValue = record.get(field.name());
                                        final Boolean isCaseSensitive = Boolean.valueOf(field.getProp("isCaseSensitive"));
                                        latestOffsets.add(JdbcUtil.IndexOffset.of(field.name(), fieldSchema.getType(), true, fieldValue, isCaseSensitive));
                                    }
                                    startPosition.setIsOpen(true);
                                    startPosition.setOffsets(latestOffsets);
                                    startPosition.setCount(startPosition.getCount() + count);
                                    if(count == 0) {
                                        startPosition.setCompleted(true);
                                    }
                                }
                            }
                        }

                        lastFetchCount = count;
                        final long time = Instant.now().getMillis() - start.getMillis();
                        LOG.info(String.format("Finished to read query [%s], total count: [%d], [%d] millisec", statement, count, time));
                    } catch (SQLException e) {
                        throw new IllegalStateException("Failed to execute query: " + preparedQuery, e);
                    }
                }
            }

            protected JdbcUtil.IndexRange createInitialIndexRange(final List<String> parameterFieldNames) throws SQLException, IOException {

                final String firstFieldName = parameterFieldNames.get(0);
                final String firstFieldMinQuery = String.format("SELECT %s FROM %s ORDER BY %s ASC LIMIT 1", firstFieldName, table, firstFieldName);
                final String firstFieldMaxQuery = String.format("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", firstFieldName, table, firstFieldName);

                // Set startOffset
                final List<JdbcUtil.IndexOffset> indexStartOffsets = new ArrayList<>();
                try(final PreparedStatement statement = connection
                        .prepareStatement(firstFieldMinQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                    if(statement.getMetaData() == null) {
                        throw new IllegalArgumentException("Failed to get schema for query: " + firstFieldMinQuery);
                    }

                    try (final ResultSet resultSet = statement.executeQuery()) {
                        if(!resultSet.next()) {
                            final ResultSetMetaData metaData = resultSet.getMetaData();
                            final Schema schema = ResultSetToRecordConverter.convertSchema(metaData);
                            final Schema.Field field = Optional.ofNullable(schema.getField(firstFieldName))
                                    .orElseGet(() -> schema.getField(firstFieldName.toLowerCase()));
                            if(field == null) {
                                throw new IllegalArgumentException("Not found keyField: " + firstFieldName + " in table: " + table);
                            }
                            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                            final String logicalType = Optional.ofNullable(fieldSchema.getLogicalType()).map(ss -> ss.getName().toLowerCase()).orElse(null);
                            final Boolean isCaseSensitive = Boolean.valueOf(field.getProp("isCaseSensitive"));
                            indexStartOffsets.add(JdbcUtil.IndexOffset.of(
                                    field.name(), AvroSchemaUtil.unnestUnion(field.schema()).getType(), true, null, logicalType, isCaseSensitive));
                            return JdbcUtil.IndexRange.of(
                                    JdbcUtil.IndexPosition.of(indexStartOffsets, true),
                                    JdbcUtil.IndexPosition.of(indexStartOffsets, false));
                        }
                        final GenericRecord record = ResultSetToRecordConverter.convert(resultSet);
                        Schema.Field field = record.getSchema().getField(firstFieldName);
                        if(field == null) {
                            // For PostgreSQL
                            field = record.getSchema().getField(firstFieldName.toLowerCase());
                        }

                        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                        final Object value = record.get(field.name());
                        final String logicalType = Optional.ofNullable(fieldSchema.getLogicalType()).map(s -> s.getName().toLowerCase()).orElse(null);
                        final Boolean isCaseSensitive = Boolean.valueOf(field.getProp("isCaseSensitive"));
                        indexStartOffsets.add(JdbcUtil.IndexOffset.of(field.name(), fieldSchema.getType(), true, value, logicalType, isCaseSensitive));
                    }
                }

                // Set stopOffset
                final List<JdbcUtil.IndexOffset> indexStopOffsets = new ArrayList<>();
                try(final PreparedStatement statement = connection
                        .prepareStatement(firstFieldMaxQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                    if(statement.getMetaData() == null) {
                        throw new IllegalArgumentException("Failed to get schema for query: " + firstFieldMinQuery);
                    }

                    try (final ResultSet resultSet = statement.executeQuery()) {
                        if(!resultSet.next()) {
                            throw new IllegalStateException();
                        }
                        final GenericRecord record = ResultSetToRecordConverter.convert(resultSet);
                        Schema.Field field = record.getSchema().getField(firstFieldName);
                        if(field == null) {
                            // For PostgreSQL
                            field = record.getSchema().getField(firstFieldName.toLowerCase());
                        }

                        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                        final Object value = record.get(field.name());
                        final Boolean isCaseSensitive = Boolean.valueOf(field.getProp("isCaseSensitive"));
                        indexStopOffsets.add(JdbcUtil.IndexOffset.of(field.name(), fieldSchema.getType(), true, value, isCaseSensitive));
                    }
                }

                return JdbcUtil.IndexRange.of(
                        JdbcUtil.IndexPosition.of(indexStartOffsets, true),
                        JdbcUtil.IndexPosition.of(indexStopOffsets, false));
            }
        }

        @DoFn.BoundedPerElement
        public static class TableReadOneDoFn extends TableReadDoFn {

            TableReadOneDoFn(
                    final Parameters parameters,
                    final String outputSchemaString,
                    final List<String> primaryKeys) {

                super(parameters, outputSchemaString, primaryKeys);
            }

            @Setup
            public void setup() throws SQLException {
                super.setup();
            }

            @Teardown
            public void teardown() throws SQLException {
                super.teardown();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws SQLException, IOException {
                final JdbcUtil.IndexRange indexRange = createInitialIndexRange(parameterFieldNames);
                JdbcUtil.IndexPosition startPosition = indexRange.getFrom();
                startPosition.setIsOpen(false);
                final JdbcUtil.IndexPosition stopPosition = JdbcUtil.IndexPosition.of(startPosition.getOffsets(), false);

                process(c, startPosition, stopPosition, null);
            }
        }

        @DoFn.UnboundedPerElement
        public static class TableReadRangeDoFn extends TableReadDoFn {

            private static final int DEFAULT_FETCH_SIZE = 50_000;

            private final boolean enableSplit;
            private final Integer splitSize;

            TableReadRangeDoFn(
                    final Parameters parameters,
                    final String outputSchemaString,
                    final List<String> primaryKeys) {

                super(parameters, outputSchemaString, primaryKeys);
                this.enableSplit = parameters.enableSplit;
                this.splitSize = parameters.splitSize;
            }

            @Setup
            public void setup() throws SQLException {
                super.setup();
            }

            @Teardown
            public void teardown() throws SQLException {
                super.teardown();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final RestrictionTracker<JdbcUtil.IndexRange, JdbcUtil.IndexPosition> tracker)
                    throws SQLException, IOException {

                final JdbcUtil.IndexRange indexRange = tracker.currentRestriction();
                final JdbcUtil.IndexPosition startPosition = indexRange.getFrom();
                final JdbcUtil.IndexPosition stopPosition  = indexRange.getTo();

                process(c, startPosition, stopPosition, tracker);
            }

            @GetInitialRestriction
            public JdbcUtil.IndexRange getInitialRestriction() throws SQLException, IOException {
                final JdbcUtil.IndexRange initialIndexRange = createInitialIndexRange(parameterFieldNames);
                LOG.info("Initial restriction: " + initialIndexRange);
                return initialIndexRange;
            }

            @GetRestrictionCoder
            public Coder<JdbcUtil.IndexRange> getRestrictionCoder() {
                final Coder<JdbcUtil.IndexRange> coder = AvroCoder.of(JdbcUtil.IndexRange.class);
                return coder;
            }

            @SplitRestriction
            public void split(
                    @Restriction JdbcUtil.IndexRange restriction,
                    OutputReceiver<JdbcUtil.IndexRange> out) throws Exception {

                if(enableSplit) {
                    final List<JdbcUtil.IndexRange> ranges = JdbcUtil.splitIndexRange(
                            null,
                            restriction.getFrom().getOffsets(),
                            restriction.getTo().getOffsets(),
                            splitSize);
                    LOG.info("Batch split restriction: " + restriction + ". size: " + ranges.size() + " for batch mode");
                    if(ranges.size() < 2) {
                        out.output(restriction);
                    }
                    int i=0;
                    for(final JdbcUtil.IndexRange range : ranges) {
                        final double ratio = restriction.getRatio() / ranges.size();
                        range.setRatio(ratio);
                        out.output(range);
                        LOG.info("Restriction " + i + ": " + range.toString());
                        i++;
                    }
                } else {
                    LOG.info("Not split restriction: " + restriction + " for batch mode");
                    out.output(restriction);
                }
            }

            @NewTracker
            public RestrictionTracker<JdbcUtil.IndexRange, JdbcUtil.IndexPosition> newTracker(
                    @Restriction JdbcUtil.IndexRange restriction) {

                return new IndexRangeTracker(this.enableSplit, restriction, 0L);
            }

            //@GetSize
            public double getSize(@Restriction JdbcUtil.IndexRange restriction) throws Exception {
                return 0.5D;//getRecordCountAndSize(file, restriction).getSize();
            }

        }

        public static class IndexRangeTracker
                extends RestrictionTracker<JdbcUtil.IndexRange, JdbcUtil.IndexPosition>
                implements RestrictionTracker.HasProgress {

            private final boolean enableSplit;
            protected JdbcUtil.IndexRange range;
            protected JdbcUtil.IndexPosition lastClaimedOffset = null;
            protected JdbcUtil.IndexPosition lastAttemptedOffset = null;

            protected boolean completed;
            protected final long approximateRecordSize;

            IndexRangeTracker(final boolean enableSplit, final JdbcUtil.IndexRange range, final long approximateRecordSize) {
                this.enableSplit = enableSplit;
                this.range = range;
                this.approximateRecordSize = approximateRecordSize;
            }

            @Override
            public boolean tryClaim(final JdbcUtil.IndexPosition position) {
                this.lastAttemptedOffset = position;
                if(position.isOverTo(this.range.getTo())) {
                    LOG.info("Position: " + position + " is over to end: " + this.range.getTo().toString());
                    return false;
                }
                this.lastClaimedOffset = position;
                return true;
            }

            @Override
            public JdbcUtil.IndexRange currentRestriction() {
                return range;
            }

            @Override
            public SplitResult<JdbcUtil.IndexRange> trySplit(double fractionOfRemainder) {
                if(enableSplit) {
                    LOG.info("Try split restriction: " + range.toString());
                    final List<JdbcUtil.IndexRange> newRanges = JdbcUtil
                            .splitIndexRange(null, range.getFrom().getOffsets(), range.getTo().getOffsets(), 2);
                    if(newRanges.size() <= 1) {
                        LOG.info("Failed to split restriction:" + range.toString());
                        return null;
                    }
                    LOG.info("Succeeded to split restriction size: " + newRanges.size());
                    final double ratio = range.getRatio() / 2.0D;
                    final JdbcUtil.IndexRange firstRange = newRanges.get(0);
                    final JdbcUtil.IndexRange secondRange = newRanges.get(1);
                    firstRange.setRatio(ratio);
                    secondRange.setRatio(ratio);
                    LOG.info("Restriction 1: " + firstRange.toString());
                    LOG.info("Restriction 2: " + secondRange.toString());

                    return SplitResult.of(newRanges.get(0), newRanges.get(1));
                }
                LOG.info("Not split restriction: " + this.range.toString());
                return null;            }

            @Override
            public void checkDone() throws IllegalStateException {
                if(completed) {
                    LOG.info("Finished splittable function for range: " + this.range.toString());
                    return;
                }
                if(lastAttemptedOffset == null) {
                    throw new IllegalStateException("Last attempted index offset should not be null. No work was claimed in range.");
                }
            }

            @Override
            public IsBounded isBounded() {
                return IsBounded.BOUNDED;
            }

            @Override
            public Progress getProgress() {
                return Progress.from(0.8, 0.2);
            }
        }

    }

}
