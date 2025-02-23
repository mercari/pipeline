package com.mercari.solution.util.gcp;

import com.google.api.client.googleapis.batch.BatchCallback;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.ArrayMap;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.TableRecordToRowConverter;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;


public class BigQueryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtil.class);

    private static final Pattern PATTERN_TIME_PARTITIONING_TABLE = Pattern.compile("\\$\\d+$");

    private static final String EXTRACT_ALL_TABLE_SCHEMA_QUERY =
            "  SELECT " +
            "    table_name, " +
            "    ARRAY_AGG(c ORDER BY ordinal_position) AS fields " +
            "  FROM " +
            "    `%s`.%s.INFORMATION_SCHEMA.COLUMNS AS c " +
            "  GROUP BY " +
            "    table_name";

    private static final String EXTRACT_PRIMARY_KEY_FIELDS_QUERY =
            "  SELECT " +
            "    table_name, " +
            "    ARRAY_AGG(c.column_name ORDER BY ordinal_position) AS fields " +
            "  FROM " +
            "    `%s`.%s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c " +
            "  GROUP BY " +
            "    table_name";

    public enum WriteFormat {
        json,
        row,
        avro,
        avrofile
    }

    public static com.mercari.solution.module.Schema getTableDefinitionSchema() {
        return com.mercari.solution.module.Schema.builder()
                .withField("tableSpec", com.mercari.solution.module.Schema.FieldType.STRING)
                .withField("tableDescription", com.mercari.solution.module.Schema.FieldType.STRING)
                .withField("jsonClustering", com.mercari.solution.module.Schema.FieldType.STRING)
                .withField("jsonTimePartitioning", com.mercari.solution.module.Schema.FieldType.STRING)
                .withField("shortTableUrn", com.mercari.solution.module.Schema.FieldType.STRING)
                .withField("tableReference", com.mercari.solution.module.Schema.FieldType.element(com.mercari.solution.module.Schema.builder()
                        .withField("projectId", com.mercari.solution.module.Schema.FieldType.STRING)
                        .withField("datasetId", com.mercari.solution.module.Schema.FieldType.STRING)
                        .withField("tableId", com.mercari.solution.module.Schema.FieldType.STRING)
                        .build()))
                .withField("clustering", com.mercari.solution.module.Schema.FieldType.element(com.mercari.solution.module.Schema.builder()
                        .withField("fields", com.mercari.solution.module.Schema.FieldType.array(com.mercari.solution.module.Schema.FieldType.STRING))
                        .build()))
                .withField("timePartitioning", com.mercari.solution.module.Schema.FieldType.element(com.mercari.solution.module.Schema.builder()
                        .withField("type", com.mercari.solution.module.Schema.FieldType.STRING)
                        .withField("field", com.mercari.solution.module.Schema.FieldType.STRING)
                        .withField("expirationMs", com.mercari.solution.module.Schema.FieldType.INT64)
                        .withField("requirePartitionFilter", com.mercari.solution.module.Schema.FieldType.BOOLEAN)
                        .build()))
                .build();
    }

    public static MElement convertToElement(final TableDestination tableDestination) {
        return MElement.builder()
                .withString("tableSpec", tableDestination.getTableSpec())
                .withString("tableDescription", tableDestination.getTableDescription())
                .withString("jsonClustering", tableDestination.getJsonClustering())
                .withString("jsonTimePartitioning", tableDestination.getJsonTimePartitioning())
                .withString("shortTableUrn", tableDestination.getShortTableUrn())
                .withElement("tableReference", MElement.builder()
                        .withString("projectId", tableDestination.getTableReference().getProjectId())
                        .withString("datasetId", tableDestination.getTableReference().getDatasetId())
                        .withString("tableId", tableDestination.getTableReference().getTableId())
                        .build())
                .build();
    }

    public static TableReference getTableReference(final String tableName, final String defaultProjectId) {
        final String[] path = tableName.replaceAll(":", ".").split("\\.");
        if(path.length < 2) {
            throw new IllegalArgumentException("table: " + tableName + " is illegal. table must be {projectId}.{datasetId}.{tableId}");
        }

        final String projectId = path.length == 2 ? defaultProjectId : path[0];
        final String datasetId = path[path.length - 2];
        final String tableId   = path[path.length - 1];

        return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
    }

    public static DatasetReference getDatasetReference(final String datasetName, final String defaultProjectId) {
        final String[] path = datasetName.replaceAll(":", ".").split("\\.");
        if(path.length < 1) {
            throw new IllegalArgumentException("dataset: " + datasetName + " is illegal. table must be {projectId}.{datasetId}");
        }

        final String projectId = path.length == 1 ? defaultProjectId : path[0];
        final String datasetId = path[path.length - 1];

        return new DatasetReference().setProjectId(projectId).setDatasetId(datasetId);
    }

    public static List<Table> getTables(final String project, final String dataset) {
        final Bigquery bigquery = getBigquery();
        try {
            final TableList tableList = bigquery.tables()
                    .list(project, dataset)
                    .execute();
            final List<Table> outputs = new ArrayList<>();
            final BatchCallback<Table, GoogleJsonErrorContainer> batchCallback = new BatchCallback<>() {

                @Override
                public void onSuccess(Table table, HttpHeaders httpHeaders) throws IOException {
                    outputs.add(table);
                }

                @Override
                public void onFailure(GoogleJsonErrorContainer googleJsonErrorContainer, HttpHeaders httpHeaders) throws IOException {
                    System.out.println(googleJsonErrorContainer.getError());
                }
            };

            BatchRequest batch = bigquery.batch();
            for(final TableList.Tables tables : tableList.getTables()) {
                final TableReference tableReference = tables.getTableReference();
                final Bigquery.Tables.Get get = bigquery.tables().get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId());
                batch = batch.queue(get.buildHttpRequest(), Table.class, GoogleJsonErrorContainer.class, batchCallback);
            }

            batch.execute();

            return outputs;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list tables from BigQuery dataset: " + dataset + ", for projectId: " + project, e);
        }
    }

    public static Schema getSchemaFromQuery(final String projectId, final String query) {
        return TableRecordToRowConverter.convertSchema(getTableSchemaFromQuery(projectId, query));
    }

    public static org.apache.avro.Schema getAvroSchemaFromQuery(final String projectId, final String query) {
        return AvroSchemaUtil.convertSchema(getTableSchemaFromQuery(projectId, query));
    }

    public static TableSchema getTableSchemaFromQuery(final String projectId, final String query) {
        final Job job = getQueryDryRunJob(projectId, query);
        return job.getStatistics().getQuery().getSchema();
    }

    private static Job getQueryDryRunJob(final String projectId, final String query) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            final Bigquery bigquery = new Bigquery.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("BigQueryClient")
                    .build();

            final String queryRunProjectId;
            if(projectId != null) {
                queryRunProjectId = projectId;
            } else {
                queryRunProjectId = OptionUtil.getDefaultProject(credential);
            }

            return getQueryDryRunJob(bigquery, queryRunProjectId, query);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dry run query: " + query + ", for projectId: " + projectId, e);
        }
    }

    public static Job getQueryDryRunJob(final Bigquery bigquery, final String projectId, final String query) {
        try {
            return bigquery.jobs().insert(projectId, new Job()
                    .setConfiguration(new JobConfiguration()
                            .setQuery(new JobConfigurationQuery()
                                    .setQuery(query)
                                    .setUseLegacySql(false))
                            .setDryRun(true)))
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException("Failed to dry run query: " + query + ", for projectId: " + projectId, e);
        }
    }

    public static Schema getSchemaFromTable(final String tableName, final String defaultProjectId) {
        return getSchemaFromTable(tableName, defaultProjectId, null);
    }

    public static Schema getSchemaFromTable(final String tableName, final String defaultProjectId, final Collection<String> fields) {
        return TableRecordToRowConverter.convertSchema(getTableSchemaFromTable(tableName, defaultProjectId), fields);
    }

    public static TableSchema getTableSchemaFromTable(final String tableName, final String defaultProjectId) {
        final Bigquery bigquery = getBigquery();
        String queryRunProjectId = null;
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            if(defaultProjectId != null) {
                queryRunProjectId = defaultProjectId;
            } else {
                queryRunProjectId = OptionUtil.getDefaultProject(credential);
            }
            final TableReference tableReference = getTableReference(tableName, queryRunProjectId);

            final Table table = bigquery.tables()
                    .get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId())
                    .execute();
            return table.getSchema();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get schema from BigQuery table: " + tableName + ", for projectId: " + queryRunProjectId, e);
        }
    }

    public static TableSchema getTableSchemaFromTable(final TableReference tableReference) {
        final Bigquery bigquery = getBigquery();
        try {
            final Table table = bigquery.tables()
                    .get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId())
                    .execute();
            return table.getSchema();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get schema from BigQuery table: " + tableReference, e);
        }
    }

    public static Map<String, TableSchema> getTableSchemasFromDataset(final String datasetName) {
        final Bigquery bigquery = getBigquery();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final String queryRunProjectId = OptionUtil.getDefaultProject(credential);

            final String[] strs = datasetName.split("\\.");

            final Map<String, TableSchema> tableSchemas = new HashMap<>();

            final TableList tableList = bigquery.tables().list(strs[0], strs[1]).execute();
            for(final TableList.Tables tables : tableList.getTables()) {
                final TableReference tableReference = tables.getTableReference();
                final Table table = bigquery.tables().get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId()).execute();
                tableSchemas.put(table.getTableReference().getTableId(), table.getSchema());
            }

            return tableSchemas;
        } catch (IOException e) {
            throw new RuntimeException("Failed to get schema from BigQuery dataset: " + datasetName, e);
        }
    }

    public static Map<String, List<String>> getPrimaryKeyFieldsFromDataset(final String datasetName, final String defaultProjectId) {
        final Bigquery bigquery = getBigquery();
        String queryRunProjectId = null;
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            if(defaultProjectId != null) {
                queryRunProjectId = defaultProjectId;
            } else {
                queryRunProjectId = OptionUtil.getDefaultProject(credential);
            }

            final String[] strs = datasetName.split("\\.");

            final String query = String.format(EXTRACT_PRIMARY_KEY_FIELDS_QUERY, strs[0], strs[1]);
            final List<TableRow> tableRows = bigquery.jobs()
                    .query(queryRunProjectId, new QueryRequest()
                            .setQuery(query)
                            .setUseLegacySql(false))
                    .execute()
                    .getRows();

            final Map<String, List<String>> primaryKeyFieldsMap = new HashMap<>();
            for(final TableRow tableRow : tableRows) {
                final List<TableCell> values = (List<TableCell>) tableRow.get("f");
                final String tableName = (String) values.get(0).get("v");
                final List<ArrayMap> fields = (List<ArrayMap>) values.get(1).get("v");
                final List<String> primaryKeyFields = new ArrayList<>();
                for(final ArrayMap field : fields) {
                    System.out.println(field);
                    final String name = (String) field.getValue(0);
                    primaryKeyFields.add(name);
                }
                primaryKeyFieldsMap.put(tableName, primaryKeyFields);
            }
            return primaryKeyFieldsMap;
        } catch (IOException e) {
            throw new RuntimeException("Failed to get schema from BigQuery table: " + datasetName + ", for projectId: " + queryRunProjectId, e);
        }
    }

    private static TableFieldSchema parseTableFieldSchema(String name, String dataType, Boolean nullable) {
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setName(name)
                .setMode(parseMode(dataType, nullable));

        if(dataType.startsWith("ARRAY<")) {
            dataType = dataType.substring(6, dataType.length() - 1);
        }

        if(dataType.startsWith("STRUCT<")) {
            final List<TableFieldSchema> fields = new ArrayList<>();

        }

        return tableFieldSchema;
    }

    private static String parseMode(final String dataType, final Boolean nullable) {
        if(dataType.startsWith("ARRAY<")) {
            return "REPEATED";
        } else if(nullable) {
            return "NULLABLE";
        } else {
            return "REQUIRED";
        }
    }

    private static String parseType(final String dataType) {
        if(dataType.startsWith("STRUCT")) {
            return "STRUCT";
        }
        return switch (dataType) {
            case "STRUCT" -> "";
            case "STRING", "INT64", "FLOAT64", "" -> dataType;
            default -> {
                if(dataType.startsWith("ARRAY<")) {
                    throw new IllegalArgumentException();
                } else if(dataType.startsWith("STRUCT")) {
                    throw new IllegalArgumentException();
                } else {
                    throw new IllegalArgumentException();
                }
            }
        };
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(final TableReference table, final String project) {
        return getTableSchemaFromTableStorage(table, project, null, null);
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(
            final String tableName, final String project, final List<String> fields, final String restriction) {

        final TableReference table = getTableReference(tableName, project);
        return getTableSchemaFromTableStorage(table, project, fields, restriction);
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(
            final TableReference table, final String project, final List<String> fields, final String restriction) {

        final String srcTable = String.format(
                "projects/%s/datasets/%s/tables/%s",
                table.getProjectId(), table.getDatasetId(), table.getTableId());
        try(final BigQueryReadClient client = BigQueryReadClient.create()) {
            final String parent = String.format("projects/%s", project);
            ReadSession.TableReadOptions.Builder options = ReadSession.TableReadOptions.newBuilder();
            if(fields != null) {
                options = options.addAllSelectedFields(fields);
            }
            if(restriction != null) {
                options = options.setRowRestriction(restriction);
            }

            final ReadSession.Builder sessionBuilder =
                    ReadSession.newBuilder()
                            .setTable(srcTable)
                            .setDataFormat(DataFormat.AVRO)
                            .setReadOptions(options);

            final CreateReadSessionRequest.Builder builder =
                    CreateReadSessionRequest.newBuilder()
                            .setParent(parent)
                            .setReadSession(sessionBuilder)
                            .setMaxStreamCount(1);

            final ReadSession session = client.createReadSession(builder.build());
            return new org.apache.avro.Schema.Parser().parse(session.getAvroSchema().getSchema());
        } catch (IOException e) {
            throw new RuntimeException("Failed to get schema from BigQuery storage table: " + srcTable, e);
        }
    }

    public static Bigquery getBigquery() {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Bigquery.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("BigQueryClient")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static List<TableRow> query(final String projectId, final String query) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            final String queryRunProjectId;
            if(projectId != null) {
                queryRunProjectId = projectId;
            } else {
                queryRunProjectId = OptionUtil.getDefaultProject(credential);
            }
            final Bigquery bigquery = new Bigquery.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("BigQueryClient")
                    .build();
            return query(bigquery, queryRunProjectId, query);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dry run query: " + query + ", for projectId: " + projectId, e);
        }
    }

    public static List<TableRow> query(final Bigquery bigquery, final String projectId, final String query) {
        try {
            final QueryResponse queryResponse = bigquery.jobs().query(projectId, new QueryRequest()
                            .setQuery(query)
                            .setUseLegacySql(false))
                    .execute();
            return queryResponse.getRows();
        } catch (IOException e) {
            throw new RuntimeException("Failed to dry run query: " + query + ", for projectId: " + projectId, e);
        }
    }

    public static List<TableRow> queryBatch(final Bigquery bigquery, final QueryRequest request) {
        return queryBatch(bigquery, null, request);
    }

    public static List<TableRow> queryBatch(final Bigquery bigquery, final String projectId, final QueryRequest request) {
        final String queryRunProjectId;
        if(projectId == null) {
            try {
                final Credentials credential = GoogleCredentials.getApplicationDefault();
                queryRunProjectId = OptionUtil.getDefaultProject(credential);
                if(queryRunProjectId == null) {
                    throw new IllegalArgumentException("Failed to get default project from credentials: " + credential.getClass().getSimpleName());
                }
            } catch (Exception e) {
                throw new RuntimeException("failed to get default project", e);
            }
        } else {
            queryRunProjectId = projectId;
        }


        try {
            final QueryResponse queryResponse = bigquery
                    .jobs()
                    .query(queryRunProjectId, request)
                    .execute();
            final List<TableRow> tableRows = new ArrayList<>(queryResponse.getRows());

            String pageToken = queryResponse.getPageToken();
            while(pageToken != null) {
                final GetQueryResultsResponse response = bigquery
                        .jobs()
                        .getQueryResults(queryRunProjectId, queryResponse.getJobReference().getJobId())
                        .setPageToken(queryResponse.getPageToken())
                        .execute();
                pageToken = response.getPageToken();
                tableRows.addAll(response.getRows());
            }
            return tableRows;
        } catch (IOException e) {
            throw new RuntimeException("Failed to dry run query: " + request + ", for projectId: " + queryRunProjectId, e);
        }
    }

    public static Map<String, Object> parseAsPrimitiveValues(final TableSchema tableSchema, final TableRow tableRow) {
        return parseAsPrimitiveValues(tableSchema.getFields(), tableRow);
    }

    private static Map<String, Object> parseAsPrimitiveValues(final List<TableFieldSchema> fields, final Map<String,Object> tableRow) {
        final Map<String, Object> values = new HashMap<>();
        if(tableRow == null || !tableRow.containsKey("f")) {
            return values;
        }
        final List<Map<String,Object>> list = (List<Map<String,Object>>) tableRow.get("f");
        for(int index=0; index<fields.size(); index++) {
            final TableFieldSchema fieldSchema = fields.get(index);
            final Map<String,Object> listValue = list.get(index);
            final Object primitiveValue = switch (fieldSchema.getMode()) {
                case "NULLABLE", "REQUIRED" -> parseAsPrimitiveValue(fieldSchema, listValue);
                case "REPEATED" -> {
                    final List<Object> primitiveValueList = new ArrayList<>();
                    final List<Map<String,Object>> repeatedValues = (List<Map<String,Object>>)listValue.get("v");
                    for(final Map<String,Object> repeatedValue : repeatedValues) {
                        final Object repeatedPrimitiveValue = parseAsPrimitiveValue(fieldSchema, repeatedValue);
                        primitiveValueList.add(repeatedPrimitiveValue);
                    }
                    yield primitiveValueList;
                }
                default -> throw new IllegalArgumentException();
            };
            values.put(fieldSchema.getName(), primitiveValue);
        }
        return values;
    }

    private static Object parseAsPrimitiveValue(TableFieldSchema fieldSchema, Map<String, Object> listValue) {
        if(listValue == null || !listValue.containsKey("v")) {
            return null;
        }
        final Object value = listValue.get("v");
        return switch (fieldSchema.getType().toUpperCase()) {
            case "BOOLEAN" -> Boolean.valueOf((String)value);
            case "STRING", "JSON" -> value.toString();
            case "BYTES" -> Base64.getDecoder().decode(value.toString());
            case "INTEGER" -> Long.valueOf((String)value);
            case "FLOAT" -> Double.valueOf((String)value);
            case "DATE" -> DateTimeUtil.toEpochDay(value.toString());
            case "TIME" -> DateTimeUtil.toMicroOfDay(value.toString());
            case "TIMESTAMP" -> {
                final Double doubleValue = Double.parseDouble(value.toString());
                yield doubleValue.longValue() * 1000L * 1000L;
            }
            case "RECORD" -> parseAsPrimitiveValues(fieldSchema.getFields(), (Map<String,Object>)value);
            default -> throw new IllegalArgumentException();
        };
    }

    public static void deleteTable(final String projectId, final String datasetId, final String tableId) {
        try {
            getBigquery().tables().delete(projectId, datasetId, tableId).execute();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to delete table: %s.%s.%s", projectId, datasetId, tableId), e);
        }
    }

    public static void deleteTable(final Bigquery bigquery, final String projectId, final String datasetId, final String tableId) {
        try {
            bigquery.tables().delete(projectId, datasetId, tableId).execute();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to delete table: %s.%s.%s", projectId, datasetId, tableId), e);
        }
    }

    public static List<JobList.Jobs> listJobs(final Bigquery bigquery, final String projectId, final List<String> stateFilter) {
        final long maxResult = 1000L;
        try {
            final Bigquery.Jobs.List list = bigquery
                    .jobs()
                    .list(projectId)
                    .setStateFilter(stateFilter)
                    .setMaxResults(maxResult);
            JobList jobList = list.execute();
            final List<JobList.Jobs> jobs = new ArrayList<>(jobList.getJobs());
            while(jobList.getNextPageToken() != null) {
                jobList = bigquery.jobs().list(projectId)
                        .setPageToken(jobList.getNextPageToken())
                        .setMaxResults(maxResult)
                        .execute();
                jobs.addAll(jobList.getJobs());
            }
            return jobs;
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to delete table: %s", projectId), e);
        }
    }

    public static Job cancelJob(final Bigquery bigquery, final String projectId, final String jobId) {
        final long maxResult = 1000L;
        try {
            final Bigquery.Jobs.Cancel cancel = bigquery
                    .jobs()
                    .cancel(projectId, jobId);
            final JobCancelResponse cancelResponse = cancel.execute();
            return cancelResponse.getJob();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to cancel job: %s", jobId), e);
        }
    }

    public static List<Job> cancelJobs(final Bigquery bigquery, final String projectId, final Collection<String> jobIds) {
        final List<Job> outputs = new ArrayList<>();
        try {
            final BatchCallback<Job, GoogleJsonErrorContainer> batchCallback = new BatchCallback<>() {

                @Override
                public void onSuccess(Job job, HttpHeaders httpHeaders) throws IOException {
                    outputs.add(job);
                }

                @Override
                public void onFailure(GoogleJsonErrorContainer googleJsonErrorContainer, HttpHeaders httpHeaders) throws IOException {
                    System.out.println(googleJsonErrorContainer.getError());
                }
            };

            final BatchRequest batch = bigquery.batch();
            for(final String jobId : jobIds) {
                final Bigquery.Jobs.Cancel cancel = bigquery
                        .jobs()
                        .cancel(projectId, jobId);
                batch.queue(cancel.buildHttpRequest(), Job.class, GoogleJsonErrorContainer.class, batchCallback);
            }

            batch.execute();

            return outputs;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list tables from BigQuery dataset: " + jobIds + ", for projectId: " + projectId, e);
        }
    }

    public static boolean isPartitioningTable(final String table) {
        return PATTERN_TIME_PARTITIONING_TABLE.matcher(table).find();
    }

    public static WriteFormat getPreferWriteFormat(
            final BigQueryIO.Write.Method method,
            final boolean isStreaming) {

        if(isStreaming) {
            if(BigQueryIO.Write.Method.STREAMING_INSERTS.equals(method)
                    || BigQueryIO.Write.Method.DEFAULT.equals(method)) {

                return WriteFormat.json;
            } else {
                return WriteFormat.row;
            }
        } else {
            if(BigQueryIO.Write.Method.FILE_LOADS.equals(method)
                    || BigQueryIO.Write.Method.DEFAULT.equals(method)) {

                return WriteFormat.avrofile;
                /*
                if(!isNestedSchema(inputSchema)) {
                    return WriteFormat.avrofile;
                } else {
                    return WriteFormat.json;
                }

                 */
            } else {
                return WriteFormat.row;
            }
        }

    }

    public static BigQueryIO.TypedRead.Method getPreferReadMethod(final TableSchema inputSchema) {
        if(isNestedSchema(inputSchema)) {
            return BigQueryIO.TypedRead.Method.EXPORT;
        } else {
            return BigQueryIO.TypedRead.Method.DIRECT_READ;
        }
    }

    private static boolean isNestedSchema(final TableSchema schema) {
        if(schema == null) {
            return false;
        }
        return schema.getFields().stream()
                .anyMatch(s -> "record".equalsIgnoreCase(s.getType()));
    }

    public static Job pollJob(final Bigquery bigquery,
                               final JobReference jobRef,
                               final Sleeper sleeper,
                               final BackOff backoff) {
        do {
            try {
                final Job job = bigquery.jobs()
                        .get(jobRef.getProjectId(), jobRef.getJobId())
                        .setLocation(jobRef.getLocation())
                        .execute();
                if (job == null) {
                    LOG.info("Still waiting for BigQuery job {} to start", jobRef);
                    continue;
                }
                JobStatus status = job.getStatus();
                if (status == null) {
                    LOG.info("Still waiting for BigQuery job {} to enter pending state", jobRef);
                    continue;
                }
                if ("DONE".equals(status.getState())) {
                    LOG.info("BigQuery job {} completed in state DONE", jobRef);
                    return job;
                }
                LOG.info("Still waiting for BigQuery job {}, currently in status {}", jobRef.getJobId(), status);
            } catch (IOException e) {
                LOG.info("Ignore the error and retry polling job status.", e);
            }
        } while (nextBackOff(sleeper, backoff));
        LOG.warn("Unable to poll job status: {}, aborting after reached max .", jobRef.getJobId());
        return null;
    }

    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) {
        try {
            return BackOffUtils.next(sleeper, backoff);
        } catch (InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }

    public static boolean isJobResultSucceeded(final Job job) {
        if(job == null) {
            return false;
        }
        if(job.getStatus().getErrorResult() != null) {
            return false;
        }
        if(job.getStatus().getErrors() != null && !job.getStatus().getErrors().isEmpty()) {
            return false;
        }
        return true;
    }

}
