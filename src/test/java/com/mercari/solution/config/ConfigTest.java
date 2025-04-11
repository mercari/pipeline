package com.mercari.solution.config;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;


public class ConfigTest {

    @Test
    public void testYaml() {
        final String configYaml1 = """
                args:
                  writeDisposition: WRITE_APPEND
                  startTimestamp: "2025-01-01T00:00:00Z"
                sources:
                  - name: BigQueryInput
                    module: bigquery
                    parameters:
                      query: |-
                        SELECT
                          *
                        FROM
                          `myproject:mydataset.mytable`
                        WHERE
                          timestamp > TIMESTAMP("${args.startTimestamp}")
                      queryLocation: asia-northeast1
                sinks:
                  - name: BigQueryOutput
                    module: bigquery
                    inputs:
                      - BigQueryInput
                    parameters:
                      table: "yourproject:yourrdataset.yourtable"
                      writeDisposition: ${args.writeDisposition}
                      createDisposition: CREATE_IF_NEEDED
                      method: FILE_LOADS
                      customGcsTempLocation: gs://mybucket/myobject
                """;
        try {
            final Config config = Config.parse(configYaml1, null, Config.Format.yaml, new String[0]);
            final SourceConfig sourceConfig = config.getSources().getFirst();
            final SinkConfig sinkConfig = config.getSinks().getFirst();
            Assert.assertEquals("BigQueryInput", sourceConfig.getName());
            Assert.assertEquals("bigquery", sourceConfig.getModule());
            Assert.assertEquals("SELECT\n" +
                    "  *\n" +
                    "FROM\n" +
                    "  `myproject:mydataset.mytable`\n" +
                    "WHERE\n" +
                    "  timestamp > TIMESTAMP(\"2025-01-01T00:00:00Z\")", sourceConfig.getParameters().get("query").getAsString());
            Assert.assertEquals("asia-northeast1", sourceConfig.getParameters().get("queryLocation").getAsString());

            Assert.assertEquals("BigQueryOutput", sinkConfig.getName());
            Assert.assertEquals("bigquery", sinkConfig.getModule());
            Assert.assertEquals("yourproject:yourrdataset.yourtable", sinkConfig.getParameters().get("table").getAsString());
            Assert.assertEquals("WRITE_APPEND", sinkConfig.getParameters().get("writeDisposition").getAsString());
            Assert.assertEquals("gs://mybucket/myobject", sinkConfig.getParameters().get("customGcsTempLocation").getAsString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTags() {
        final String configTag1 = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "tags": ["tag1"],
                      "parameters": {
                        "from": 1,
                        "to": 10,
                        "type": "int64"
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "select",
                      "module": "select",
                      "inputs": ["create"],
                      "tags": ["tag2"],
                      "parameters": {
                        "select": [
                          { "name": "value" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "debug",
                      "module": "debug",
                      "inputs": ["select"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        try {
            final Config config = Config.parse(configTag1, null, Config.Format.json, new String[0]);
            Assert.assertNull(config.getSystem().getContext());
            Assert.assertNull(config.getSources().getFirst().getIgnore());
            Assert.assertNull(config.getTransforms().getFirst().getIgnore());
            Assert.assertNull(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, "tag1", Config.Format.json, new String[0]);
            Assert.assertEquals("tag1", config.getSystem().getContext());
            Assert.assertFalse(config.getSources().getFirst().getIgnore());
            Assert.assertTrue(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, "tag2", Config.Format.json, new String[0]);
            Assert.assertEquals("tag2", config.getSystem().getContext());
            Assert.assertTrue(config.getSources().getFirst().getIgnore());
            Assert.assertFalse(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, "tag3", Config.Format.json, new String[0]);
            Assert.assertEquals("tag3", config.getSystem().getContext());
            Assert.assertTrue(config.getSources().getFirst().getIgnore());
            Assert.assertTrue(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testGetTemplateArgs() {
        final String[] args = {
                "args.startDate=2021-01-01",
                "args.create=false",
                "args.projectId=myproject",
                "args.table=mytable",
                "args.instanceId=myinstance",
                "databaseId=mydatabaseId",
                "template.tableId"
        };

        final Map<String, String> parameters = Config.extractArgs(args);
        Assert.assertEquals(5, parameters.size());
        Assert.assertEquals("2021-01-01", parameters.get("startDate"));
        Assert.assertEquals("false", parameters.get("create"));
        Assert.assertEquals("myproject", parameters.get("projectId"));
        Assert.assertEquals("mytable", parameters.get("table"));
        Assert.assertEquals("myinstance", parameters.get("instanceId"));
    }

    @Test
    public void testConfigBeamSQL() throws Exception {

        final String configYaml = """
                sources:
                  - name: BigQueryInput
                    module: bigquery
                    parameters:
                      query: |-
                        SELECT
                          BField1, BField2
                        FROM
                          `myproject.mydataset.mytable`
                        WHERE
                          StartDate > DATE("${args.startDate}")
                  - name: SpannerInput
                    module: spanner
                    parameters:
                      projectId: myproject
                      instanceId: myinstance
                      databaseId: mydatabase
                      fields:
                        - SField1
                        - SField2
                transforms:
                  - name: beamsql
                    module: beamsql
                    inputs:
                      - BigQueryInput
                      - SpannerInput
                    parameters:
                      sql: |-
                        SELECT
                          BigQueryInput.BField1 AS Field1, IF(BigQueryInput.BField2 IS NULL, SpannerInput.SField2, BigQueryInput.BField2) AS Field2
                        FROM
                          BigQueryInput
                        LEFT JOIN
                          SpannerInput
                        ON
                          BigQueryInput.BField1 = SpannerInput.SField1
                sinks:
                  - name: SpannerOutput
                    module: spanner
                    input: beamsql
                    parameters:
                      projectId: ${args.projectId}
                      instanceId: ${args.instanceId}
                      databaseId: mydatabase
                      table: ${args.table}
                      createTable: ${args.create}
                """;
        final String[] args = {
                "args.startDate=2021-01-01",
                "args.create=false",
                "args.keyFields=['Field1','Field2']",
                "args.projectId=anotherproject",
                "args.instanceId=anotherinstance",
                "args.table=anothertable"
        };
        final Config config = Config.parse(configYaml, null, Config.Format.yaml, args);

        Assert.assertEquals(2, config.getSources().size());
        Assert.assertEquals(1, config.getTransforms().size());
        Assert.assertEquals(1, config.getSinks().size());

        // Source BigQuery
        final SourceConfig inputBigqueryConfig = config.getSources().stream()
                .filter(s -> s.getName().equals("BigQueryInput"))
                .findAny()
                .orElseThrow();
        Assert.assertEquals("SELECT\n" +
                "  BField1, BField2\n" +
                "FROM\n" +
                "  `myproject.mydataset.mytable`\n" +
                "WHERE\n" +
                "  StartDate > DATE(\"2021-01-01\")", inputBigqueryConfig.getParameters().getAsJsonPrimitive("query").getAsString());

        // Source Spanner
        final SourceConfig inputSpannerConfig = config.getSources().stream()
                .filter(s -> s.getName().equals("SpannerInput"))
                .findAny()
                .orElseThrow();

        // Sink Spanner
        final JsonObject outputSpannerParameters = config.getSinks().stream()
                .filter(s -> s.getName().equals("SpannerOutput"))
                .findAny()
                .orElseThrow()
                .getParameters();

        Assert.assertEquals(
                "anotherproject",
                outputSpannerParameters.get("projectId").getAsString());
        Assert.assertEquals(
                "anotherinstance",
                outputSpannerParameters.get("instanceId").getAsString());
        Assert.assertEquals(
                "mydatabase",
                outputSpannerParameters.get("databaseId").getAsString());
        Assert.assertEquals(
                "anothertable",
                outputSpannerParameters.get("table").getAsString());
        Assert.assertFalse(outputSpannerParameters.get("createTable").getAsBoolean());
    }

}
