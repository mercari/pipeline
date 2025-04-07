package com.mercari.solution.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;


public class ConfigTest {

    @Test
    public void testYaml() {
        final String configYaml1 = """
                args:
                  writeDisposition: WRITE_APPEND
                sources:
                  - name: BigQueryInput
                    module: bigquery
                    parameters:
                      query: |-
                        SELECT *
                        FROM `myproject:mydataset.mytable`
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
            final Config config = Config.parse(configYaml1, Set.of(), new String[0], false);
            final SourceConfig sourceConfig = config.getSources().getFirst();
            final SinkConfig sinkConfig = config.getSinks().getFirst();
            Assert.assertEquals("BigQueryInput", sourceConfig.getName());
            Assert.assertEquals("bigquery", sourceConfig.getModule());
            Assert.assertEquals("SELECT *\n" +
                    "FROM `myproject:mydataset.mytable`", sourceConfig.getParameters().get("query").getAsString());
            Assert.assertEquals("asia-northeast1", sourceConfig.getParameters().get("queryLocation").getAsString());

            Assert.assertEquals("BigQueryOutput", sinkConfig.getName());
            Assert.assertEquals("bigquery", sinkConfig.getModule());
            Assert.assertEquals("yourproject:yourrdataset.yourtable", sinkConfig.getParameters().get("table").getAsString());
            Assert.assertEquals("${args.writeDisposition}", sinkConfig.getParameters().get("writeDisposition").getAsString());
            Assert.assertEquals("gs://mybucket/myobject", sinkConfig.getParameters().get("customGcsTempLocation").getAsString());

            System.out.println(config);
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
            final Config config = Config.parse(configTag1, Set.of(), new String[0], false);
            Assert.assertEquals(Set.of(), config.getTags());
            Assert.assertNull(config.getSources().getFirst().getIgnore());
            Assert.assertNull(config.getTransforms().getFirst().getIgnore());
            Assert.assertNull(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, Set.of("tag1"), new String[0], false);
            Assert.assertEquals(Set.of("tag1"), config.getTags());
            Assert.assertFalse(config.getSources().getFirst().getIgnore());
            Assert.assertTrue(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, Set.of("tag1", "tag2"), new String[0], false);
            Assert.assertEquals(Set.of("tag1", "tag2"), config.getTags());
            Assert.assertFalse(config.getSources().getFirst().getIgnore());
            Assert.assertFalse(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            final Config config = Config.parse(configTag1, Set.of("tag1", "tag2", "tag3"), new String[0], false);
            Assert.assertEquals(Set.of("tag1", "tag2", "tag3"), config.getTags());
            Assert.assertFalse(config.getSources().getFirst().getIgnore());
            Assert.assertFalse(config.getTransforms().getFirst().getIgnore());
            Assert.assertTrue(config.getSinks().getFirst().getIgnore());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testGetTemplateArgs() {
        final String[] args = {
                "template.startDate=2021-01-01",
                "template.create=false",
                "template.array1=[1,2,3]",
                "template.array2=['a','b','c']",
                "SpannerInput.projectId=ohmyproject",
                "SpannerInput.table=ohmytable",
                "SpannerOutput.instanceId=ohmyinstance",
        };

        final Map<String, Object> parameters = Config.getTemplateArgs(args);
        Assert.assertEquals(4, parameters.size());
        Assert.assertEquals("2021-01-01", parameters.get("startDate"));
        Assert.assertEquals(false, parameters.get("create"));
        Assert.assertEquals(Arrays.asList(1L,2L,3L), parameters.get("array1"));
        Assert.assertEquals(Arrays.asList("a","b","c"), parameters.get("array2"));
    }

    @Test
    public void testConfigBeamSQL() throws Exception {

        final String templateFilePath = "config/beamsql-join-bigquery-and-spanner-to-spanner.json";
        final String configJson = ResourceUtil.getResourceFileAsString(templateFilePath);
        final String[] args = {
                "template.startDate=2021-01-01",
                "template.create=false",
                "template.keyFields=['Field1','Field2']",
                "SpannerInput.projectId=ohmyproject",
                "SpannerInput.table=ohmytable",
                "SpannerOutput.instanceId=ohmyinstance"
        };
        final Config config = Config.parse(configJson, new HashSet<>(), args, true);

        Assert.assertEquals(2, config.getSources().size());
        Assert.assertEquals(1, config.getTransforms().size());
        Assert.assertEquals(1, config.getSinks().size());

        // Source BigQuery
        final SourceConfig inputBigqueryConfig = config.getSources().stream()
                .filter(s -> s.getName().equals("BigQueryInput"))
                .findAny()
                .orElseThrow();
        Assert.assertEquals(3, inputBigqueryConfig.getArgs().size());
        Assert.assertEquals("2021-01-01", inputBigqueryConfig.getArgs().get("startDate"));
        Assert.assertEquals(false, inputBigqueryConfig.getArgs().get("create"));
        Assert.assertEquals(Arrays.asList("Field1","Field2"), inputBigqueryConfig.getArgs().get("keyFields"));

        final JsonObject inputBigQueryParameters = inputBigqueryConfig.getParameters();
        Assert.assertTrue(inputBigQueryParameters.has("query"));
        Assert.assertEquals(
                "SELECT BField1, BField2 FROM `myproject.mydataset.mytable` WHERE StartDate > DATE('2021-01-01')",
                inputBigQueryParameters.get("query").getAsString());

        // Source Spanner
        final SourceConfig inputSpannerConfig = config.getSources().stream()
                .filter(s -> s.getName().equals("SpannerInput"))
                .findAny()
                .orElseThrow();
        Assert.assertEquals(3, inputSpannerConfig.getArgs().size());
        Assert.assertEquals("2021-01-01", inputSpannerConfig.getArgs().get("startDate"));
        Assert.assertEquals(false, inputSpannerConfig.getArgs().get("create"));
        Assert.assertEquals(Arrays.asList("Field1","Field2"), inputBigqueryConfig.getArgs().get("keyFields"));

        final JsonObject inputSpannerParameters = inputSpannerConfig.getParameters();
        Assert.assertTrue(inputSpannerParameters.has("projectId"));
        Assert.assertTrue(inputSpannerParameters.has("instanceId"));
        Assert.assertTrue(inputSpannerParameters.has("databaseId"));
        Assert.assertTrue(inputSpannerParameters.has("table"));
        Assert.assertEquals(
                "ohmyproject",
                inputSpannerParameters.get("projectId").getAsString());
        Assert.assertEquals(
                "myinstance",
                inputSpannerParameters.get("instanceId").getAsString());
        Assert.assertEquals(
                "mydatabase",
                inputSpannerParameters.get("databaseId").getAsString());
        Assert.assertEquals(
                "ohmytable",
                inputSpannerParameters.get("table").getAsString());

        // Transform BeamSQL
        final TransformConfig beamsqlConfig = config.getTransforms().stream()
                .filter(s -> s.getName().equals("beamsql"))
                .findAny()
                .orElseThrow();
        Assert.assertEquals(3, inputSpannerConfig.getArgs().size());
        Assert.assertEquals("2021-01-01", inputSpannerConfig.getArgs().get("startDate"));
        Assert.assertEquals(false, inputSpannerConfig.getArgs().get("create"));
        Assert.assertEquals(Arrays.asList("Field1","Field2"), inputBigqueryConfig.getArgs().get("keyFields"));

        final JsonObject beamsqlParameters = beamsqlConfig.getParameters();
        Assert.assertEquals(
                "SELECT BigQueryInput.BField1 AS Field1, IF(BigQueryInput.BField2 IS NULL, SpannerInput.SField2, BigQueryInput.BField2) AS Field2 FROM BigQueryInput LEFT JOIN SpannerInput ON BigQueryInput.BField1 = SpannerInput.SField1 WHERE BigQueryInput.Date = DATE('2021-01-01')",
                beamsqlParameters.get("sql").getAsString());

        // Sink Spanner
        final JsonObject outputSpannerParameters = config.getSinks().stream()
                .filter(s -> s.getName().equals("SpannerOutput"))
                .findAny()
                .orElseThrow()
                .getParameters();

        Assert.assertTrue(outputSpannerParameters.has("projectId"));
        Assert.assertTrue(outputSpannerParameters.has("instanceId"));
        Assert.assertTrue(outputSpannerParameters.has("databaseId"));
        Assert.assertTrue(outputSpannerParameters.has("table"));
        Assert.assertTrue(outputSpannerParameters.has("createTable"));
        Assert.assertEquals(
                "anotherproject",
                outputSpannerParameters.get("projectId").getAsString());
        Assert.assertEquals(
                "ohmyinstance",
                outputSpannerParameters.get("instanceId").getAsString());
        Assert.assertEquals(
                "anotherdatabase",
                outputSpannerParameters.get("databaseId").getAsString());
        Assert.assertEquals(
                "anothertable",
                outputSpannerParameters.get("table").getAsString());
        Assert.assertFalse(outputSpannerParameters.get("createTable").getAsBoolean());

        final List<String> keyFields = new ArrayList<>();
        for(JsonElement element : outputSpannerParameters.get("keyFields").getAsJsonArray()) {
            keyFields.add(element.getAsString());
        }

        Assert.assertEquals(Arrays.asList("Field1","Field2"), keyFields);
    }

}
