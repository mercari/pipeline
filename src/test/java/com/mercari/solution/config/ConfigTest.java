package com.mercari.solution.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class ConfigTest {

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
        final Config config = Config.parse(configJson, args, true);

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
