package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class CreateSourceTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCreateRange() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "from": 1,
                        "to": 100,
                        "select": [
                          { "name": "a", "field": "value" },
                          { "name": "b", "expression": "value % 10", "type": "int64" },
                          { "name": "c", "func": "hash", "field": "b" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = MPipeline.loadConfig(configJson);
        final String[] args = new String[]{};
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config, args);

        final MCollection output = outputs.get("create");

        // Assert output schema
        final Schema outputSchema = output.getSchema();
        for(final Schema.Field field : outputSchema.getFields()) {
            switch (field.getName()) {
                case "a", "b" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.int64);
                case "c" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.string);
            }
        }

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final Long a = row.getAsLong("a");
                final Long b = row.getAsLong("b");
                Assert.assertEquals(a % 10, b, DELTA);
                count++;
            }
            Assert.assertEquals(100, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testCreateElements() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "stringField": "a", "intField": 1, "floatField": 0.15, "boolField": true, "timestampField": "2024-10-10T00:00:00Z" },
                          { "stringField": "b", "intField": 2, "floatField": 1.15, "boolField": false, "timestampField": "2024-10-20T00:00:00Z" },
                          { "stringField": "c", "intField": 3, "floatField": 2.15, "boolField": true, "timestampField": "2024-10-30T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "stringField", "type": "string" },
                          { "name": "intField", "type": "int" },
                          { "name": "floatField", "type": "float" },
                          { "name": "boolField", "type": "boolean" },
                          { "name": "timestampField", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "timestampField"
                    }
                  ]
                }
                """;

        final Config config = MPipeline.loadConfig(configJson);
        final String[] args = new String[]{};
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config, args);

        final MCollection output = outputs.get("create");

        // Assert output schema
        final Schema outputSchema = output.getSchema();
        //Assert.assertEquals(12, outputSchema.countFields());
        for(final Schema.Field field : outputSchema.getFields()) {
            switch (field.getName()) {
                case "stringField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.string);
                case "intField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.int32);
                case "longField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.int64);
                case "floatField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.float32);
                case "doubleField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.float64);
                case "boolField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.bool);
                case "timestampField" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.timestamp);
            }
        }

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                switch (row.getPrimitiveValue("stringField").toString()) {
                    case "a" -> {
                        Assert.assertEquals(true, row.getPrimitiveValue("boolField"));
                        Assert.assertEquals(1, row.getPrimitiveValue("intField"));
                        Assert.assertEquals(0.15F, row.getPrimitiveValue("floatField"));
                        Assert.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-10T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                    case "b" -> {
                        Assert.assertEquals(false, row.getPrimitiveValue("boolField"));
                        Assert.assertEquals(2, row.getPrimitiveValue("intField"));
                        Assert.assertEquals(1.15F, row.getPrimitiveValue("floatField"));
                        Assert.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-20T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                    case "c" -> {
                        Assert.assertEquals(true, row.getPrimitiveValue("boolField"));
                        Assert.assertEquals(3, row.getPrimitiveValue("intField"));
                        Assert.assertEquals(2.15F, row.getPrimitiveValue("floatField"));
                        Assert.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-30T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                }
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

}
