package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class AggregationTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testAggregateGroupFields() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create1",
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
                    },
                    {
                      "name": "create2",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "stringField": "a", "longField": 1, "doubleField": 0.15, "dateField": "2024-01-01", "timestampField": "2024-10-10T00:00:00Z" },
                          { "stringField": "b", "longField": 2, "doubleField": 1.15, "dateField": "2024-02-01", "timestampField": "2024-10-20T00:00:00Z" },
                          { "stringField": "c", "longField": 3, "doubleField": 2.15, "dateField": "2024-03-01", "timestampField": "2024-10-30T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "stringField", "type": "string" },
                          { "name": "longField", "type": "int" },
                          { "name": "doubleField", "type": "float" },
                          { "name": "dateField", "type": "date" },
                          { "name": "timestampField", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "timestampField"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "aggregation",
                      "module": "aggregation",
                      "inputs": ["create1", "create2"],
                      "parameters": {
                        "groupFields": ["stringField"],
                        "aggregations": [
                          {
                            "input": "create1",
                            "fields": [
                              { "name": "count1", "op": "count" },
                              { "name": "sum1", "op": "sum", "field": "floatField" },
                              { "name": "max1", "op": "max", "field": "intField" },
                              { "name": "min1", "op": "min", "field": "floatField" },
                              { "name": "last1", "op": "last", "field": "timestampField" },
                              { "name": "first1", "op": "first", "field": "stringField" },
                              { "name": "lastBool1", "op": "last", "field": "boolField" }
                            ]
                          },
                          {
                            "input": "create2",
                            "fields": [
                              { "name": "count2", "op": "count" },
                              { "name": "sum2", "op": "sum", "field": "doubleField" },
                              { "name": "max2", "op": "max", "field": "longField" },
                              { "name": "min2", "op": "min", "field": "doubleField" },
                              { "name": "last2", "op": "last", "field": "dateField" },
                              { "name": "first2", "op": "first", "field": "timestampField" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = MPipeline.loadConfig(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("aggregation");

        // Assert output schema
        final com.mercari.solution.module.Schema outputSchema = output.getSchema();
        //Assert.assertEquals(12, outputSchema.countFields());
        for(final com.mercari.solution.module.Schema.Field field : outputSchema.getFields()) {
            /*
            switch (field.getName()) {
                case "count1", "count2" -> Assert.assertEquals(field.getFieldType().getType(), com.mercari.solution.module.Schema.Type.int64);
                case "sum1", "sum2" -> Assert.assertEquals(field.getFieldType().getType(), com.mercari.solution.module.Schema.Type.int64);
                case "max1", "max2" -> Assert.assertEquals(field.getFieldType().getType(), com.mercari.solution.module.Schema.Type.int64);
                case "min1", "min2" -> Assert.assertEquals(field.getFieldType().getType(), com.mercari.solution.module.Schema.Type.int64);
                case "first1", "first2" -> Assert.assertEquals(field.getFieldType().getType(), Schema.Type.int64);
            }

             */
        }

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                System.out.println(row.convert(outputSchema, DataType.ROW));
                count++;
            }
            System.out.println(count);
            //Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

}
