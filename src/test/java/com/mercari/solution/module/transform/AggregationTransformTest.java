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
                          { "stringField": "a", "intField": 1, "floatField": 0.15, "boolField": true,  "timestampField": "2024-10-10T00:00:00Z" },
                          { "stringField": "b", "intField": 2, "floatField": 1.15, "boolField": false, "timestampField": "2024-10-20T00:00:00Z" },
                          { "stringField": "c", "intField": 3, "floatField": 2.15, "boolField": true,  "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "d", "intField": 4, "floatField": 2.15, "boolField": false, "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "e", "intField": 5, "floatField": 4.15, "boolField": true,  "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "f", "intField": 6, "floatField":-5.15, "boolField": false, "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "g", "intField": 7, "floatField":-6.15, "boolField": true,  "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "h", "intField": 8, "floatField":-7.15, "boolField": false, "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "i", "intField": 9, "floatField":-8.15, "boolField": true,  "timestampField": "2024-10-30T00:00:00Z" },
                          { "stringField": "j", "intField":10, "floatField":    0, "boolField": false, "timestampField": "2024-10-30T00:00:00Z" }
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
                      "name": "aggregation1",
                      "module": "aggregation",
                      "inputs": ["create1"],
                      "parameters": {
                        "groupFields": [],
                        "aggregations": [
                          {
                            "input": "create1",
                            "fields": [
                              { "name": "count", "op": "count" },
                              { "name": "sumInt", "op": "sum", "field": "intField" },
                              { "name": "maxFloat", "op": "max", "field": "floatField" },
                              { "name": "minFloat", "op": "min", "field": "floatField" },
                              { "name": "lastString", "op": "last", "field": "stringField" },
                              { "name": "firsts", "op": "first", "fields": ["stringField","intField","floatField","timestampField"] },
                              { "name": "argmaxString", "op": "argmax", "field": "stringField", "comparingField": "floatField" },
                              { "name": "argminString", "op": "argmin", "fields": ["stringField","intField","boolField","floatField"], "comparingField": "floatField" },
                              { "name": "arrayAggSingle", "op": "array_agg", "field": "intField" },
                              { "name": "arrayAggMulti", "op": "array_agg", "fields": ["stringField", "intField", "floatField"] }
                            ]
                          }
                        ]
                      }
                    },
                    {
                      "name": "aggregation2",
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

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("aggregation1");

        // Assert output schema
        final com.mercari.solution.module.Schema outputSchema = output.getSchema();
        System.out.println(outputSchema);
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
