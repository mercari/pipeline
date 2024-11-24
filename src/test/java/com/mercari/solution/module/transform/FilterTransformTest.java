package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class FilterTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3],
                        "select": [
                          { "name": "sequence" },
                          { "name": "data", "type": "json", "value": '[{ "fieldA": "value1", "fieldB": "value2" },{ "fieldA": "value3", "fieldB": "value4" }]' },
                          { "name": "message", "func": "struct", "mode": "repeated", "fields": [
                            { "name": "field1", "type": "string", "value": "str1" },
                            { "name": "field2", "type": "string", "value": "str2" }
                          ] }
                        ]
                      },
                      "timestampAttribute": "sequence"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "filter",
                      "module": "filter",
                      "inputs": ["create"],
                      "parameters": {
                        "select": [
                          { "name": "events", "func": "struct", "mode": "repeated", "fields": [
                            { "name": "id", "func": "jsonpath", "field": "data", "path": "$.fieldA" },
                            { "name": "description", "func": "jsonpath", "field": "data", "path": "$.fieldB" }
                          ], "each": "data" }
                        ],
                        "flattenField": "events"
                      }
                    },
                    {
                      "name": "filter2",
                      "module": "filter",
                      "inputs": ["filter"],
                      "parameters": {
                        "select": [
                            { "name": "constantValue", "type": "string", "value": "1234567890" },
                            { "name": "id", "type": "string", "field": "events.id" },
                            { "name": "events", "func": "struct", "mode": "repeated", "fields": [
                              { "name": "name", "type": "string", "value": "events.description" },
                              { "name": "uid", "func": "hash", "text": "myevent#" },
                              { "name": "properties", "func": "struct", "fields": [
                                { "name": "key1", "func": "struct", "fields": [
                                  { "name": "name", "field": "events.id" }
                                ]},
                                { "name": "key2", "func": "struct", "fields": [
                                  { "name": "description", "field": "events.description" }
                                ]}
                              ]}
                            ]}
                          ]
                      }
                    }
                  ]
                }
                """;

        final Config config = MPipeline.loadConfig(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("filter2");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                System.out.println(row);
                count++;
            }
            System.out.println(count);
            //Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();

    }

}
