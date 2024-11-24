package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class PartitionTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test1() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create1",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [3, 0, 2, 1]
                      },
                      "timestampAttribute": "sequence"
                    },
                    {
                      "name": "create2",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [5, 1, 4, 3]
                      },
                      "timestampAttribute": "sequence"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "partition",
                      "module": "partition",
                      "inputs": ["create1", "create2"],
                      "parameters": {
                        "exclusive": true,
                        "partitions": [
                          {
                            "name": "output1",
                            "filter": [
                              { "key": "value", "op": "in", "value": [1, 2, 3] }
                            ]
                          },
                          {
                            "name": "output2",
                            "filter": [
                              { "key": "value", "op": "in", "value": [4, 5, 6] }
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

        final MCollection output1 = outputs.get("partition.output1");
        PAssert.that(output1.getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assert.assertTrue((long)element.getPrimitiveValue("value") > 0 && (long)element.getPrimitiveValue("value") < 4);
                count++;
            }
            Assert.assertEquals(5, count);
            return null;
        });

        final MCollection output2 = outputs.get("partition.output2");
        PAssert.that(output2.getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assert.assertTrue((long)element.getPrimitiveValue("value") > 3 && (long)element.getPrimitiveValue("value") < 7);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }
}
