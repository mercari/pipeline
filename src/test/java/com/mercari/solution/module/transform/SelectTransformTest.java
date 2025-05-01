package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class SelectTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testStatefulSelect() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    timestampAttribute: field_ts
                    parameters:
                      type: element
                      elements:
                        - field_string: string_value1
                          field_long: 10
                          field_ts: "2025-01-01T00:00:00Z"
                        - field_string: string_value2
                          field_long: 20
                          field_ts: "2025-01-01T00:00:01Z"
                        - field_string: string_value3
                          field_long: 30
                          field_ts: "2025-01-01T00:00:02Z"
                    schema:
                      fields:
                        - name: field_string
                          type: string
                        - name: field_long
                          type: int64
                        - name: field_ts
                          type: timestamp
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: field_long_sum
                          func: sum
                          field: field_long
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("select");

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
