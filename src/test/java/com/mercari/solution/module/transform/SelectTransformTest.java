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
    public void testStatelessSelect() throws IOException {
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
                        - field_string: string_value4
                          field_long: 40
                          field_ts: "2025-01-01T00:00:03Z"
                        - field_string: string_value5
                          field_long: 50
                          field_ts: "2025-01-01T00:00:04Z"
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
                        - name: field_long_renamed
                          field: field_long
                        - name: field_long_casted_string
                          type: string
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
                          field_nested:
                            field_string: nested_string_value1
                            field_long: -10
                        - field_string: string_value2
                          field_long: 20
                          field_ts: "2025-01-01T00:00:01Z"
                          field_nested:
                            field_string: nested_string_value2
                            field_long: -20
                        - field_string: string_value3
                          field_long: 30
                          field_ts: "2025-01-01T00:00:02Z"
                          field_nested:
                            field_string: nested_string_value3
                            field_long: -30
                        - field_string: string_value4
                          field_long: 40
                          field_ts: "2025-01-01T00:00:03Z"
                          field_nested:
                            field_string: nested_string_value4
                            field_long: -40
                        - field_string: string_value5
                          field_long: 50
                          field_ts: "2025-01-01T00:00:04Z"
                          field_nested:
                            field_string: nested_string_value5
                            field_long: -50
                    schema:
                      fields:
                        - name: field_string
                          type: string
                        - name: field_long
                          type: int64
                        - name: field_ts
                          type: timestamp
                        - name: field_nested
                          type: record
                          fields:
                            - name: field_string
                              type: string
                            - name: field_long
                              type: int64
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: field_long_sum_count3
                          func: sum
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_sum_duration3
                          func: sum
                          field: field_long
                          range:
                            duration: 2
                        - name: field_long_avg_count3
                          func: avg
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_min_count3
                          func: min
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_argmin_count3
                          func: argmin
                          field: field_string
                          comparingField: field_long
                          range:
                            count: 3
                        - name: field_long_string_array_agg_count3
                          func: array_agg
                          fields:
                            - field_long
                            - field_string
                          range:
                            count: 3
                        - name: field_nested_long_sum_count3
                          func: sum
                          field: field_nested.field_long
                          range:
                            count: 3
                        - name: field_sum_long_sum_count3
                          expression: "field_long_sum_count3 * 2"
                        - name: lag_long
                          func: lag
                          expression: "(field_long[2] - field_long[0]) / (2 * field_long[0])"
                        - name: field_long_array_agg_count3
                          func: array_agg
                          field: field_long
                          range:
                            count: 3
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
