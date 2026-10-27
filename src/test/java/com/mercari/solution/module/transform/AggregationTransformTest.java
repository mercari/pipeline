package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
                              { "name": "argmaxString", "op": "arg_max", "field": "stringField", "comparingField": "floatField" },
                              { "name": "argminString", "op": "arg_min", "fields": ["stringField","intField","boolField","floatField"], "comparingField": "floatField" },
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

    @Test
    public void testLateElementsInGlobalWindowSelectLatestInputTimestamp() throws Exception {
        final Instant base = Instant.parse("2026-10-22T11:00:00Z");
        final List<Instant> times = List.of(base, base.plus(Duration.standardSeconds(30)), base.minus(Duration.standardSeconds(30)));
        final TestStream<MElement> events = TestStream.create(ElementCoder.of(EVENT_SCHEMA))
                .advanceWatermarkTo(base.plus(Duration.standardMinutes(10)))
                .addElements(event(times.get(0)), event(times.get(1)), event(times.get(2)))
                .advanceWatermarkToInfinity();

        assertSelectedEventTimestamps(applyAggregation(events, "LATEST"), times, false, true);
        pipeline.run();
    }

    @Test
    public void testLateElementsInGlobalWindowSelectEarliestInputTimestamp() throws Exception {
        final Instant base = Instant.parse("2026-10-22T11:00:00Z");
        final List<Instant> times = List.of(base, base.minus(Duration.standardSeconds(30)), base.plus(Duration.standardSeconds(30)));
        final TestStream<MElement> events = TestStream.create(ElementCoder.of(EVENT_SCHEMA))
                .advanceWatermarkTo(base.plus(Duration.standardMinutes(10)))
                .addElements(event(times.get(0)), event(times.get(1)), event(times.get(2)))
                .advanceWatermarkToInfinity();

        assertSelectedEventTimestamps(applyAggregation(events, "EARLIEST"), times, true, true);
        pipeline.run();
    }

    @Test
    public void testOnTimeElementInGlobalWindowKeepsPaneTimestamp() throws Exception {
        final Instant base = Instant.parse("2026-10-22T11:00:00Z");
        final List<Instant> times = List.of(base);
        final TestStream<MElement> events = TestStream.create(ElementCoder.of(EVENT_SCHEMA))
                .addElements(event(times.get(0)))
                .advanceWatermarkToInfinity();

        assertSelectedEventTimestamps(applyAggregation(events, "LATEST"), times, false, false);
        pipeline.run();
    }

    private static final Schema EVENT_SCHEMA = Schema.of(List.of(
            Schema.Field.of("user", Schema.FieldType.STRING)));

    private static TimestampedValue<MElement> event(final Instant timestamp) {
        return TimestampedValue.of(MElement.of(EVENT_SCHEMA, Map.of("user", "a"), timestamp.getMillis()), timestamp);
    }

    private PCollection<MElement> applyAggregation(final TestStream<MElement> events, final String timestampCombiner) throws Exception {
        final String configJson = """
                {
                  "transforms": [
                    {
                      "name": "aggregation",
                      "module": "aggregation",
                      "inputs": ["events"],
                      "parameters": {
                        "groupFields": ["user"],
                        "window": { "type": "global", "timestampCombiner": "%s" },
                        "trigger": { "type": "repeatedly", "foreverTrigger": { "type": "afterPane", "elementCountAtLeast": 1 } },
                        "accumulationMode": "accumulating",
                        "aggregations": [
                          { "input": "events", "fields": [ { "name": "count", "op": "count" } ] }
                        ],
                        "select": [
                          { "name": "count", "field": "count" },
                          { "name": "eventTimestamp", "func": "event_timestamp" }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(timestampCombiner);

        final TransformConfig transformConfig = Config.load(configJson).getTransforms().getFirst();
        final Transform transform = Transform.create(
                transformConfig, pipeline.getOptions(), List.of(), List.of(), MErrorHandler.empty());
        return MCollectionTuple
                .of("events", pipeline.apply(events), EVENT_SCHEMA)
                .apply("aggregation", transform)
                .getSinglePCollection();
    }

    // A pane holding the first n elements must select the max (or min) of their timestamps if it was emitted
    // at the end of the global window, and its own pane timestamp otherwise.
    private static void assertSelectedEventTimestamps(
            final PCollection<MElement> outputs,
            final List<Instant> times,
            final boolean earliest,
            final boolean late) {

        PAssert.that(outputs).satisfies(elements -> {
            int paneTimestampCount = 0;
            int endOfWindowCount = 0;
            for(final MElement element : elements) {
                final int n = ((Long) element.getPrimitiveValue("count")).intValue();
                final long selectedMicros = (Long) element.getPrimitiveValue("eventTimestamp");
                final Instant paneTimestamp = element.getTimestamp();
                final Instant expected;
                if(paneTimestamp.isBefore(GlobalWindow.INSTANCE.maxTimestamp())) {
                    expected = paneTimestamp;
                    paneTimestampCount++;
                } else {
                    endOfWindowCount++;
                    final Comparator<Instant> order = Comparator.naturalOrder();
                    expected = earliest
                            ? Collections.min(times.subList(0, n), order)
                            : Collections.max(times.subList(0, n), order);
                }
                Assert.assertEquals(expected.getMillis() * 1000L, selectedMicros);
            }
            if(late) {
                Assert.assertEquals(0, paneTimestampCount);
                Assert.assertTrue(endOfWindowCount > 0);
            } else {
                Assert.assertTrue(paneTimestampCount > 0);
            }
            return null;
        });
    }

}
