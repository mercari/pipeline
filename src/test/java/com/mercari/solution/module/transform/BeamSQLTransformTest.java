package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

public class BeamSQLTransformTest {

    private static final double DELTA = 1e-15;

    /*
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMathUDFs() {
        testMathUDFs("zetasql");
        testMathUDFs("calcite");
    }

    @Test
    public void testArrayUDFs() {
        testArrayUDFs("zetasql");
        testArrayUDFs("calcite");
    }

    @Test
    public void testUDAFs() {
        //testUDAFs("zetasql");
        testUDAFs("calcite");
    }

    private void testMathUDFs(final String planner) {
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsqlTestMathUDFs");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT stringField, MDT_GREATEST_INT64(longFieldA, longFieldB) AS longFieldMax, MDT_LEAST_INT64(longFieldA, longFieldB) AS longFieldMin, MDT_GREATEST_FLOAT64(doubleFieldA, doubleFieldB) AS doubleFieldMax, MDT_LEAST_FLOAT64(doubleFieldA, doubleFieldB) AS doubleFieldMin, MDT_GENERATE_UUID() AS uuidField FROM rowInput");
        beamsqlParameters.addProperty("planner", planner);
        configBeamSql.setParameters(beamsqlParameters);

        final Schema schema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longFieldA", Schema.FieldType.INT64.withNullable(true))
                .addField("longFieldB", Schema.FieldType.INT64.withNullable(true))
                .addField("doubleFieldA", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("doubleFieldB", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("longFieldA", 1L)
                .withFieldValue("longFieldB", 2L)
                .withFieldValue("doubleFieldA", 0.1D)
                .withFieldValue("doubleFieldB", 0.2D)
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("longFieldA", 1L)
                .withFieldValue("longFieldB", null)
                .withFieldValue("doubleFieldA", 0.1D)
                .withFieldValue("doubleFieldB", null)
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:03.000Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("stringField", "c")
                .withFieldValue("longFieldA", null)
                .withFieldValue("longFieldB", 2L)
                .withFieldValue("doubleFieldA", null)
                .withFieldValue("doubleFieldB", 0.2D)
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:03.000Z"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("stringField", "d")
                .withFieldValue("longFieldA", null)
                .withFieldValue("longFieldB", null)
                .withFieldValue("doubleFieldA", null)
                .withFieldValue("doubleFieldB", null)
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(row1, row2, row3, row4))
                .setRowSchema(schema);
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, schema);
        final FCollection<Row> results = BeamSQLTransform.transform(Arrays.asList(fCollection), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                Assert.assertNotNull(row.getString("uuidField"));
                if("a".equals(row.getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(row.getInt64("longFieldMax")).longValue());
                    Assert.assertEquals(1L, Objects.requireNonNull(row.getInt64("longFieldMin")).longValue());
                    Assert.assertEquals(0.2D, Objects.requireNonNull(row.getDouble("doubleFieldMax")), DELTA);
                    Assert.assertEquals(0.1D, Objects.requireNonNull(row.getDouble("doubleFieldMin")), DELTA);
                } else if("b".equals(row.getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(row.getInt64("longFieldMax")).longValue());
                    Assert.assertEquals(1L, Objects.requireNonNull(row.getInt64("longFieldMin")).longValue());
                    Assert.assertEquals(0.1D, Objects.requireNonNull(row.getDouble("doubleFieldMax")), DELTA);
                    Assert.assertEquals(0.1D, Objects.requireNonNull(row.getDouble("doubleFieldMin")), DELTA);
                } else if("c".equals(row.getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(row.getInt64("longFieldMax")).longValue());
                    Assert.assertEquals(2L, Objects.requireNonNull(row.getInt64("longFieldMin")).longValue());
                    Assert.assertEquals(0.2D, Objects.requireNonNull(row.getDouble("doubleFieldMax")), DELTA);
                    Assert.assertEquals(0.2D, Objects.requireNonNull(row.getDouble("doubleFieldMin")), DELTA);
                } else if("d".equals(row.getString("stringField"))) {
                    Assert.assertNull(row.getInt64("longFieldMax"));
                    Assert.assertNull(row.getInt64("longFieldMin"));
                    Assert.assertNull(row.getInt64("doubleFieldMax"));
                    Assert.assertNull(row.getInt64("doubleFieldMin"));
                }
            }
            Assert.assertEquals(4, count);
            return null;
        });

        pipeline.run();
    }

    private void testArrayUDFs(final String planner) {
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsqlTestArrayUDFs");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT stringField, MDT_CONTAINS_ALL_INT64(longFieldA, longFieldB) AS cl, MDT_CONTAINS_ALL_STRING(stringFieldA, stringFieldB) AS cs FROM rowInput");
        beamsqlParameters.addProperty("planner", planner);
        configBeamSql.setParameters(beamsqlParameters);

        final Schema schema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longFieldA", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
                .addField("longFieldB", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
                .addField("stringFieldA", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
                .addField("stringFieldB", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("longFieldA", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("longFieldB", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("stringFieldA", Arrays.asList("a", "b", "c", "d", "e"))
                .withFieldValue("stringFieldB", Arrays.asList("a", "b", "c", "d", "e"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("longFieldA", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("longFieldB", Arrays.asList(1L, 3L, 5L))
                .withFieldValue("stringFieldA", Arrays.asList("a", "b", "c", "d", "e"))
                .withFieldValue("stringFieldB", Arrays.asList("a", "c", "e"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("stringField", "c")
                .withFieldValue("longFieldA", Arrays.asList(1L, 3L, 5L))
                .withFieldValue("longFieldB", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("stringFieldA", Arrays.asList("a", "c", "e"))
                .withFieldValue("stringFieldB", Arrays.asList("a", "b", "c", "d", "e"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("stringField", "d")
                .withFieldValue("longFieldA", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("longFieldB", null)
                .withFieldValue("stringFieldA", Arrays.asList("a", "b", "c", "d", "e"))
                .withFieldValue("stringFieldB", null)
                .build();
        final Row row5 = Row.withSchema(schema)
                .withFieldValue("stringField", "e")
                .withFieldValue("longFieldA", null)
                .withFieldValue("longFieldB", Arrays.asList(1L, 2L, 3L, 4L, 5L))
                .withFieldValue("stringFieldA", null)
                .withFieldValue("stringFieldB", Arrays.asList("a", "b", "c", "d", "e"))
                .build();
        final Row row6 = Row.withSchema(schema)
                .withFieldValue("stringField", "f")
                .withFieldValue("longFieldA", null)
                .withFieldValue("longFieldB", null)
                .withFieldValue("stringFieldA", null)
                .withFieldValue("stringFieldB", null)
                .build();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(row1, row2, row3, row4, row5, row6))
                .setRowSchema(schema);
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, schema);
        final FCollection<Row> results = BeamSQLTransform.transform(Arrays.asList(fCollection), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                if("a".equals(row.getString("stringField"))) {
                    Assert.assertTrue(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertTrue(Objects.requireNonNull(row.getBoolean("cs")));
                } else if("b".equals(row.getString("stringField"))) {
                    Assert.assertTrue(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertTrue(Objects.requireNonNull(row.getBoolean("cs")));
                } else if("c".equals(row.getString("stringField"))) {
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cs")));
                } else if("d".equals(row.getString("stringField"))) {
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cs")));
                } else if("e".equals(row.getString("stringField"))) {
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cs")));
                } else if("f".equals(row.getString("stringField"))) {
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cl")));
                    Assert.assertFalse(Objects.requireNonNull(row.getBoolean("cs")));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    private void testUDAFs(final String planner) {
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsqlTestUDAFs");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT stringField, MDT_ARRAY_AGG_INT64(longFieldA) AS lfa, MDT_ARRAY_AGG_INT64(longFieldB) AS lfb, MDT_COUNT_DISTINCT_INT64(longFieldB) as ldfb, MDT_ARRAY_AGG_STRING(stringFieldA) AS sfa, MDT_ARRAY_AGG_STRING(stringFieldB) AS sfb, MDT_ARRAY_AGG_DISTINCT_STRING(stringFieldA) AS sfda, MDT_COUNT_DISTINCT_STRING(stringFieldA) AS sfdd FROM rowInput GROUP BY stringField");
        beamsqlParameters.addProperty("planner", planner);
        configBeamSql.setParameters(beamsqlParameters);

        final Schema schema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longFieldA", Schema.FieldType.INT64.withNullable(true))
                .addField("longFieldB", Schema.FieldType.INT64.withNullable(true))
                .addField("stringFieldA", Schema.FieldType.STRING.withNullable(true))
                .addField("stringFieldB", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("longFieldA", 1L)
                .withFieldValue("longFieldB", 2L)
                .withFieldValue("stringFieldA", "a")
                .withFieldValue("stringFieldB", "b")
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("longFieldA", 3L)
                .withFieldValue("longFieldB", 2L)
                .withFieldValue("stringFieldA", "c")
                .withFieldValue("stringFieldB", "b")
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("longFieldA", 5L)
                .withFieldValue("longFieldB", null)
                .withFieldValue("stringFieldA", "c")
                .withFieldValue("stringFieldB", null)
                .build();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(row1, row2, row3))
                .setRowSchema(schema);
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, schema);
        final FCollection<Row> results = BeamSQLTransform.transform(Arrays.asList(fCollection), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                Assert.assertEquals("a", row.getString("stringField"));
                Assert.assertEquals(Long.valueOf(2), row.getInt64("sfdd"));
                Assert.assertEquals(Long.valueOf(1), row.getInt64("ldfb"));

                Assert.assertEquals(3, row.getArray("sfa").size());
                Assert.assertEquals(2, row.getArray("sfda").size());
                Assert.assertEquals(3, row.getArray("lfa").size());
                Assert.assertEquals(2, row.getArray("sfb").size());
                Assert.assertEquals(2, row.getArray("lfb").size());

                Assert.assertTrue(Arrays.asList(1L, 3L, 5L).containsAll(row.getArray("lfa")));
                Assert.assertTrue(Arrays.asList("a", "c", "c").containsAll(row.getArray("sfa")));
                Assert.assertTrue(Arrays.asList(2L).containsAll(row.getArray("lfb")));
                Assert.assertTrue(Arrays.asList("b").containsAll(row.getArray("sfb")));
            }
            Assert.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

     */

}
