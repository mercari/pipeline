package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class BeamSQLTransformTest {

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
                        "elements": [1],
                        "type": "int64"
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "beamsql",
                      "module": "beamsql",
                      "inputs": ["create"],
                      "parameters": {
                        "sql": "SELECT '012345' AS user_id, '2024-01-01T00:00:00.123Z' AS client_event_timestamp, 'category1' AS `category`, 1000 AS `count`, true AS `flag`, CAST(12.34 AS DOUBLE) AS `amount`, TIMESTAMP '2024-01-12 12:34:59.123' AS created_at, '111' || '#' || '21' AS insert_id FROM `create`"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("beamsql");

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
    public void testSimpleQuery() throws Exception {
        final String sql1 = """
                SELECT
                  *
                FROM
                  `create`
                WHERE
                  CHAR_LENGTH(user_id) = 0
                """;

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "logging": [
                        { "name": "input", "level": "info" }
                      ],
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "user_id": "a", "amount":  100, "category": "A", "timestamp": "2025-01-01T00:00:01Z" },
                          { "user_id": "a", "amount":  200, "category": "B", "timestamp": "2025-01-01T00:00:02Z" },
                          { "user_id": "a", "amount":  300, "category": "C", "timestamp": "2025-01-01T00:00:03Z" },
                          { "user_id": "a", "amount":  400, "category": "D", "timestamp": "2025-01-01T00:00:04Z" },
                          { "user_id": "a", "amount":  500, "category": "E", "timestamp": "2025-01-01T00:00:05Z" },
                          { "user_id": "a", "amount":  600, "category": "F", "timestamp": "2025-01-01T00:00:06Z" },
                          { "user_id": "a", "amount":  700, "category": "G", "timestamp": "2025-01-01T00:00:07Z" },
                          { "user_id": "a", "amount":  800, "category": "H", "timestamp": "2025-01-01T00:00:08Z" },
                          { "user_id": "a", "amount":  900, "category": "I", "timestamp": "2025-01-01T00:00:09Z" },
                          { "user_id": "a", "amount": 1000, "category": "J", "timestamp": "2025-01-01T00:00:10Z" },
                          { "user_id": "b", "amount":  100, "category": "A", "timestamp": "2025-01-01T00:00:01Z" },
                          { "user_id": "b", "amount":  200, "category": "B", "timestamp": "2025-01-01T00:00:02Z" },
                          { "user_id": "b", "amount":  300, "category": "C", "timestamp": "2025-01-01T00:00:03Z" },
                          { "user_id": "b", "amount":  400, "category": "D", "timestamp": "2025-01-01T00:00:04Z" },
                          { "user_id": "b", "amount":  500, "category": "E", "timestamp": "2025-01-01T00:00:05Z" },
                          { "user_id": "b", "amount":  600, "category": "F", "timestamp": "2025-01-01T00:00:06Z" },
                          { "user_id": "b", "amount":  700, "category": "G", "timestamp": "2025-01-01T00:00:07Z" },
                          { "user_id": "b", "amount":  800, "category": "H", "timestamp": "2025-01-01T00:00:08Z" },
                          { "user_id": "b", "amount":  900, "category": "I", "timestamp": "2025-01-01T00:00:09Z" },
                          { "user_id": "b", "amount": 1000, "category": "J", "timestamp": "2025-01-01T00:00:10Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "user_id", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "category", "type": "string" },
                          { "name": "timestamp", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "timestamp"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "beamsql1",
                      "module": "beamsql",
                      "inputs": ["create"],
                      "parameters": {
                        "sql": "%s"
                      }
                    }
                  ]
                }
                """;

        final String configText = String.format(configJson, sql1);
        new PubsubTableProvider();
        final Config config = Config.load(configText);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output1 = outputs.get("beamsql1");

        PAssert.that(output1.getCollection()).satisfies(rows -> {
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
    public void testMatchRecognize() throws Exception {

        final String sql1 = """
                WITH `Table` AS (
                  SELECT
                    user_id,
                    CAST(amount AS DECIMAL) AS amount,
                    category,
                    `timestamp`
                  FROM `create`
                )
                SELECT
                  user_id,
                  category_a,
                  category_b,
                  category_c,
                  category_d,
                  amount_a,
                  amount_b,
                  amount_c,
                  amount_d,
                  `timestamp`
                FROM
                  `Table`
                MATCH_RECOGNIZE(
                  PARTITION BY user_id
                  ORDER BY `timestamp`
                  MEASURES
                    A.`timestamp` AS `timestamp`,
                    A.category AS category_a,
                    B.category AS category_b,
                    C.category AS category_c,
                    FIRST(D.category) AS category_d,
                    A.amount AS amount_a,
                    B.amount AS amount_b,
                    C.amount AS amount_c,
                    D.amount AS amount_d
                  PATTERN (A B (D|C))
                  SUBSET E = (A, B, C, D)
                  DEFINE
                    B AS category = 'B',
                    D AS amount > PREV(D.amount, 1),
                    C AS category = 'C'
                ) AS M
                """;

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "user_id": "a", "amount":  100, "category": "A", "timestamp": "2025-01-01T00:00:01Z" },
                          { "user_id": "a", "amount":  200, "category": "B", "timestamp": "2025-01-01T00:00:02Z" },
                          { "user_id": "a", "amount":  300, "category": "C", "timestamp": "2025-01-01T00:00:03Z" },
                          { "user_id": "a", "amount":  400, "category": "D", "timestamp": "2025-01-01T00:00:04Z" },
                          { "user_id": "a", "amount":  500, "category": "E", "timestamp": "2025-01-01T00:00:05Z" },
                          { "user_id": "a", "amount":  600, "category": "F", "timestamp": "2025-01-01T00:00:06Z" },
                          { "user_id": "a", "amount":  700, "category": "G", "timestamp": "2025-01-01T00:00:07Z" },
                          { "user_id": "a", "amount":  800, "category": "H", "timestamp": "2025-01-01T00:00:08Z" },
                          { "user_id": "a", "amount":  900, "category": "I", "timestamp": "2025-01-01T00:00:09Z" },
                          { "user_id": "a", "amount": 1000, "category": "J", "timestamp": "2025-01-01T00:00:10Z" },
                          { "user_id": "b", "amount":  100, "category": "A", "timestamp": "2025-01-01T00:00:01Z" },
                          { "user_id": "b", "amount":  200, "category": "B", "timestamp": "2025-01-01T00:00:02Z" },
                          { "user_id": "b", "amount":  300, "category": "C", "timestamp": "2025-01-01T00:00:03Z" },
                          { "user_id": "b", "amount":  400, "category": "D", "timestamp": "2025-01-01T00:00:04Z" },
                          { "user_id": "b", "amount":  500, "category": "E", "timestamp": "2025-01-01T00:00:05Z" },
                          { "user_id": "b", "amount":  600, "category": "F", "timestamp": "2025-01-01T00:00:06Z" },
                          { "user_id": "b", "amount":  700, "category": "G", "timestamp": "2025-01-01T00:00:07Z" },
                          { "user_id": "b", "amount":  800, "category": "H", "timestamp": "2025-01-01T00:00:08Z" },
                          { "user_id": "b", "amount":  900, "category": "I", "timestamp": "2025-01-01T00:00:09Z" },
                          { "user_id": "b", "amount": 1000, "category": "J", "timestamp": "2025-01-01T00:00:10Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "user_id", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "category", "type": "string" },
                          { "name": "timestamp", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "timestamp"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "beamsql1",
                      "module": "beamsql",
                      "inputs": ["create"],
                      "parameters": {
                        "sql": "%s"
                      }
                    }
                  ]
                }
                """;

        final String configText = String.format(configJson, sql1);
        new PubsubTableProvider();
        final Config config = Config.load(configText);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output1 = outputs.get("beamsql1");

        PAssert.that(output1.getCollection()).satisfies(rows -> {
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

    /*
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
