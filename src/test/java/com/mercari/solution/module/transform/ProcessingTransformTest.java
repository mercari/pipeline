package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;

public class ProcessingTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    /*
    @Test
    public void testProcessingStatefulSingleStage() {

        final TransformConfig config = new TransformConfig();
        config.setName("processing");
        config.setModule("processing");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray stages = new JsonArray();
        {
            final JsonObject stage = new JsonObject();
            stage.addProperty("name", "stage1");
            stage.addProperty("type", "timeseries");

            final JsonArray inputs = new JsonArray();
            inputs.add("rowInput");
            stage.add("inputs", inputs);

            final JsonArray groupFields = new JsonArray();
            groupFields.add("userId");
            stage.add("groupFields", groupFields);

            final JsonArray remainFields = new JsonArray();
            remainFields.add("long");
            remainFields.add("string");
            remainFields.add("timestamp");
            stage.add("remainFields", remainFields);

            final JsonArray outputFields = new JsonArray();
            outputFields.add("long");
            outputFields.add("string");
            outputFields.add("timestamp");
            //stage.add("outputFields", outputFields);

            final JsonObject outputRenameFields = new JsonObject();
            //outputRenameFields.addProperty("long", "l");
            //outputRenameFields.addProperty("string", "s");
            outputRenameFields.addProperty("timestamp", "t");
            //stage.add("outputRenameFields", outputRenameFields);

            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "string");
            filter.addProperty("op", "=");
            filter.addProperty("value", "D");
            //stage.add("filter", filter);

            final JsonArray steps = new JsonArray();
            // expression
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "expressionField1");
                step.addProperty("op", "expression");
                step.addProperty("expression", "(double_0 + double_1) / max(long, long_0)");
                steps.add(step);
            }
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "expressionField2");
                step.addProperty("op", "expression");
                step.addProperty("expression", "(double + long) / max(long, double)");
                steps.add(step);
            }
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "expressionFieldEMA");
                step.addProperty("op", "expression");
                step.addProperty("expression", "long * 0.8 + expressionFieldEMA_0 * 0.2");
                //steps.add(step);
            }
            // current_timestamp
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "currentTimestampField");
                step.addProperty("op", "current_timestamp");
                steps.add(step);
            }
            // hash
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "hashField");
                step.addProperty("op", "hash");
                step.addProperty("algorithm", "HmacSHA256");
                step.addProperty("secret", "My Secret Key");
                step.addProperty("size", 16);
                step.addProperty("field", "string");
                steps.add(step);
            }
            // binning
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "binningField");
                step.addProperty("op", "binning");
                step.addProperty("field", "double");
                final JsonArray bins = new JsonArray();
                bins.add(300);
                bins.add(600);
                bins.add(900);
                step.add("bins", bins);
                steps.add(step);
            }
            // lag
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "lagField");
                step.addProperty("op", "lag");
                //step.addProperty("field", "string");
                final JsonArray fields = new JsonArray();
                fields.add("string");
                fields.add("long");
                step.add("fields", fields);

                final JsonArray lags = new JsonArray();
                lags.add(1);
                lags.add(2);
                lags.add(3);
                step.add("lags", lags);
                steps.add(step);
            }
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "lagExpression");
                step.addProperty("op", "lag");
                step.addProperty("expression", "(long + userId_0) / long_0");
                final JsonArray lags = new JsonArray();
                lags.add(1);
                lags.add(2);
                lags.add(3);
                step.add("lags", lags);
                steps.add(step);
            }
            // max
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "maxField");
                step.addProperty("op", "max");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // min
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "minField");
                step.addProperty("op", "min");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // argmax
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "argmaxField");
                step.addProperty("op", "argmax");
                step.addProperty("comparingField", "double");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                fields.add("timestamp");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // argmin
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "argminField");
                step.addProperty("op", "argmin");
                step.addProperty("comparingField", "double");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                fields.add("timestamp");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // sum
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "sumField");
                step.addProperty("op", "sum");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // avg
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "avgField");
                step.addProperty("op", "avg");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(3);
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // std
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "stdField");
                step.addProperty("op", "std");

                final JsonArray fields = new JsonArray();
                fields.add("long");
                fields.add("double");
                //step.addProperty("field", "long");
                step.add("fields", fields);

                final JsonArray ranges = new JsonArray();
                ranges.add(4);
                ranges.add(5);
                //step.add("ranges", ranges);
                step.addProperty("range", 3);
                steps.add(step);
            }
            // linear_regression
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "linearRegField");
                step.addProperty("op", "linear_regression");
                step.addProperty("trainSize", 8);
                step.addProperty("trainSizeUnit", "count");
                step.addProperty("regularizationType", "ridge");
                step.addProperty("trainIntervalSize", 1);
                step.addProperty("standardize", true);
                step.addProperty("alpha", 1.0);
                step.addProperty("components", 3);

                final JsonArray targetFields = new JsonArray();
                targetFields.add("long");
                step.add("targetFields", targetFields);

                final JsonArray featureFields = new JsonArray();
                featureFields.add("fieldA");
                featureFields.add("fieldB");
                featureFields.add("fieldC");
                featureFields.add("binningField");
                step.add("featureFields", featureFields);

                final JsonArray horizons = new JsonArray();
                horizons.add(1);
                horizons.add(2);
                horizons.add(3);
                step.add("horizons", horizons);
                steps.add(step);
            }
            {
                final JsonObject step = new JsonObject();
                step.addProperty("name", "linearRegExpression");
                step.addProperty("op", "linear_regression");
                step.addProperty("trainSize", 8);
                step.addProperty("trainSizeUnit", "count");
                step.addProperty("regularizationType", "ridge");
                step.addProperty("trainIntervalSize", 1);
                step.addProperty("standardize", true);
                step.addProperty("alpha", 1.0);
                step.addProperty("n", 3);

                final JsonArray targetExpressions = new JsonArray();
                targetExpressions.add("long");
                targetExpressions.add("long - long_0");
                step.add("targetExpressions", targetExpressions);

                final JsonArray featureFields = new JsonArray();
                //fields.add("lagExpression_lag1");
                //fields.add("lagExpression_lag2");
                //fields.add("lagExpression_lag3");
                featureFields.add("fieldA");
                featureFields.add("fieldB");
                featureFields.add("fieldC");

                step.add("featureFields", featureFields);

                final JsonArray horizons = new JsonArray();
                horizons.add(1);
                horizons.add(2);
                horizons.add(3);
                step.add("horizons", horizons);
                steps.add(step);
            }

            stage.add("steps", steps);

            stages.add(stage);
        }

        final JsonObject parameters = new JsonObject();
        parameters.add("stages", stages);

        //final JsonObject triggerParameters = new JsonObject();
        //parameters.add("trigger", triggerParameters);

        parameters.addProperty("outputType", "row");

        config.setParameters(parameters);

        final Schema inputSchema = Schema.builder()
                .addField(Schema.Field.of("id", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("userId", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("string", Schema.FieldType.STRING.withNullable(true)))
                .addField(Schema.Field.of("bool", Schema.FieldType.BOOLEAN.withNullable(false)))
                .addField(Schema.Field.of("long", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("long_a", Schema.FieldType.INT64.withNullable(true)))
                .addField(Schema.Field.of("double", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("date", CalciteUtils.DATE.withNullable(true)))
                .addField(Schema.Field.of("time", CalciteUtils.TIME.withNullable(true)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .addField(Schema.Field.of("fieldA", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("fieldB", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("fieldC", Schema.FieldType.DOUBLE.withNullable(true)))
                .build();

        final Row row1 = Row.withSchema(inputSchema)
                .withFieldValue("id", "1")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 1L)
                .withFieldValue("double", 100D)
                .withFieldValue("date", LocalDate.parse("2022-01-01"))
                .withFieldValue("time", LocalTime.parse("01:01:01.001"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:00:00Z"))
                .withFieldValue("fieldA", 100D)
                .withFieldValue("fieldB", 200D)
                .withFieldValue("fieldC", 300D)
                .build();
        final Row row2 = Row.withSchema(inputSchema)
                .withFieldValue("id", "2")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "A")
                .withFieldValue("bool", true)
                .withFieldValue("long", 2L)
                .withFieldValue("double", 200D)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:01:01Z"))
                .withFieldValue("fieldA", 150D)
                .withFieldValue("fieldB", 250D)
                .withFieldValue("fieldC", 350D)
                .build();
        final Row row3 = Row.withSchema(inputSchema)
                .withFieldValue("id", "3")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "B")
                .withFieldValue("bool", true)
                .withFieldValue("long", 3L)
                .withFieldValue("double", null)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:02:02Z"))
                .withFieldValue("fieldA", 200D)
                .withFieldValue("fieldB", 300D)
                .withFieldValue("fieldC", 400D)
                .build();
        final Row row4 = Row.withSchema(inputSchema)
                .withFieldValue("id", "4")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "C")
                .withFieldValue("bool", true)
                .withFieldValue("long", 4L)
                .withFieldValue("double", 300D)
                .withFieldValue("date", LocalDate.parse("2022-03-03"))
                .withFieldValue("time", LocalTime.parse("03:03:03.003"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:03:03Z"))
                .withFieldValue("fieldA", 250D)
                .withFieldValue("fieldB", 350D)
                .withFieldValue("fieldC", 450D)
                .build();
        final Row row5 = Row.withSchema(inputSchema)
                .withFieldValue("id", "5")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "D")
                .withFieldValue("bool", true)
                .withFieldValue("long", 5L)
                .withFieldValue("double", 400D)
                .withFieldValue("date", LocalDate.parse("2022-04-04"))
                .withFieldValue("time", LocalTime.parse("04:04:04.004"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:04:04Z"))
                .withFieldValue("fieldA", 300D)
                .withFieldValue("fieldB", 400D)
                .withFieldValue("fieldC", 500D)
                .build();
        final Row row6 = Row.withSchema(inputSchema)
                .withFieldValue("id", "6")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "E")
                .withFieldValue("bool", true)
                .withFieldValue("long", 6L)
                .withFieldValue("double", 500D)
                .withFieldValue("date", null)
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:05:05Z"))
                .withFieldValue("fieldA", 350D)
                .withFieldValue("fieldB", 450D)
                .withFieldValue("fieldC", 550D)
                .build();
        final Row row7 = Row.withSchema(inputSchema)
                .withFieldValue("id", "7")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "F")
                .withFieldValue("bool", true)
                .withFieldValue("long", 7L)
                .withFieldValue("double", null)
                .withFieldValue("date", LocalDate.parse("2022-06-06"))
                .withFieldValue("time", LocalTime.parse("06:06:06.006"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:06:06Z"))
                .withFieldValue("fieldA", 600D)
                .withFieldValue("fieldB", 700D)
                .withFieldValue("fieldC", 800D)
                .build();
        final Row row8 = Row.withSchema(inputSchema)
                .withFieldValue("id", "8")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "G")
                .withFieldValue("bool", true)
                .withFieldValue("long", 8L)
                .withFieldValue("double", 600D)
                .withFieldValue("date", LocalDate.parse("2022-07-07"))
                .withFieldValue("time", LocalTime.parse("07:07:07.007"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:07:07Z"))
                .withFieldValue("fieldA", 650D)
                .withFieldValue("fieldB", 750D)
                .withFieldValue("fieldC", 850D)
                .build();
        final Row row9 = Row.withSchema(inputSchema)
                .withFieldValue("id", "9")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "H")
                .withFieldValue("bool", true)
                .withFieldValue("long", 9L)
                .withFieldValue("double", 700D)
                .withFieldValue("date", LocalDate.parse("2022-08-08"))
                .withFieldValue("time", LocalTime.parse("08:08:08.008"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:08:08Z"))
                .withFieldValue("fieldA", 700D)
                .withFieldValue("fieldB", 800D)
                .withFieldValue("fieldC", 900D)
                .build();
        final Row row10 = Row.withSchema(inputSchema)
                .withFieldValue("id", "10")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 10L)
                .withFieldValue("double", 800D)
                .withFieldValue("date", LocalDate.parse("2022-09-09"))
                .withFieldValue("time", LocalTime.parse("09:09:09.009"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:09:09Z"))
                .withFieldValue("fieldA", 750D)
                .withFieldValue("fieldB", 850D)
                .withFieldValue("fieldC", 950D)
                .build();
        final Row row11 = Row.withSchema(inputSchema)
                .withFieldValue("id", "11")
                .withFieldValue("userId", 1L)
                .withFieldValue("string", "I")
                .withFieldValue("bool", true)
                .withFieldValue("long", 11L)
                .withFieldValue("double", 900D)
                .withFieldValue("date", LocalDate.parse("2022-10-10"))
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:10:10Z"))
                .withFieldValue("fieldA", 800D)
                .withFieldValue("fieldB", 900D)
                .withFieldValue("fieldC", 1000D)
                .build();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create
                        .of(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11)
                        .withCoder(RowCoder.of(inputSchema)))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, inputSchema);

        final Map<String, FCollection<?>> outputs = ProcessingTransform.transform(Arrays.asList(fCollection1), config);

        final FCollection<?> outputs1 = outputs.get("processing.stage1");

        final Schema outputSchema1 = outputs1.getSchema();

        final PCollection<Row> output1 = (PCollection<Row>) outputs1.getCollection();
        PAssert.that(output1).satisfies(rows -> {
            int count = 0;
            // TODO
            for(final Row row : rows) {

            }
            return null;
        });

        pipeline.run();

    }

     */

}
