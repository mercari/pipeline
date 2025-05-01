package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class TokenizeTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    /*
    @Test
    public void testJapaneseTokenizer() {
        final TransformConfig config = new TransformConfig();
        config.setName("tokenize1");
        config.setModule("tokenize");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray fields = new JsonArray();

        final JsonObject field1 = new JsonObject();
        field1.addProperty("name", "text1Output");
        field1.addProperty("input", "text1");
        final JsonObject tokenizer1 = new JsonObject();
        tokenizer1.addProperty("type", "JapaneseTokenizer");
        field1.add("tokenizer", tokenizer1);
        final JsonArray filters1 = new JsonArray();
        final JsonObject filter11 = new JsonObject();
        filter11.addProperty("type", "JapaneseBaseFormFilter");
        filters1.add(filter11);
        field1.add("filters", filters1);
        fields.add(field1);

        final JsonObject field2 = new JsonObject();
        field2.addProperty("name", "text2Output");
        field2.addProperty("input", "text2");
        final JsonObject tokenizer2 = new JsonObject();
        tokenizer2.addProperty("type", "WhitespaceTokenizer");
        field2.add("tokenizer", tokenizer2);
        final JsonObject filter21 = new JsonObject();
        final JsonObject filter22 = new JsonObject();
        filter21.addProperty("type", "StopFilter");
        filter22.addProperty("type", "LowerCaseFilter");
        final JsonArray filters2 = new JsonArray();
        filters2.add(filter21);
        filters2.add(filter22);
        field2.add("filters", filters2);
        fields.add(field2);

        final JsonObject field3 = new JsonObject();
        field3.addProperty("name", "text3Output");
        field3.addProperty("input", "text3");
        final JsonObject tokenizer3 = new JsonObject();
        tokenizer3.addProperty("type", "JapaneseTokenizer");
        field3.add("tokenizer", tokenizer1);
        final JsonArray filters3 = new JsonArray();
        final JsonObject filter31 = new JsonObject();
        filter31.addProperty("type", "JapaneseBaseFormFilter");
        final JsonObject filter32 = new JsonObject();
        filter32.addProperty("type", "JapanesePartOfSpeechStopFilter");
        final JsonArray stopPartOfSpeechPath = new JsonArray();
        stopPartOfSpeechPath.add("助詞-接続助詞");
        stopPartOfSpeechPath.add("助動詞");
        filter32.add("tags", stopPartOfSpeechPath);
        filters3.add(filter32);
        field3.add("filters", filters3);
        fields.add(field3);

        final JsonObject parameters = new JsonObject();
        parameters.add("fields", fields);
        config.setParameters(parameters);

        final Schema schema = Schema.builder()
                .addField(Schema.Field.of("text1", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("text2", Schema.FieldType.STRING.withNullable(true)))
                .addField(Schema.Field.of("text3", Schema.FieldType.STRING.withNullable(true)))
                .addField(Schema.Field.of("int", Schema.FieldType.INT32.withNullable(false)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("text1", "東京都に行きました")
                .withFieldValue("text2", null)
                .withFieldValue("text3", "押して駄目なら引いてみろ")
                .withFieldValue("int", 1)
                .withFieldValue("timestamp", Instant.parse("2022-04-23T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("text1", "人生楽ありゃ苦もあるさ")
                .withFieldValue("text2", "Well done is better than well said.")
                .withFieldValue("text3", "")
                .withFieldValue("int", 2)
                .withFieldValue("timestamp", Instant.parse("2022-04-24T00:01:00Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("text1", "本日天気晴朗なれども波高し")
                .withFieldValue("text2", "Live as if were to die tomorrow. Learn as if you were to live forever.")
                .withFieldValue("text3", null)
                .withFieldValue("int", 3)
                .withFieldValue("timestamp", Instant.parse("2022-04-25T00:02:00Z"))
                .build();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(row1, row2, row3).withCoder(RowCoder.of(schema)))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = TokenizeTransform.transform(Arrays.asList(fCollection1), config);

        final Schema outputSchema = outputs1.get("tokenize1").getSchema();
        Assert.assertEquals(8, outputSchema.getFieldCount());
        Assert.assertTrue(outputSchema.hasField("text1"));
        Assert.assertTrue(outputSchema.hasField("text2"));
        Assert.assertTrue(outputSchema.hasField("text3"));
        Assert.assertTrue(outputSchema.hasField("int"));
        Assert.assertTrue(outputSchema.hasField("timestamp"));
        Assert.assertTrue(outputSchema.hasField("text1Output"));
        Assert.assertTrue(outputSchema.hasField("text2Output"));
        Assert.assertTrue(outputSchema.hasField("text3Output"));

        final Schema outputSchemaChild1 = outputSchema.getField("text1Output").getType().getCollectionElementType().getRowSchema();
        final Schema outputSchemaChild2 = outputSchema.getField("text2Output").getType().getCollectionElementType().getRowSchema();
        final Schema outputSchemaChild3 = outputSchema.getField("text3Output").getType().getCollectionElementType().getRowSchema();
        Assert.assertEquals(10, outputSchemaChild1.getFieldCount());
        Assert.assertEquals(4, outputSchemaChild2.getFieldCount());
        Assert.assertEquals(10, outputSchemaChild3.getFieldCount());

        final PCollection<Row> output1 = (PCollection<Row>) outputs1.get("tokenize1").getCollection();
        PAssert.that(output1).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                if(row.getInt32("int") == 1) {
                    Assert.assertEquals(6, row.getArray("text1Output").size());
                    Assert.assertEquals(0, row.getArray("text2Output").size());
                    Assert.assertEquals(4, row.getArray("text3Output").size());
                } else if(row.getInt32("int") == 2) {
                    Assert.assertEquals(7, row.getArray("text1Output").size());
                    Assert.assertEquals(7, row.getArray("text2Output").size());
                    Assert.assertEquals(0, row.getArray("text3Output").size());
                } else if(row.getInt32("int") == 3) {
                    Assert.assertEquals(7, row.getArray("text1Output").size());
                    Assert.assertEquals(15, row.getArray("text2Output").size());
                    Assert.assertEquals(0, row.getArray("text3Output").size());
                }
                count += 1;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

     */

}
