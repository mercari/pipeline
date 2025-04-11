package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectFunctionTest {

    @Test
    public void test() {

        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("intField", Schema.FieldType.INT32),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("floatField", Schema.FieldType.FLOAT32),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("enumField", Schema.FieldType.enumeration(List.of("a","b","c"))),
                Schema.Field.of("timestampField", Schema.FieldType.TIMESTAMP),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.builder()
                                .withField("stringField", Schema.FieldType.STRING)
                        .build())),
                Schema.Field.of("arrayNestedField", Schema.FieldType.array(Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .build())))
        );

        final String config = """
                [
                  { "name": "longField" },
                  { "name": "renameIntField", "field": "intField" },
                  { "name": "constantStringField", "type": "string", "value": "constantStringValue" },
                  { "name": "expressionField", "expression": "doubleField * intField / longField" },
                  { "name": "hashField", "func": "hash", "field": "stringField" },
                  { "name": "hashArrayField", "func": "hash", "fields": ["stringField","intField","longField"], "size": 32 },
                  { "name": "currentTimestampField", "func": "current_timestamp" },
                  { "name": "eventTimestampField", "func": "event_timestamp" },
                  { "name": "concatField", "func": "concat", "delimiter": " ", "fields": ["stringField","intField","longField"] },
                  { "name": "intField", "field": "nestedField.stringField", "type": "int32" },
                  { "name": "stringFieldA", "field": "nestedField.stringField", "type": "string" },
                  { "name": "structField", "func": "struct", "mode": "repeated", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "intFieldA", "field": "intField" },
                    { "name": "textFieldA", "func": "hash", "text": "${stringFieldA}" },
                    { "name": "nestedStructField", "func": "struct", "fields": [
                      { "name": "stringFieldA", "field": "stringField" },
                      { "name": "nestedNestedStructField", "func": "struct", "fields": [
                        { "name": "timestampField" },
                        { "name": "enumFieldA", "field": "enumField" }
                      ] }
                    ] }
                  ] },
                  { "name": "structEachField", "each": "arrayNestedField", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "intFieldA", "field": "intField" },
                    { "name": "textFieldA", "func": "hash", "text": "${stringFieldA}" },
                    { "name": "nestedStringField", "field": "arrayNestedField.stringField" }
                  ] },
                  { "name": "jsonField", "func": "json", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "longFieldA", "field": "longField" },
                    { "name": "nestedStructField", "func": "struct", "fields": [
                      { "name": "enumField" },
                      { "name": "doubleFieldA", "field": "doubleField" },
                      { "name": "timestampField" },
                      { "name": "nestedNestedStructField", "func": "struct", "fields": [
                        { "name": "timestampField" },
                        { "name": "enumFieldA", "field": "enumField" }
                      ] }
                    ] }
                  ] },
                  { "name": "hashField2", "func": "hash", "field": "stringField" },
                ]
                """;

        final JsonArray array = new Gson().fromJson(config, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);

        final Schema outputSchema = SelectFunction.createSchema(selectFunctions, "structEachField");
        Assert.assertTrue(outputSchema.hasField("longField"));
        Assert.assertTrue(outputSchema.hasField("renameIntField"));
        Assert.assertTrue(outputSchema.hasField("constantStringField"));
        Assert.assertTrue(outputSchema.hasField("expressionField"));
        Assert.assertTrue(outputSchema.hasField("hashField"));
        Assert.assertTrue(outputSchema.hasField("hashArrayField"));
        Assert.assertTrue(outputSchema.hasField("currentTimestampField"));
        Assert.assertTrue(outputSchema.hasField("eventTimestampField"));
        Assert.assertTrue(outputSchema.hasField("concatField"));
        Assert.assertTrue(outputSchema.hasField("structField"));
        Assert.assertTrue(outputSchema.hasField("jsonField"));
        Assert.assertTrue(outputSchema.hasField("intField"));
        Assert.assertEquals(Schema.Type.int64, outputSchema.getField("longField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.int32, outputSchema.getField("renameIntField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.string, outputSchema.getField("constantStringField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.float64, outputSchema.getField("expressionField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.string, outputSchema.getField("hashField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.string, outputSchema.getField("hashArrayField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.timestamp, outputSchema.getField("currentTimestampField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.timestamp, outputSchema.getField("eventTimestampField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.string, outputSchema.getField("concatField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.array, outputSchema.getField("structField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.string, outputSchema.getField("jsonField").getFieldType().getType());
        Assert.assertEquals(Schema.Type.int32, outputSchema.getField("intField").getFieldType().getType());

        //
        for(final SelectFunction selectFunction : selectFunctions) {
            selectFunction.setup();
        }

        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "stringValue");
        values.put("intField", 32);
        values.put("longField", 10L);
        values.put("floatField", -5.5F);
        values.put("doubleField", 10.10D);
        values.put("enumField", 1);
        values.put("timestampField", Instant.parse("2024-08-30T00:00:00Z").getMillis() * 1000L);

        {
            final Map<String, Object> nestedArrayFieldValues = new HashMap<>();
            nestedArrayFieldValues.put("stringField", "Z");
            values.put("arrayNestedField", List.of(nestedArrayFieldValues));
        }

        final Map<String, Object> nestedFieldValues = new HashMap<>();
        nestedFieldValues.put("stringField", "100");
        values.put("nestedField", nestedFieldValues);

        final Instant eventTimestamp = Instant.parse("2024-01-01T00:00:00Z");

        Map<String,Object> results = SelectFunction.apply(selectFunctions, values, eventTimestamp);
        Assert.assertEquals(10L, results.get("longField"));
        Assert.assertEquals(32, results.get("renameIntField"));
        Assert.assertEquals("constantStringValue", results.get("constantStringField"));
        Assert.assertEquals(32.32, results.get("expressionField"));
        //Assert.assertEquals("dbcc96aec884f7d5057672df21e7446c1415ca7669fdabac78e49f4d852d5a0a", results.get("hashField"));
        //Assert.assertEquals("e6c8c04775d64362367b36c61dc00615eabd1c00887fec05ceae4e5ab9cd215b", results.get("hashArrayField"));
        Assert.assertNotNull(results.get("currentTimestampField"));
        Assert.assertEquals(eventTimestamp.getMillis() * 1000L, results.get("eventTimestampField"));
        Assert.assertEquals("stringValue 32 10", results.get("concatField"));
        Assert.assertEquals(100, results.get("intField"));

        final JsonObject jsonObject = new Gson().fromJson((String)results.get("jsonField"), JsonObject.class);
        Assert.assertEquals("stringValue", jsonObject.get("stringFieldA").getAsString());
        final JsonObject nestedJsonObject = jsonObject.get("nestedStructField").getAsJsonObject();
        Assert.assertEquals("2024-08-30T00:00:00Z", nestedJsonObject.get("timestampField").getAsString());
        Assert.assertEquals("b", nestedJsonObject.get("enumField").getAsString());
    }

}
