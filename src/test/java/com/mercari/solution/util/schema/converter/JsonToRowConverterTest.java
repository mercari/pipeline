package com.mercari.solution.util.schema.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class JsonToRowConverterTest {

    @Test
    public void test() {
        final Row row = TestDatum.generateRow();
        final String json = RowToJsonConverter.convert(row);
        final Row revertedRow = JsonToRowConverter.convert(row.getSchema(), json);
        testFlatField(revertedRow);
        final Row revertedRowChild = revertedRow.getRow("recordField");
        testFlatField(revertedRowChild);
        final Row revertedRowGrandchild = revertedRowChild.getRow("recordField");
        testFlatField(revertedRowGrandchild);

        for(final Row child : revertedRow.<Row>getArray("recordArrayField")) {
            testFlatField(child);
        }

        for(final Row child : revertedRowChild.<Row>getArray("recordArrayField")) {
            testFlatField(child);
        }
    }

    @Test
    public void testValidateSchema() {
        final Row row = TestDatum.generateRow();
        final String json = RowToJsonConverter.convert(row);
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        Assert.assertTrue(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));

        // Check existing field
        jsonObject.remove("stringField");
        Assert.assertTrue(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));
        jsonObject.remove("stringArrayField");
        Assert.assertTrue(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));

        // Check additional field
        jsonObject.addProperty("newStringField", "stringValue");
        Assert.assertFalse(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));
        jsonObject.remove("newStringField");
        final JsonArray jsonArray = new JsonArray();
        jsonObject.add("newArrayField", jsonArray);
        Assert.assertFalse(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));
        jsonArray.add("stringValue");
        jsonObject.add("newArrayField", jsonArray);
        Assert.assertFalse(JsonToRowConverter.validateSchema(row.getSchema(), jsonObject));
    }

    private void testFlatField(final Row row) {
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), row.getBoolean("booleanField"));
        Assert.assertEquals(TestDatum.getStringFieldValue(), row.getString("stringField"));
        Assert.assertEquals(TestDatum.getBytesFieldValue(), new String(row.getBytes("bytesField"), StandardCharsets.UTF_8));
        Assert.assertEquals(TestDatum.getIntFieldValue(), row.getInt32("intField"));
        Assert.assertEquals(TestDatum.getLongFieldValue(), row.getInt64("longField"));
        Assert.assertEquals(TestDatum.getFloatFieldValue(), row.getFloat("floatField"));
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), row.getDouble("doubleField"));
        Assert.assertEquals(TestDatum.getDateFieldValue(),row.getValue("dateField"));
        Assert.assertEquals(TestDatum.getTimestampFieldValue(), row.getDateTime("timestampField"));
        Assert.assertEquals(TestDatum.getDecimalFieldValue(), row.getDecimal("decimalField"));
        // TODO array check
    }

}
