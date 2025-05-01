package com.mercari.solution.util.schema.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;

public class AvroToJsonConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final GenericRecord record = TestDatum.generateRecord();
        final JsonObject json = new Gson().fromJson(AvroToJsonConverter.convert(record), JsonObject.class);
        testFlatField(json);
        final JsonObject childJson = json.get("recordField").getAsJsonObject();
        testFlatField(childJson);
        for(JsonElement g : childJson.get("recordArrayField").getAsJsonArray()) {
            testFlatField(g.getAsJsonObject());
        }
        final JsonObject grandchildJson = childJson.get("recordField").getAsJsonObject();
        testFlatField(grandchildJson);

        for(JsonElement c : json.get("recordArrayField").getAsJsonArray()) {
            testFlatField(c.getAsJsonObject());
            final JsonObject gc = c.getAsJsonObject().get("recordField").getAsJsonObject();
            testFlatField(gc);
            for(JsonElement g : c.getAsJsonObject().get("recordArrayField").getAsJsonArray()) {
                testFlatField(g.getAsJsonObject());
            }
        }
    }

    @Test
    public void testNull() {
        final GenericRecord record = TestDatum.generateRecordNull();
        final JsonObject json = new Gson().fromJson(AvroToJsonConverter.convert(record), JsonObject.class);
        testFlatFieldNull(json);
        final JsonObject childJson = json.get("recordField").getAsJsonObject();
        testFlatFieldNull(childJson);
        for(JsonElement g : childJson.get("recordArrayField").getAsJsonArray()) {
            testFlatFieldNull(g.getAsJsonObject());
        }
        final JsonObject grandchildJson = childJson.get("recordField").getAsJsonObject();
        testFlatFieldNull(grandchildJson);

        for(JsonElement c : json.get("recordArrayField").getAsJsonArray()) {
            testFlatFieldNull(c.getAsJsonObject());
            final JsonObject gc = c.getAsJsonObject().get("recordField").getAsJsonObject();
            testFlatFieldNull(gc);
            for(JsonElement g : c.getAsJsonObject().get("recordArrayField").getAsJsonArray()) {
                testFlatFieldNull(g.getAsJsonObject());
            }
        }
    }

    private void testFlatField(final JsonObject jsonObject) {
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), jsonObject.get("booleanField").getAsBoolean());
        Assert.assertEquals(TestDatum.getStringFieldValue(), jsonObject.get("stringField").getAsString());
        Assert.assertEquals(TestDatum.getBytesFieldValue(), new String(
                Base64.getDecoder().decode(jsonObject.get("bytesField").getAsString()), StandardCharsets.UTF_8));
        Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), jsonObject.get("intField").getAsInt());
        Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), jsonObject.get("longField").getAsLong());
        Assert.assertEquals(TestDatum.getFloatFieldValue(), jsonObject.get("floatField").getAsFloat(), DELTA);
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), jsonObject.get("doubleField").getAsDouble(), DELTA);
        Assert.assertEquals(TestDatum.getDateFieldValue().toEpochDay(), LocalDate.parse(jsonObject.get("dateField").getAsString()).toEpochDay());
        Assert.assertEquals(TestDatum.getTimeFieldValue().toSecondOfDay(), LocalTime.parse(jsonObject.get("timeField").getAsString()).toSecondOfDay());
        Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Instant.parse(jsonObject.get("timestampField").getAsString()).getMillis());
        Assert.assertEquals(TestDatum.getDecimalFieldValue().toString(), jsonObject.get("decimalField").getAsString());
        // TODO array check
    }

    private void testFlatFieldNull(final JsonObject jsonObject) {
        Assert.assertTrue(jsonObject.get("booleanField").isJsonNull());
        Assert.assertTrue(jsonObject.get("bytesField").isJsonNull());
        Assert.assertTrue(jsonObject.get("stringField").isJsonNull());
        Assert.assertTrue(jsonObject.get("intField").isJsonNull());
        Assert.assertTrue(jsonObject.get("longField").isJsonNull());
        Assert.assertTrue(jsonObject.get("floatField").isJsonNull());
        Assert.assertTrue(jsonObject.get("doubleField").isJsonNull());
        Assert.assertTrue(jsonObject.get("dateField").isJsonNull());
        Assert.assertTrue(jsonObject.get("timeField").isJsonNull());
        Assert.assertTrue(jsonObject.get("timestampField").isJsonNull());
        Assert.assertTrue(jsonObject.get("decimalField").isJsonNull());
    }

}
