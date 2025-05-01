package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;

public class StructToJsonConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final Struct struct = TestDatum.generateStruct();
        final JsonObject json = new Gson().fromJson(StructToJsonConverter.convert(struct), JsonObject.class);
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
        final Struct struct = TestDatum.generateStructNull();
        final JsonObject json = new Gson().fromJson(StructToJsonConverter.convert(struct), JsonObject.class);
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
        Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), jsonObject.get("longField").getAsLong());
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), jsonObject.get("doubleField").getAsDouble(), DELTA);
        Assert.assertEquals(TestDatum.getDateFieldValue().toEpochDay(), LocalDate.parse(jsonObject.get("dateField").getAsString()).toEpochDay());
        Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Instant.parse(jsonObject.get("timestampField").getAsString()).getMillis());
        Assert.assertEquals(TestDatum.getDecimalFieldValue().toString(), jsonObject.get("decimalField").getAsString());
        // TODO array check
    }

    private void testFlatFieldNull(final JsonObject jsonObject) {
        Assert.assertTrue(jsonObject.get("booleanField").isJsonNull());
        Assert.assertTrue(jsonObject.get("bytesField").isJsonNull());
        Assert.assertTrue(jsonObject.get("stringField").isJsonNull());
        Assert.assertTrue(jsonObject.get("longField").isJsonNull());
        Assert.assertTrue(jsonObject.get("doubleField").isJsonNull());
        Assert.assertTrue(jsonObject.get("dateField").isJsonNull());
        Assert.assertTrue(jsonObject.get("timestampField").isJsonNull());
        Assert.assertTrue(jsonObject.get("decimalField").isJsonNull());

        Assert.assertTrue(jsonObject.get("booleanArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("bytesArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("stringArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("longArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("doubleArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("dateArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("timestampArrayField").isJsonNull());
        Assert.assertTrue(jsonObject.get("decimalArrayField").isJsonNull());
    }

}
