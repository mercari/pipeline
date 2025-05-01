package com.mercari.solution.util.schema.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;

public class JsonToAvroConverterTest {

    @Test
    public void test() {
        final GenericRecord record = TestDatum.generateRecord();
        final String json = AvroToJsonConverter.convert(record);
        final GenericRecord revertedRecord = JsonToAvroConverter.convert(record.getSchema(), json);
        testFlatField(revertedRecord);
        final GenericRecord revertedRecordChild = (GenericRecord)revertedRecord.get("recordField");
        testFlatField(revertedRecordChild);
        final GenericRecord revertedRecordGrandchild = (GenericRecord)revertedRecordChild.get("recordField");
        testFlatField(revertedRecordGrandchild);

        for(final GenericRecord child : (List<GenericRecord>)revertedRecord.get("recordArrayField")) {
            testFlatField(child);
        }

        for(final GenericRecord child : (List<GenericRecord>)revertedRecordChild.get("recordArrayField")) {
            testFlatField(child);
        }
    }

    @Test
    public void testValidateSchema() {
        final GenericRecord record = TestDatum.generateRecord();
        final String json = AvroToJsonConverter.convert(record);

        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        Assert.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));

        // Check existing field
        jsonObject.remove("stringField");
        Assert.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonObject.remove("stringArrayField");
        Assert.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));

        // Check additional field
        jsonObject.addProperty("newStringField", "stringValue");
        Assert.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonObject.remove("newStringField");
        final JsonArray jsonArray = new JsonArray();
        jsonObject.add("newArrayField", jsonArray);
        Assert.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonArray.add("stringValue");
        jsonObject.add("newArrayField", jsonArray);
        Assert.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
    }

    private void testFlatField(final GenericRecord record) {
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), record.get("booleanField"));
        Assert.assertEquals(TestDatum.getStringFieldValue(), record.get("stringField"));
        Assert.assertEquals(TestDatum.getBytesFieldValue(), new String(Base64.getDecoder().decode(((ByteBuffer)record.get("bytesField")).array()), StandardCharsets.UTF_8));
        Assert.assertEquals(TestDatum.getIntFieldValue(), record.get("intField"));
        Assert.assertEquals(TestDatum.getLongFieldValue(), record.get("longField"));
        Assert.assertEquals(TestDatum.getFloatFieldValue(), record.get("floatField"));
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), record.get("doubleField"));
        Assert.assertEquals(TestDatum.getDateFieldValue(), LocalDate.ofEpochDay((int)record.get("dateField")));
        Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), (long)record.get("timestampField")/1000);
        int scale = AvroSchemaUtil.getLogicalTypeDecimal(record.getSchema().getField("decimalField").schema()).getScale();
        Assert.assertEquals(TestDatum.getDecimalFieldValue(), BigDecimal.valueOf(new BigInteger(((ByteBuffer)record.get("decimalField")).array()).longValue(), scale));
        // TODO array check
    }

}
