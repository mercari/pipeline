package com.mercari.solution.util.schema.converter;

import com.mercari.solution.TestDatum;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

public class RowToAvroConverterTest {

    @Test
    public void testConvertSchema() {
        final String json = "{ \"fields\": [{ \"name\": \"field0\", \"type\": \"string\", \"mode\": \"required\" }, { \"name\": \"field1\", \"type\": \"long\", \"mode\": \"required\", \"defaultValue\": 10 }, { \"name\": \"field2\", \"type\": \"row\", \"mode\": \"required\",  \"fields\": [ { \"name\": \"fieldA\", \"mode\": \"required\", \"type\": \"string\", \"defaultValue\": \"ok\" } ] } ] }";
        final com.mercari.solution.module.Schema inputSchema = com.mercari.solution.module.Schema.parse(json);

        final org.apache.beam.sdk.schemas.Schema rowSchema = inputSchema.getRowSchema();
        final Schema schema = RowToRecordConverter.convertSchema(rowSchema);

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("field0", "ok");
        builder.set("field2", new GenericRecordBuilder(schema.getField("field2").schema()).build());
        GenericRecord record = builder.build();
        Assert.assertEquals("ok", record.get("field0").toString());
        Assert.assertEquals(10L, record.get("field1"));
        Assert.assertEquals("ok", ((GenericRecord)record.get("field2")).get("fieldA").toString());
    }

    @Test
    public void testConvert() {
        final Row sourceRow = TestDatum.generateRow();
        final Schema schema = RowToRecordConverter.convertSchema(sourceRow.getSchema());
        final GenericRecord sourceRecord = RowToRecordConverter.convert(schema, sourceRow);

        final Row targetRow = AvroToRowConverter.convert(sourceRow.getSchema(), sourceRecord);
        Assert.assertEquals(RowToJsonConverter.convert(sourceRow), RowToJsonConverter.convert(targetRow));
    }

}
