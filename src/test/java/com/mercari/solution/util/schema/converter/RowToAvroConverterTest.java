package com.mercari.solution.util.schema.converter;

import com.mercari.solution.TestDatum;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

public class RowToAvroConverterTest {

    @Test
    public void testConvertSchema() {
        String json = """
                {
                  "fields": [
                    { "name": "field0", "type": "string", "mode": "required" },
                    { "name": "field1", "type": "long", "mode": "required", "defaultValue": 10 },
                    { "name": "field2", "type": "row", "mode": "nullable", "fields": [
                      { "name": "fieldA", "type": "string", "mode": "required", "defaultValue": "ok" }
                    ]}
                  ]
                }
                """;
        final com.mercari.solution.module.Schema inputSchema = com.mercari.solution.module.Schema.parse(json);
        final Schema schema = inputSchema.getAvroSchema();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("field0", "ok");
        builder.set("field2", new GenericRecordBuilder(AvroSchemaUtil.unnestUnion(schema.getField("field2").schema())).build());
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
