package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.EntityToRowConverter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;

public class EntityToRowConverterTest {


    private static final double DELTA = 1e-15;

    @Test
    public void testConvert() {
        final Key key = Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("MyKind").setName("mykey").build())
                .build();
        final Schema schema = Schema.builder()
                .addField("booleanField", Schema.FieldType.BOOLEAN)
                .addField("stringField", Schema.FieldType.STRING)
                .addField("bytesField", Schema.FieldType.BYTES)
                .addField("intField", Schema.FieldType.INT32)
                .addField("longField", Schema.FieldType.INT64)
                .addField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("dateField", CalciteUtils.DATE)
                .addField("timeField", CalciteUtils.TIME)
                .addField("timestampField", Schema.FieldType.DATETIME)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .build();

        final Schema schemaWithKey = EntityToRowConverter.addKeyToSchema(schema);

        byte[] bytes = "this is bytesField value".getBytes();

        final Entity entity = Entity.newBuilder()
                .setKey(key)
                .putProperties("booleanField", Value.newBuilder().setBooleanValue(true).build())
                .putProperties("stringField", Value.newBuilder().setStringValue("str").build())
                .putProperties("bytesField", Value.newBuilder().setBlobValue(ByteString.copyFrom(bytes)).build())
                .putProperties("intField", Value.newBuilder().setIntegerValue(1).build())
                .putProperties("longField", Value.newBuilder().setIntegerValue(1L).build())
                .putProperties("floatField", Value.newBuilder().setDoubleValue(1F).build())
                .putProperties("doubleField", Value.newBuilder().setDoubleValue(1D).build())
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(Instant.parse("2023-04-20T00:00:00Z"))).build())
                .putProperties("dateField", Value.newBuilder().setStringValue("2021-12-01").build())
                .putProperties("timeField", Value.newBuilder().setStringValue("18:51:32.123").build())
                .putProperties("decimalField", Value.newBuilder().setStringValue("1234.56789").build())
                .build();

        final Row convertedRow = EntityToRowConverter.convert(schemaWithKey, entity);

        Assert.assertEquals(true, convertedRow.getBoolean("booleanField"));
        Assert.assertEquals("str", convertedRow.getString("stringField"));
        Assert.assertEquals(new String(bytes), new String(convertedRow.getBytes("bytesField")));
        Assert.assertEquals(1, convertedRow.getInt32("intField").intValue());
        Assert.assertEquals(1L, convertedRow.getInt64("longField").longValue());
        Assert.assertEquals(1F, convertedRow.getFloat("floatField"), DELTA);
        Assert.assertEquals(1D, convertedRow.getDouble("doubleField"), DELTA);
        Assert.assertEquals(Instant.parse("2023-04-20T00:00:00Z"), convertedRow.getDateTime("timestampField").toInstant());
        Assert.assertEquals(LocalDate.of(2021,12,1), convertedRow.getLogicalTypeValue("dateField", LocalDate.class));
        Assert.assertEquals(LocalTime.of(18, 51,32, 123000000), convertedRow.getLogicalTypeValue("timeField", LocalTime.class));

        final BigDecimal decimal = convertedRow.getDecimal("decimalField");
        Assert.assertEquals(1234.56789D, decimal.doubleValue(), DELTA);

        final Row keyRow = convertedRow.getRow("__key__");
        Assert.assertEquals("", keyRow.getString("namespace"));
        Assert.assertEquals("", keyRow.getString("app"));
        Assert.assertEquals("\"MyKind\", \"mykey\"", keyRow.getString("path"));
        Assert.assertEquals("MyKind", keyRow.getString("kind"));
        Assert.assertEquals("mykey", keyRow.getString("name"));
        Assert.assertNull(keyRow.getInt64("id"));
    }

}
