package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class AvroToRowConverterTest {

    @Test
    public void testConvertSchema() {
        final Schema inputSchema = SchemaBuilder
                .record("root")
                .fields()
                .name("stringField").doc("this is string field").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("jsonField").type(AvroSchemaUtil.NULLABLE_JSON).noDefault()
                .name("intField").doc("this is int field").type(AvroSchemaUtil.REQUIRED_INT).withDefault(0)
                .name("longField").doc("this is long field").type(AvroSchemaUtil.REQUIRED_LONG).withDefault(0L)
                .name("booleanField").doc("this is boolean field").type(AvroSchemaUtil.REQUIRED_BOOLEAN).withDefault(false)
                .name("decimalField").doc("this is decimal field").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
                .name("floatField").doc("this is float field").type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault()
                .name("doubleField").doc("this is double field").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("timeField").doc("this is time field").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE).noDefault()
                .name("dateField").doc("this is date field").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").doc("this is timestamp field").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("bytesField").doc("this is bytes field").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("enumField").doc("this is enum field").orderDescending().type(Schema
                        .createUnion(Schema.create(Schema.Type.NULL), Schema.createEnum("enumField", "", "", Arrays.asList("a","b","c")))).noDefault()
                .name("stringArrayField").doc("this is string array field").type(AvroSchemaUtil.NULLABLE_ARRAY_STRING_TYPE).noDefault()
                .name("jsonArrayField").doc("this is json array field").type(AvroSchemaUtil.NULLABLE_ARRAY_JSON_TYPE).noDefault()
                .name("intArrayField").doc("this is int array field").type(AvroSchemaUtil.NULLABLE_ARRAY_INT_TYPE).noDefault()
                .name("longArrayField").doc("this is long array field").type(AvroSchemaUtil.NULLABLE_ARRAY_LONG_TYPE).noDefault()
                .name("booleanArrayField").doc("this is boolean array field").type(AvroSchemaUtil.NULLABLE_ARRAY_BOOLEAN_TYPE).noDefault()
                .name("decimalArrayField").doc("this is decimal array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DECIMAL_TYPE).noDefault()
                .name("floatArrayField").doc("this is float array field").type(AvroSchemaUtil.NULLABLE_ARRAY_FLOAT_TYPE).noDefault()
                .name("doubleArrayField").doc("this is double array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DOUBLE_TYPE).noDefault()
                .name("timeArrayField").doc("this is time array field").type(AvroSchemaUtil.NULLABLE_ARRAY_TIME_TYPE).noDefault()
                .name("dateArrayField").doc("this is date array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DATE_TYPE).noDefault()
                .name("timestampArrayField").doc("this is timestamp array field").type(AvroSchemaUtil.NULLABLE_ARRAY_TIMESTAMP_TYPE).noDefault()
                .endRecord();
        final org.apache.beam.sdk.schemas.Schema outputSchema = AvroToRowConverter.convertSchema(inputSchema);

        System.out.println(outputSchema);

        // Field
        final org.apache.beam.sdk.schemas.Schema.Field stringField = outputSchema.getField("stringField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true), stringField.getType());
        Assert.assertEquals("this is string field", stringField.getDescription());
        Assert.assertEquals(0, (int)stringField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", stringField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field jsonField = outputSchema.getField("jsonField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true), stringField.getType());
        Assert.assertEquals(1, (int)jsonField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", jsonField.getOptions().getValue("order"));
        Assert.assertEquals("JSON", jsonField.getOptions().getValue("sqlType"));

        final org.apache.beam.sdk.schemas.Schema.Field intField = outputSchema.getField("intField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(false), intField.getType());
        Assert.assertEquals("this is int field", intField.getDescription());
        Assert.assertEquals(2, (int)intField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", intField.getOptions().getValue("order"));
        Assert.assertEquals(0, (int)intField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field longField = outputSchema.getField("longField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(false), longField.getType());
        Assert.assertEquals("this is long field", longField.getDescription());
        Assert.assertEquals(3, (int)longField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", longField.getOptions().getValue("order"));
        Assert.assertEquals(0L, (long)longField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field booleanField = outputSchema.getField("booleanField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN, booleanField.getType());
        Assert.assertEquals("this is boolean field", booleanField.getDescription());
        Assert.assertEquals(4, (int)booleanField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", booleanField.getOptions().getValue("order"));
        Assert.assertEquals(false, (boolean)booleanField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field decimalField = outputSchema.getField("decimalField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL.withNullable(true), decimalField.getType());
        Assert.assertEquals(9, (int)decimalField.getOptions().getValue("scale"));
        Assert.assertEquals(38, (int)decimalField.getOptions().getValue("precision"));
        Assert.assertEquals("this is decimal field", decimalField.getDescription());
        Assert.assertEquals(5, (int)decimalField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", decimalField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field floatField = outputSchema.getField("floatField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT.withNullable(true), floatField.getType());
        Assert.assertEquals("this is float field", floatField.getDescription());
        Assert.assertEquals(6, (int)floatField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", floatField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field doubleField = outputSchema.getField("doubleField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE.withNullable(true), doubleField.getType());
        Assert.assertEquals("this is double field", doubleField.getDescription());
        Assert.assertEquals(7, (int)doubleField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", doubleField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timeField = outputSchema.getField("timeField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()).withNullable(true), timeField.getType());
        Assert.assertEquals("this is time field", timeField.getDescription());
        Assert.assertEquals(8, (int)timeField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", timeField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field dateField = outputSchema.getField("dateField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true), dateField.getType());
        Assert.assertEquals("this is date field", dateField.getDescription());
        Assert.assertEquals(9, (int)dateField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", dateField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timestampField = outputSchema.getField("timestampField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true), timestampField.getType());
        Assert.assertEquals("this is timestamp field", timestampField.getDescription());
        Assert.assertEquals(10, (int)timestampField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", timestampField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field bytesField = outputSchema.getField("bytesField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true), bytesField.getType());
        Assert.assertEquals("this is bytes field", bytesField.getDescription());
        Assert.assertEquals(11, (int)bytesField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", bytesField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field enumField = outputSchema.getField("enumField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(EnumerationType.create("a","b","c")).withNullable(true), enumField.getType());
        Assert.assertEquals("this is enum field", enumField.getDescription());
        Assert.assertEquals(12, (int)enumField.getOptions().getValue("pos"));
        Assert.assertEquals("DESCENDING", enumField.getOptions().getValue("order"));

        // Array Field
        final org.apache.beam.sdk.schemas.Schema.Field stringArrayField = outputSchema.getField("stringArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING).withNullable(true), stringArrayField.getType());
        Assert.assertEquals("this is string array field", stringArrayField.getDescription());
        Assert.assertEquals(13, (int)stringArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", stringArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field jsonArrayField = outputSchema.getField("jsonArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING).withNullable(true), jsonArrayField.getType());
        Assert.assertEquals("JSON", jsonArrayField.getOptions().getValue("sqlType"));
        Assert.assertEquals("this is json array field", jsonArrayField.getDescription());
        Assert.assertEquals(14, (int)jsonArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", jsonArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field intArrayField = outputSchema.getField("intArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.INT32).withNullable(true), intArrayField.getType());
        Assert.assertEquals("this is int array field", intArrayField.getDescription());
        Assert.assertEquals(15, (int)intArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", intArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field longArrayField = outputSchema.getField("longArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.INT64).withNullable(true), longArrayField.getType());
        Assert.assertEquals("this is long array field", longArrayField.getDescription());
        Assert.assertEquals(16, (int)longArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", longArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field booleanArrayField = outputSchema.getField("booleanArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN).withNullable(true), booleanArrayField.getType());
        Assert.assertEquals("this is boolean array field", booleanArrayField.getDescription());
        Assert.assertEquals(17, (int)booleanArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", booleanArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field decimalArrayField = outputSchema.getField("decimalArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL).withNullable(true), decimalArrayField.getType());
        Assert.assertEquals(9, (int)decimalArrayField.getOptions().getValue("scale"));
        Assert.assertEquals(38, (int)decimalArrayField.getOptions().getValue("precision"));
        Assert.assertEquals("this is decimal array field", decimalArrayField.getDescription());
        Assert.assertEquals(18, (int)decimalArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", decimalArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field floatArrayField = outputSchema.getField("floatArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT).withNullable(true), floatArrayField.getType());
        Assert.assertEquals("this is float array field", floatArrayField.getDescription());
        Assert.assertEquals(19, (int)floatArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", floatArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field doubleArrayField = outputSchema.getField("doubleArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE).withNullable(true), doubleArrayField.getType());
        Assert.assertEquals("this is double array field", doubleArrayField.getDescription());
        Assert.assertEquals(20, (int)doubleArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", doubleArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timeArrayField = outputSchema.getField("timeArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType())).withNullable(true), timeArrayField.getType());
        Assert.assertEquals("this is time array field", timeArrayField.getDescription());
        Assert.assertEquals(21, (int)timeArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", timeArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field dateArrayField = outputSchema.getField("dateArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType())).withNullable(true), dateArrayField.getType());
        Assert.assertEquals("this is date array field", dateArrayField.getDescription());
        Assert.assertEquals(22, (int)dateArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", dateArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timestampArrayField = outputSchema.getField("timestampArrayField");
        Assert.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME).withNullable(true), timestampArrayField.getType());
        Assert.assertEquals("this is timestamp array field", timestampArrayField.getDescription());
        Assert.assertEquals(23, (int)timestampArrayField.getOptions().getValue("pos"));
        Assert.assertEquals("ASCENDING", timestampArrayField.getOptions().getValue("order"));

    }

}
