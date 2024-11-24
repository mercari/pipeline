package com.mercari.solution.util.schema;

import com.mercari.solution.TestDatum;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AvroSchemaUtilTest {

    @Test
    public void testSelectFields() {
        final GenericRecord record = TestDatum.generateRecord();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        final Schema schema = AvroSchemaUtil.selectFields(record.getSchema(), fields);

        // schema test
        Assert.assertEquals(5, schema.getFields().size());
        Assert.assertNotNull(schema.getField("stringField"));
        Assert.assertNotNull(schema.getField("intField"));
        Assert.assertNotNull(schema.getField("longField"));
        Assert.assertNotNull(schema.getField("recordField"));
        Assert.assertNotNull(schema.getField("recordArrayField"));

        final Schema schemaChild = AvroSchemaUtil.unnestUnion(schema.getField("recordField").schema());
        Assert.assertEquals(5, schemaChild.getFields().size());
        Assert.assertNotNull(schemaChild.getField("stringField"));
        Assert.assertNotNull(schemaChild.getField("doubleField"));
        Assert.assertNotNull(schemaChild.getField("booleanField"));
        Assert.assertNotNull(schemaChild.getField("recordField"));
        Assert.assertNotNull(schemaChild.getField("recordArrayField"));

        Assert.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schemaChild.getField("recordArrayField").schema()).getType());
        final Schema schemaChildChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schemaChild.getField("recordArrayField").schema()).getElementType());
        Assert.assertEquals(2, schemaChildChildren.getFields().size());
        Assert.assertNotNull(schemaChildChildren.getField("intField"));
        Assert.assertNotNull(schemaChildChildren.getField("floatField"));

        final Schema schemaGrandchild = AvroSchemaUtil.unnestUnion(schemaChild.getField("recordField").schema());
        Assert.assertEquals(2, schemaGrandchild.getFields().size());
        Assert.assertNotNull(schemaGrandchild.getField("intField"));
        Assert.assertNotNull(schemaGrandchild.getField("floatField"));

        Assert.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schema.getField("recordArrayField").schema()).getType());
        final Schema schemaChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schema.getField("recordArrayField").schema()).getElementType());
        Assert.assertEquals(4, schemaChildren.getFields().size());
        Assert.assertNotNull(schemaChildren.getField("stringField"));
        Assert.assertNotNull(schemaChildren.getField("timestampField"));
        Assert.assertNotNull(schemaChildren.getField("recordField"));
        Assert.assertNotNull(schemaChildren.getField("recordArrayField"));

        final Schema schemaChildrenChild = AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordField").schema());
        Assert.assertEquals(2, schemaChildrenChild.getFields().size());
        Assert.assertNotNull(schemaChildrenChild.getField("intField"));
        Assert.assertNotNull(schemaChildrenChild.getField("floatField"));

        Assert.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordArrayField").schema()).getType());
        final Schema schemaChildrenChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordArrayField").schema()).getElementType());
        Assert.assertEquals(2, schemaChildrenChildren.getFields().size());
        Assert.assertNotNull(schemaChildrenChildren.getField("intField"));
        Assert.assertNotNull(schemaChildrenChildren.getField("floatField"));


        // record test
        final GenericRecord selectedRecord = AvroSchemaUtil.toBuilder(schema, record).build();
        Assert.assertEquals(5, selectedRecord.getSchema().getFields().size());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedRecord.get("stringField"));
        Assert.assertEquals(TestDatum.getIntFieldValue(), selectedRecord.get("intField"));
        Assert.assertEquals(TestDatum.getLongFieldValue(), selectedRecord.get("longField"));

        final GenericRecord selectedRecordChild = (GenericRecord) selectedRecord.get("recordField");
        Assert.assertEquals(5, selectedRecordChild.getSchema().getFields().size());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedRecordChild.get("stringField"));
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), selectedRecordChild.get("doubleField"));
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedRecordChild.get("booleanField"));

        final GenericRecord selectedRecordGrandchild = (GenericRecord) selectedRecordChild.get("recordField");
        Assert.assertEquals(2, selectedRecordGrandchild.getSchema().getFields().size());
        Assert.assertEquals(TestDatum.getIntFieldValue(), selectedRecordGrandchild.get("intField"));
        Assert.assertEquals(TestDatum.getFloatFieldValue(), selectedRecordGrandchild.get("floatField"));

        Assert.assertEquals(2, ((List)selectedRecord.get("recordArrayField")).size());
        for(final GenericRecord child : (List<GenericRecord>)selectedRecord.get("recordArrayField")) {
            Assert.assertEquals(4, child.getSchema().getFields().size());
            Assert.assertEquals(TestDatum.getStringFieldValue(), child.get("stringField"));
            Assert.assertEquals(TestDatum.getTimestampFieldValue(), Instant.ofEpochMilli((Long)child.get("timestampField") / 1000));

            Assert.assertEquals(2, ((List)child.get("recordArrayField")).size());
            for(final GenericRecord grandchild : (List<GenericRecord>)child.get("recordArrayField")) {
                Assert.assertEquals(2, grandchild.getSchema().getFields().size());
                Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
                Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
            }

            final GenericRecord grandchild = (GenericRecord) child.get("recordField");
            Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
            Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
        }


        // null fields record test
        final GenericRecord recordNull = TestDatum.generateRecordNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Schema schemaNull = AvroSchemaUtil.selectFields(recordNull.getSchema(), newFields);

        final GenericRecord selectedRecordNull = AvroSchemaUtil.toBuilder(schemaNull, recordNull).build();
        Assert.assertEquals(7, selectedRecordNull.getSchema().getFields().size());
        Assert.assertNull(selectedRecordNull.get("stringField"));
        Assert.assertNull(selectedRecordNull.get("intField"));
        Assert.assertNull(selectedRecordNull.get("longField"));
        Assert.assertNull(selectedRecordNull.get("recordFieldNull"));
        Assert.assertNull(selectedRecordNull.get("recordArrayFieldNull"));

        final GenericRecord selectedRecordChildNull = (GenericRecord) selectedRecordNull.get("recordField");
        Assert.assertEquals(5, selectedRecordChildNull.getSchema().getFields().size());
        Assert.assertNull(selectedRecordChildNull.get("stringField"));
        Assert.assertNull(selectedRecordChildNull.get("doubleField"));
        Assert.assertNull(selectedRecordChildNull.get("booleanField"));

        final GenericRecord selectedRecordGrandchildNull = (GenericRecord) selectedRecordChildNull.get("recordField");
        Assert.assertEquals(2, selectedRecordGrandchildNull.getSchema().getFields().size());
        Assert.assertNull(selectedRecordGrandchildNull.get("intField"));
        Assert.assertNull(selectedRecordGrandchildNull.get("floatField"));

        Assert.assertEquals(2, ((List)selectedRecordNull.get("recordArrayField")).size());
        for(final GenericRecord child : (List<GenericRecord>)selectedRecordNull.get("recordArrayField")) {
            Assert.assertEquals(4, child.getSchema().getFields().size());
            Assert.assertNull(child.get("stringField"));
            Assert.assertNull(child.get("timestampField"));

            Assert.assertEquals(2, ((List)child.get("recordArrayField")).size());
            for(final GenericRecord grandchild : (List<GenericRecord>)child.get("recordArrayField")) {
                Assert.assertEquals(2, grandchild.getSchema().getFields().size());
                Assert.assertNull(grandchild.get("intField"));
                Assert.assertNull(grandchild.get("floatField"));
            }

            final GenericRecord grandchild = (GenericRecord) child.get("recordField");
            Assert.assertNull(grandchild.get("intField"));
            Assert.assertNull(grandchild.get("floatField"));
        }
    }

    @Test
    public void testIsValidFieldName() {
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("myfield"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("Field"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("a1234"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("_a1234"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("f"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("_"));
        Assert.assertTrue(AvroSchemaUtil.isValidFieldName("_1"));

        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("@field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("1field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("1"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName(""));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName(" "));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName(" field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("parent.field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("parent-field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("parent/field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName("parent@field"));
        Assert.assertFalse(AvroSchemaUtil.isValidFieldName(null));
    }

    @Test
    public void testPrimitiveEncodeDecode() throws IOException {

        Boolean b = true;
        byte[] bytes = AvroSchemaUtil.encode(b);
        Object o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.BOOLEAN, bytes);
        Assert.assertEquals(b, o);

        Integer i = 12345;
        bytes = AvroSchemaUtil.encode(i);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.INT32, bytes);
        Assert.assertEquals(i, o);

        Long l = 1234567890L;
        bytes = AvroSchemaUtil.encode(l);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.INT64, bytes);
        Assert.assertEquals(l, o);

        Float f = 12345.67890F;
        bytes = AvroSchemaUtil.encode(f);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.FLOAT32, bytes);
        Assert.assertEquals(f, o);

        Double d = 1234567890.987654D;
        bytes = AvroSchemaUtil.encode(d);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.FLOAT64, bytes);
        Assert.assertEquals(d, o);

        String s = "あいうえおかきくけこ123456789abcdefg";
        bytes = AvroSchemaUtil.encode(s);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.STRING, bytes);
        Assert.assertEquals(s, o);

    }

}
