package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.TestDatum;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class TableRowToAvroConverterTest {

    @Test
    public void convertTest() {
        final GenericRecord originalRecord = TestDatum.generateRecord();
        final TableRow tableRow = AvroToTableRowConverter.convert(originalRecord);
        final GenericRecord record = TableRowToAvroConverter.convert(originalRecord.getSchema(), tableRow);

        testFlatField(originalRecord, record);

        final GenericRecord childRecord = (GenericRecord)record.get("recordField");
        testFlatField((GenericRecord)record.get("recordField"), childRecord);
        for(GenericRecord g : (List<GenericRecord>)childRecord.get("recordArrayField")) {
            testFlatField(originalRecord, g);
        }
        final GenericRecord grandchild = (GenericRecord)childRecord.get("recordField");
        testFlatField(originalRecord, grandchild);

        for(GenericRecord c : (List<GenericRecord>)record.get("recordArrayField")) {
            testFlatField(originalRecord, c);
            final GenericRecord gc = (GenericRecord)c.get("recordField");
            testFlatField(originalRecord, gc);
            for(GenericRecord g : (List<GenericRecord>)c.get("recordArrayField")) {
                testFlatField(originalRecord, g);
            }
        }
    }

    @Test
    public void convertTestNull() {
        final GenericRecord originalRecord = TestDatum.generateRecordNull();
        final TableRow tableRow = AvroToTableRowConverter.convert(originalRecord);
        final GenericRecord record = TableRowToAvroConverter.convert(originalRecord.getSchema(), tableRow);

        testFlatField(originalRecord, record);

        final GenericRecord childRecord = (GenericRecord)record.get("recordField");
        testFlatField((GenericRecord)record.get("recordField"), childRecord);
        for(GenericRecord g : (List<GenericRecord>)childRecord.get("recordArrayField")) {
            testFlatField(originalRecord, g);
        }
        final GenericRecord grandchild = (GenericRecord)childRecord.get("recordField");
        testFlatField(originalRecord, grandchild);

        for(GenericRecord c : (List<GenericRecord>)record.get("recordArrayField")) {
            testFlatField(originalRecord, c);
            final GenericRecord gc = (GenericRecord)c.get("recordField");
            testFlatField(originalRecord, gc);
            for(GenericRecord g : (List<GenericRecord>)c.get("recordArrayField")) {
                testFlatField(originalRecord, g);
            }
        }
    }

    private void testFlatField(final GenericRecord originalRecord, final GenericRecord record) {
        Assert.assertEquals(originalRecord.get("booleanField"), record.get("booleanField"));
        Assert.assertEquals(originalRecord.get("stringField"), record.get("stringField"));
        Assert.assertEquals(originalRecord.get("bytesField"), record.get("bytesField"));
        Assert.assertEquals(originalRecord.get("intField"), record.get("intField"));
        Assert.assertEquals(originalRecord.get("longField"), record.get("longField"));
        Assert.assertEquals(originalRecord.get("floatField"), record.get("floatField"));
        Assert.assertEquals(originalRecord.get("doubleField"), record.get("doubleField"));
        Assert.assertEquals(originalRecord.get("dateField"), record.get("dateField"));
        Assert.assertEquals(originalRecord.get("timeField"), record.get("timeField"));
        Assert.assertEquals(originalRecord.get("timestampField"), record.get("timestampField"));
        Assert.assertEquals(originalRecord.get("decimalField"), record.get("decimalField"));
    }

}
