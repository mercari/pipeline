package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

public class RowToTableRowConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final Row row = TestDatum.generateRow();
        final TableRow tableRow = RowToTableRowConverter.convert(row);
        testFlatField(tableRow);
        final TableRow childRow = (TableRow)tableRow.get("recordField");
        testFlatField(childRow);
        for(TableRow g : (List<TableRow>)childRow.get("recordArrayField")) {
            testFlatField(g);
        }
        final TableRow grandchildRow = (TableRow)childRow.get("recordField");
        testFlatField(grandchildRow);

        for(TableRow c : (List<TableRow>)tableRow.get("recordArrayField")) {
            testFlatField(c);
            final TableRow gc = (TableRow)c.get("recordField");
            testFlatField(gc);
            for(TableRow g : (List<TableRow>)c.get("recordArrayField")) {
                testFlatField(g);
            }
        }
    }

    private void testFlatField(final TableRow tableRow) {
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), tableRow.get("booleanField"));
        Assert.assertEquals(TestDatum.getStringFieldValue(), tableRow.get("stringField"));
        Assert.assertEquals(TestDatum.getBytesFieldValue(), new String(
                Base64.getDecoder().decode(tableRow.get("bytesField").toString()), StandardCharsets.UTF_8));
        Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), tableRow.get("intField"));
        Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), tableRow.get("longField"));
        Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), (Double)tableRow.get("floatField"), DELTA);
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), (Double)tableRow.get("doubleField"), DELTA);
        Assert.assertEquals(TestDatum.getDateFieldValue().toEpochDay(), LocalDate.parse(tableRow.get("dateField").toString()).toEpochDay());
        Assert.assertEquals(TestDatum.getTimeFieldValue().toSecondOfDay(), LocalTime.parse(tableRow.get("timeField").toString()).toSecondOfDay());
        Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Instant.parse(tableRow.get("timestampField").toString() + "Z").getMillis());
        Assert.assertEquals(TestDatum.getDecimalFieldValue().toString(), tableRow.get("decimalField"));
    }

}
