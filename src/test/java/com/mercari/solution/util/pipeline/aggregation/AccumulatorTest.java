package com.mercari.solution.util.pipeline.aggregation;

import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AccumulatorTest {

    /*
    @Test
    public void testCoder() throws Exception {
        final Accumulator accumulator = Accumulator.of();
        accumulator.empty = false;
        accumulator.put(Schema.FieldType.STRING, "stringField0", "a");
        accumulator.put(Schema.FieldType.STRING, "stringField1", "b");
        accumulator.put(Schema.FieldType.STRING, "stringField2", "c");
        accumulator.putDouble("doubleField0", 0D);
        accumulator.putDouble("doubleField1", 1D);
        accumulator.putDouble("doubleField2", 2D);
        accumulator.putLong("longField1", 100L);
        accumulator.putLong("longField2", -100L);
        accumulator.putLong("longField1", 200L);
        final AvroCoder<Accumulator> coder = AvroCoder.of(Accumulator.class);
        try(ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
            coder.encode(accumulator, writer);
            final byte[] serialized = writer.toByteArray();
            try(ByteArrayInputStream is = new ByteArrayInputStream(serialized)) {
                final Accumulator deserialized = coder.decode(is);
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.STRING, "stringField0"),
                        deserialized.get(Schema.FieldType.STRING, "stringField0"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.STRING, "stringField1"),
                        deserialized.get(Schema.FieldType.STRING, "stringField1"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.STRING, "stringField2"),
                        deserialized.get(Schema.FieldType.STRING, "stringField2"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.DOUBLE, "doubleField0"),
                        deserialized.get(Schema.FieldType.DOUBLE, "doubleField0"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.DOUBLE, "doubleField1"),
                        deserialized.get(Schema.FieldType.DOUBLE, "doubleField1"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.DOUBLE, "doubleField2"),
                        deserialized.get(Schema.FieldType.DOUBLE, "doubleField2"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.INT64, "longField0"),
                        deserialized.get(Schema.FieldType.INT64, "longField0"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.INT64, "longField1"),
                        deserialized.get(Schema.FieldType.INT64, "longField1"));
                Assert.assertEquals(
                        accumulator.get(Schema.FieldType.INT64, "longField2"),
                        deserialized.get(Schema.FieldType.INT64, "longField2"));
            }
        }

    }

     */

}
