package com.mercari.solution.util.schema.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroToProtoConverterTest {

    @Test
    public void testConvert() {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);

        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);

        final GenericRecord record = ProtoToAvroConverter.convert(schema, descriptor, protoBytes, printer);
        final DynamicMessage message1 = AvroToProtoConverter.convert(descriptor, record);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRecordValues(message1, record, printer);
        assertRecordValues(message2, record, printer);

        testNest(message1, record, printer);
        testNest(message2, record, printer);
    }

    @Test
    public void testConvertNull() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test_null.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);

        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);

        final GenericRecord record = ProtoToAvroConverter.convert(schema, descriptor, protoBytes, printer);
        final DynamicMessage message1 = AvroToProtoConverter.convert(descriptor, record);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRecordNullValues(message1, printer);
        assertRecordNullValues(message2, printer);
    }

    private void testNest(final DynamicMessage message, final GenericRecord record, final JsonFormat.Printer printer) {
        if(ProtoSchemaUtil.hasField(message, "child")) {
            final GenericRecord child = (GenericRecord) record.get("child");
            final DynamicMessage childMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "child");
            assertRecordValues(childMessage, child, printer);

            final List<GenericRecord> grandchildren = (List<GenericRecord>)child.get("grandchildren");
            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final GenericRecord r : grandchildren) {
                assertRecordValues(grandchildrenMessages.get(i), r, printer);
                i++;
            }

            if(ProtoSchemaUtil.hasField(childMessage, "grandchild")) {
                final GenericRecord grandchild = (GenericRecord) child.get("grandchild");
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(childMessage, "grandchild");
                assertRecordValues(grandchildMessage, grandchild, printer);
            }
        }

        final List<GenericRecord> children = (List<GenericRecord>)record.get("children");
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "children");
        int i = 0;
        for(final GenericRecord r : children) {
            assertRecordValues(childrenMessages.get(i), r, printer);
            i++;
        }
    }

    private void assertRecordValues(final DynamicMessage message, final GenericRecord record, final JsonFormat.Printer printer) {

        // Build-in type
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValue", printer), record.get("boolValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValue", printer), record.get("stringValue"));
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(((ByteBuffer)record.get("bytesValue")).array(), StandardCharsets.UTF_8));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValue", printer), record.get("intValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValue", printer), record.get("longValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValue", printer), record.get("floatValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValue", printer), record.get("doubleValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValue", printer), record.get("uintValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValue", printer), record.get("ulongValue"));

        // Google-provided type
        Assert.assertEquals(
                (int)((LocalDate) ProtoSchemaUtil.getValue(message, "dateValue", printer)).toEpochDay(),
                record.get("dateValue"));
        Assert.assertEquals(
                ((LocalTime) ProtoSchemaUtil.getValue(message, "timeValue", printer)).toSecondOfDay() * 1000_000L,
                record.get("timeValue"));
        Assert.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "datetimeValue", printer)).getMillis() * 1000,
                record.get("datetimeValue"));
        Assert.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "timestampValue", printer)).getMillis() * 1000,
                record.get("timestampValue"));

        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer),
                record.get("wrappedBoolValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer),
                record.get("wrappedStringValue"));
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(((ByteBuffer)record.get("wrappedBytesValue")).array(), StandardCharsets.UTF_8));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer),
                record.get("wrappedInt32Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer),
                record.get("wrappedInt64Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer),
                record.get("wrappedFloatValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer),
                record.get("wrappedDoubleValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer),
                record.get("wrappedUInt32Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer),
                record.get("wrappedUInt64Value"));

        // Any not supported

        // Enum
        GenericData.EnumSymbol enumSymbol = (GenericData.EnumSymbol)record.get("enumValue");
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex(),
                    enumSymbol.getSchema().getEnumOrdinal(record.get("enumValue").toString()));
        } else {
            Assert.assertEquals(0, enumSymbol.getSchema().getEnumOrdinal(record.get("enumValue").toString()));
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), record.get("entityName"));
        } else if(ProtoSchemaUtil.hasField(message, "entityAge")) {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), record.get("entityAge"));
        }

        // Map not supported

        // Repeated
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValues", printer), record.get("boolValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValues", printer), record.get("stringValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValues", printer), record.get("intValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValues", printer), record.get("longValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValues", printer), record.get("floatValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValues", printer), record.get("doubleValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValues", printer), record.get("uintValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValues", printer), record.get("ulongValues"));

        List<DynamicMessage> list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "dateValues");
        int i = 0;
        if(ProtoSchemaUtil.hasField(message, "dateValues")) {
            Assert.assertEquals(list.size(), ((List)record.get("dateValues")).size());
            for (int epochDay : (List<Integer>)record.get("dateValues")) {
                Assert.assertEquals(
                        (int) ProtoSchemaUtil.getEpochDay(
                                (com.google.type.Date) (ProtoSchemaUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        epochDay);
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Date>(), record.get("dateValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timeValues");
            Assert.assertEquals(list.size(), ((List)record.get("timeValues")).size());
            i = 0;
            for (long microSecondOfDay : (List<Long>)record.get("timeValues")) {
                Assert.assertEquals(
                        1000_000 * ProtoSchemaUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        microSecondOfDay);
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<LocalTime>(), record.get("timeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "datetimeValues");
            Assert.assertEquals(list.size(), ((List)record.get("datetimeValues")).size());
            i = 0;
            for (long epochMicros : (List<Long>)record.get("datetimeValues")) {
                Assert.assertEquals(
                        1000 * ProtoSchemaUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        epochMicros);
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Instant>(), record.get("datetimeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timestampValues");
            Assert.assertEquals(list.size(), ((List)record.get("timestampValues")).size());
            i = 0;
            for (long epochMicros : (List<Long>)record.get("timestampValues")) {
                Assert.assertEquals(
                        Timestamps.toMicros(
                                (com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        epochMicros);
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Instant>(), record.get("timestampValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>) ProtoSchemaUtil.getFieldValue(message, "enumValues");
            Assert.assertEquals(enums.size(), ((List)record.get("enumValues")).size());
            i = 0;
            for(var enumValue : (List<GenericData.EnumSymbol>)record.get("enumValues")) {
                Assert.assertEquals(
                        enums.get(i).getName(),
                        enumValue.toString());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<EnumerationType.Value>(), record.get("enumValues"));
        }
    }

    private void assertRecordNullValues(final DynamicMessage message, final JsonFormat.Printer printer) {
        // Build-in type
        Assert.assertEquals(false, ProtoSchemaUtil.getValue(message, "boolValue", printer));
        Assert.assertEquals("", ProtoSchemaUtil.getValue(message, "stringValue", printer));
        Assert.assertEquals("", new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8));
        Assert.assertEquals(0, ProtoSchemaUtil.getValue(message, "intValue", printer));
        Assert.assertEquals(0L, ProtoSchemaUtil.getValue(message, "longValue", printer));
        Assert.assertEquals(0F, ProtoSchemaUtil.getValue(message, "floatValue", printer));
        Assert.assertEquals(0D, ProtoSchemaUtil.getValue(message, "doubleValue", printer));
        Assert.assertEquals(0, ProtoSchemaUtil.getValue(message, "uintValue", printer));
        Assert.assertEquals(0L, ProtoSchemaUtil.getValue(message, "ulongValue", printer));

        // Google-provided type
        if(ProtoSchemaUtil.hasField(message,"dateValue")) {
            Assert.assertEquals(
                    LocalDate.of(1,1,1).toEpochDay(),
                    ProtoSchemaUtil.getEpochDay(
                            (com.google.type.Date)(ProtoSchemaUtil.convertBuildInValue("google.type.Date",
                                    (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "dateValue"))))
            );
        }

        // TODO Somehow it becomes null when it is 00:00:00.
        /*
        if(ProtoSchemaUtil.hasField(message,"timeValue")) {
            Assert.assertEquals(
                    LocalTime.of(0,0,0).toSecondOfDay(),
                    ProtoSchemaUtil.getSecondOfDay((com.google.type.TimeOfDay)(ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "timeValue")))));
        }
        */
        if(ProtoSchemaUtil.hasField(message,"datetimeValue")) {
            Assert.assertEquals(
                    -62135596800000L,
                    ProtoSchemaUtil.getEpochMillis((com.google.type.DateTime)(ProtoSchemaUtil.convertBuildInValue("google.type.DateTime",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "datetimeValue")))));
        }

        Assert.assertEquals(
                false,
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer));
        Assert.assertEquals(
                "",
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer));
        Assert.assertEquals(
                "",
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8));
        Assert.assertEquals(
                0,
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer));
        Assert.assertEquals(
                0L,
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer));
        Assert.assertEquals(
                0F,
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer));
        Assert.assertEquals(
                0D,
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer));
        Assert.assertEquals(
                0,
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer));
        Assert.assertEquals(
                0L,
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer));

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(
                    0,
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex());
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            Assert.assertEquals("", ProtoSchemaUtil.getValue(message, "entityName", printer));
        } else if(ProtoSchemaUtil.hasField(message, "entityAge")) {
            Assert.assertEquals(0, ProtoSchemaUtil.getValue(message, "entityAge", printer));
        }

        // Repeated
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "boolValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "stringValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "intValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "longValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "floatValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "doubleValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "uintValues", printer));
        Assert.assertEquals(new ArrayList<>(), ProtoSchemaUtil.getValue(message, "ulongValues", printer));
    }

}
