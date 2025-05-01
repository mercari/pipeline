package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RowToProtoConverterTest {

    @Test
    public void testConvert() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToRowConverter.convertSchema(descriptor);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());

        final Row row = ProtoToRowConverter.convert(schema, descriptor, protoBytes, printer);

        final DynamicMessage message1 = RowToProtoConverter.convert(descriptor, row);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRowValues(message1, row, printer);
        assertRowValues(message2, row, printer);

        testNest(message1, row, printer);
        testNest(message2, row, printer);
    }

    @Test
    public void testConvertNull() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test_null.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToRowConverter.convertSchema(descriptor);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());

        final Row row = ProtoToRowConverter.convert(schema, descriptor, protoBytes, printer);

        final DynamicMessage message1 = RowToProtoConverter.convert(descriptor, row);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRowNullValues(message1, printer);
        assertRowNullValues(message2, printer);
    }

    private void testNest(final DynamicMessage message, final Row row, final JsonFormat.Printer printer) {
        if(ProtoSchemaUtil.hasField(message, "child")) {
            final Row child = row.getRow("child");
            final DynamicMessage childMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "child");
            assertRowValues(childMessage, child, printer);

            final Collection<Row> grandchildren = child.getArray("grandchildren");
            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final Row c : grandchildren) {
                assertRowValues(grandchildrenMessages.get(i), c, printer);
                i++;
            }

            if(ProtoSchemaUtil.hasField(childMessage, "grandchild")) {
                final Row grandchild = child.getRow("grandchild");
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(childMessage, "grandchild");
                assertRowValues(grandchildMessage, grandchild, printer);
            }
        }

        final Collection<Row> children = row.getArray("children");
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "children");
        int i = 0;
        for(final Row c : children) {
            assertRowValues(childrenMessages.get(i), c, printer);
            i++;
        }
    }

    private void assertRowValues(final DynamicMessage message, final Row row, final JsonFormat.Printer printer) {

        // Build-in type
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValue", printer), row.getBoolean("boolValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValue", printer), row.getString("stringValue"));
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(row.getBytes("bytesValue"), StandardCharsets.UTF_8));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValue", printer), row.getInt32("intValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValue", printer), row.getInt64("longValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValue", printer), row.getFloat("floatValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValue", printer), row.getDouble("doubleValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValue", printer), row.getInt32("uintValue"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValue", printer), row.getInt64("ulongValue"));

        // Google-provided type
        if(ProtoSchemaUtil.hasField(message,"dateValue")) {
            Assert.assertEquals(
                    ProtoSchemaUtil.getEpochDay(
                            (com.google.type.Date)(ProtoSchemaUtil.convertBuildInValue("google.type.Date",
                                    (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "dateValue")))),
                    ((LocalDate)row.getValue("dateValue")).toEpochDay());
        }
        if(ProtoSchemaUtil.hasField(message,"timeValue")) {
            Assert.assertEquals(
                    ProtoSchemaUtil.getSecondOfDay((com.google.type.TimeOfDay)(ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "timeValue")))),
                    ((LocalTime)row.getValue("timeValue")).toSecondOfDay());
        }

        if(ProtoSchemaUtil.hasField(message,"datetimeValue")) {
            Assert.assertEquals(
                    ProtoSchemaUtil.getEpochMillis((com.google.type.DateTime)(ProtoSchemaUtil.convertBuildInValue("google.type.DateTime",
                            (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "datetimeValue")))),
                    ((Instant)row.getValue("datetimeValue")).getMillis());
        }

        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer),
                row.getBoolean("wrappedBoolValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer),
                row.getString("wrappedStringValue"));
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(row.getBytes("wrappedBytesValue"), StandardCharsets.UTF_8));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer),
                row.getInt32("wrappedInt32Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer),
                row.getInt64("wrappedInt64Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer),
                row.getFloat("wrappedFloatValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer),
                row.getDouble("wrappedDoubleValue"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer),
                row.getInt32("wrappedUInt32Value"));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer),
                row.getInt64("wrappedUInt64Value"));

        // Any not support

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex(),
                    ((EnumerationType.Value)row.getValue("enumValue")).getValue());
        } else {
            Assert.assertEquals(0, ((EnumerationType.Value)row.getValue("enumValue")).getValue());
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), row.getString("entityName"));
        } else if(ProtoSchemaUtil.hasField(message, "entityAge")) {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), row.getInt32("entityAge"));
        }

        // Map not support

        // Repeated
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValues", printer), row.getArray("boolValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValues", printer), row.getArray("stringValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValues", printer), row.getArray("intValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValues", printer), row.getArray("longValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValues", printer), row.getArray("floatValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValues", printer), row.getArray("doubleValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValues", printer), row.getArray("uintValues"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValues", printer), row.getArray("ulongValues"));

        List<DynamicMessage> list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "dateValues");
        int i = 0;
        if(ProtoSchemaUtil.hasField(message, "dateValues")) {
            Assert.assertEquals(list.size(), row.getArray("dateValues").size());

            for (var localDate : row.getArray("dateValues")) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getEpochDay(
                                (com.google.type.Date) (ProtoSchemaUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        ((LocalDate) localDate).toEpochDay());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Date>(), row.getArray("dateValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timeValues");
            Assert.assertEquals(list.size(), row.getArray("timeValues").size());
            i = 0;
            for (var localTime : row.getArray("timeValues")) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        ((LocalTime) localTime).toSecondOfDay());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<LocalTime>(), row.getArray("timeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "datetimeValues");
            Assert.assertEquals(list.size(), row.getArray("datetimeValues").size());
            i = 0;
            for (var localDateTime : row.getArray("datetimeValues")) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        ((Instant) localDateTime).getMillis());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Instant>(), row.getArray("datetimeValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timestampValues");
            Assert.assertEquals(list.size(), row.getArray("timestampValues").size());
            i = 0;
            for (var instant : row.getArray("timestampValues")) {
                Assert.assertEquals(
                        Timestamps.toMillis(
                                (com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        ((Instant) instant).getMillis());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Instant>(), row.getArray("timestampValues"));
        }

        if(ProtoSchemaUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>) ProtoSchemaUtil.getFieldValue(message, "enumValues");
            Assert.assertEquals(enums.size(), row.getArray("enumValues").size());
            i = 0;
            for(var json : row.getArray("enumValues")) {
                Assert.assertEquals(
                        enums.get(i).getIndex(),
                        ((EnumerationType.Value)json).getValue());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<EnumerationType.Value>(), row.getArray("enumValues"));
        }

    }

    private void assertRowNullValues(final DynamicMessage message, final JsonFormat.Printer printer) {
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
