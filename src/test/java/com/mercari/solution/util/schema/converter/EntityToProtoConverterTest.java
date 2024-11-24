package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.EntityToProtoConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityToProtoConverterTest {

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

        final Entity entity = ProtoToEntityConverter.convert(schema, descriptor, protoBytes, printer);

        final DynamicMessage message1 = EntityToProtoConverter.convert(descriptor, entity);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertEntityValues(message1, entity, printer);
        assertEntityValues(message2, entity, printer);

        testNest(message1, entity, printer);
        testNest(message2, entity, printer);
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

        final Entity entity = ProtoToEntityConverter.convert(schema, descriptor, protoBytes, printer);

        final DynamicMessage message1 = EntityToProtoConverter.convert(descriptor, entity);
        final DynamicMessage message2 = ProtoSchemaUtil.convert(descriptor, message1.toByteArray());

        assertRowNullValues(message1, printer);
        assertRowNullValues(message2, printer);
    }

    private void testNest(final DynamicMessage message, final Entity entity, final JsonFormat.Printer printer) {
        if(ProtoSchemaUtil.hasField(message, "child")) {
            final Entity child = entity.getPropertiesOrThrow("child").getEntityValue();
            final DynamicMessage childMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(message, "child");
            assertEntityValues(childMessage, child, printer);

            final List<Entity> grandchildren = child.getPropertiesOrThrow("grandchildren").getArrayValue()
                    .getValuesList()
                    .stream()
                    .map(Value::getEntityValue)
                    .collect(Collectors.toList());

            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final Entity e : grandchildren) {
                assertEntityValues(grandchildrenMessages.get(i), e, printer);
                i++;
            }

            if(ProtoSchemaUtil.hasField(childMessage, "grandchild")) {
                final Entity grandchild = child.getPropertiesOrThrow("grandchild").getEntityValue();
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoSchemaUtil.getFieldValue(childMessage, "grandchild");
                assertEntityValues(grandchildMessage, grandchild, printer);
            }
        }

        final List<Entity> children = entity.getPropertiesOrThrow("children").getArrayValue()
                .getValuesList()
                .stream()
                .map(Value::getEntityValue)
                .collect(Collectors.toList());
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "children");
        int i = 0;
        for(final Entity e : children) {
            assertEntityValues(childrenMessages.get(i), e, printer);
            i++;
        }
    }

    private void assertEntityValues(final DynamicMessage message, final Entity entity, final JsonFormat.Printer printer) {

        // Build-in type
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValue", printer), entity.getPropertiesOrThrow("boolValue").getBooleanValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValue", printer), entity.getPropertiesOrThrow("stringValue").getStringValue());
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(entity.getPropertiesOrThrow("bytesValue").getBlobValue().toByteArray(), StandardCharsets.UTF_8));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValue", printer), (int)entity.getPropertiesOrThrow("intValue").getIntegerValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValue", printer), entity.getPropertiesOrThrow("longValue").getIntegerValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValue", printer), (float)entity.getPropertiesOrThrow("floatValue").getDoubleValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValue", printer), entity.getPropertiesOrThrow("doubleValue").getDoubleValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValue", printer), (int)entity.getPropertiesOrThrow("uintValue").getIntegerValue());
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValue", printer), entity.getPropertiesOrThrow("ulongValue").getIntegerValue());

        // Google-provided type
        Assert.assertEquals(
                (int)((LocalDate) ProtoSchemaUtil.getValue(message, "dateValue", printer)).toEpochDay(),
                LocalDate.parse(entity.getPropertiesOrThrow("dateValue").getStringValue()).toEpochDay());
        Assert.assertEquals(
                ((LocalTime) ProtoSchemaUtil.getValue(message, "timeValue", printer)).toSecondOfDay(),
                LocalTime.parse(entity.getPropertiesOrThrow("timeValue").getStringValue()).toSecondOfDay());
        Assert.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "datetimeValue", printer)).getMillis(),
                Timestamps.toMillis(entity.getPropertiesOrThrow("datetimeValue").getTimestampValue()));
        Assert.assertEquals(
                ((Instant) ProtoSchemaUtil.getValue(message, "timestampValue", printer)).getMillis(),
                Timestamps.toMillis(entity.getPropertiesOrThrow("timestampValue").getTimestampValue()));

        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedBoolValue", printer),
                entity.getPropertiesOrThrow("wrappedBoolValue").getBooleanValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedStringValue", printer),
                entity.getPropertiesOrThrow("wrappedStringValue").getStringValue());
        Assert.assertEquals(
                new String((byte[]) ProtoSchemaUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(entity.getPropertiesOrThrow("wrappedBytesValue").getBlobValue().toByteArray(), StandardCharsets.UTF_8));
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt32Value", printer),
                (int)entity.getPropertiesOrThrow("wrappedInt32Value").getIntegerValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedInt64Value", printer),
                entity.getPropertiesOrThrow("wrappedInt64Value").getIntegerValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedFloatValue", printer),
                (float)entity.getPropertiesOrThrow("wrappedFloatValue").getDoubleValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedDoubleValue", printer),
                entity.getPropertiesOrThrow("wrappedDoubleValue").getDoubleValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt32Value", printer),
                (int)entity.getPropertiesOrThrow("wrappedUInt32Value").getIntegerValue());
        Assert.assertEquals(
                ProtoSchemaUtil.getValue(message, "wrappedUInt64Value", printer),
                entity.getPropertiesOrThrow("wrappedUInt64Value").getIntegerValue());

        // Any is not supported

        // Enum
        String enumSymbol = entity.getPropertiesOrThrow("enumValue").getStringValue();
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getName(),
                    enumSymbol);
        } else {
            Assert.assertEquals(ProtoSchemaUtil.getField(message, "enumValue").getEnumType().getValues().get(0).getName(), enumSymbol);
        }

        // OneOf
        if(ProtoSchemaUtil.hasField(message, "entityName")) {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), entity.getPropertiesOrThrow("entityName").getStringValue());
        } else {
            Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), (int) entity.getPropertiesOrThrow("entityAge").getIntegerValue());
        }

        // Map is not supported

        // Repeated
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "boolValues", printer), entity.getPropertiesOrThrow("boolValues")
                .getArrayValue().getValuesList().stream().map(Value::getBooleanValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "stringValues", printer), entity.getPropertiesOrThrow("stringValues")
                .getArrayValue().getValuesList().stream().map(Value::getStringValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "intValues", printer), entity.getPropertiesOrThrow("intValues")
                .getArrayValue().getValuesList().stream().map(Value::getIntegerValue).map(Long::intValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "longValues", printer), entity.getPropertiesOrThrow("longValues")
                .getArrayValue().getValuesList().stream().map(Value::getIntegerValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "floatValues", printer), entity.getPropertiesOrThrow("floatValues")
                .getArrayValue().getValuesList().stream().map(Value::getDoubleValue).map(Double::floatValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "doubleValues", printer), entity.getPropertiesOrThrow("doubleValues")
                .getArrayValue().getValuesList().stream().map(Value::getDoubleValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "uintValues", printer), entity.getPropertiesOrThrow("uintValues")
                .getArrayValue().getValuesList().stream().map(Value::getIntegerValue).map(Long::intValue).collect(Collectors.toList()));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "ulongValues", printer), entity.getPropertiesOrThrow("ulongValues")
                .getArrayValue().getValuesList().stream().map(Value::getIntegerValue).collect(Collectors.toList()));

        List<DynamicMessage> list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "dateValues");
        final List<String> dateList = entity.getPropertiesOrThrow("dateValues").getArrayValue().getValuesList()
                .stream()
                .map(Value::getStringValue)
                .collect(Collectors.toList());
        int i = 0;
        if(ProtoSchemaUtil.hasField(message, "dateValues")) {
            Assert.assertEquals(list.size(), dateList.size());
            for (String date : dateList) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getEpochDay(
                                (com.google.type.Date) (ProtoSchemaUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        LocalDate.parse(date).toEpochDay());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<String>(), dateList);
        }

        final List<String> timeList = entity.getPropertiesOrThrow("timeValues").getArrayValue().getValuesList()
                .stream()
                .map(Value::getStringValue)
                .collect(Collectors.toList());
        if(ProtoSchemaUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timeValues");
            Assert.assertEquals(list.size(), timeList.size());
            i = 0;
            for (String time : timeList) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoSchemaUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        LocalTime.parse(time).toSecondOfDay());
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<String>(), timeList);
        }

        final List<Timestamp> datetimeList = entity.getPropertiesOrThrow("datetimeValues").getArrayValue().getValuesList()
                .stream()
                .map(Value::getTimestampValue)
                .collect(Collectors.toList());
        if(ProtoSchemaUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "datetimeValues");
            Assert.assertEquals(list.size(), datetimeList.size());
            i = 0;
            for (Timestamp datetime : datetimeList) {
                Assert.assertEquals(
                        ProtoSchemaUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoSchemaUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        Timestamps.toMillis(datetime));
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Timestamp>(), datetimeList);
        }

        final List<Timestamp> timestampList = entity.getPropertiesOrThrow("timestampValues").getArrayValue().getValuesList()
                .stream()
                .map(Value::getTimestampValue)
                .collect(Collectors.toList());
        if(ProtoSchemaUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "timestampValues");
            Assert.assertEquals(list.size(), timestampList.size());
            i = 0;
            for (Timestamp timestamp : timestampList) {
                Assert.assertEquals(
                        Timestamps.toMicros(
                                (com.google.protobuf.Timestamp) (ProtoSchemaUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        Timestamps.toMicros(timestamp));
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<Timestamp>(), timestampList);
        }

        final List<String> enumList = entity.getPropertiesOrThrow("enumValues").getArrayValue().getValuesList()
                .stream()
                .map(Value::getStringValue)
                .collect(Collectors.toList());
        if(ProtoSchemaUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>) ProtoSchemaUtil.getFieldValue(message, "enumValues");
            Assert.assertEquals(enums.size(), enumList.size());
            i = 0;
            for(String enumValue : enumList) {
                Assert.assertEquals(
                        enums.get(i).getName(),
                        enumValue);
                i++;
            }
        } else {
            Assert.assertEquals(new ArrayList<String>(), enumList);
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
