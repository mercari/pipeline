package com.mercari.solution.util.schema.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.ResourceUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToRowConverterTest {

    @Test
    public void testToSchema() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToRowConverter.convertSchema(descriptor);

        assertSchemaFields(schema, descriptor);

        // Assert nested fields
        //// Child
        assertSchemaFields(
                schema.getField("child").getType().getRowSchema(),
                descriptor.getFields().stream()
                        .filter(f -> f.getName().equals("child"))
                        .findAny()
                        .get()
                        .getMessageType());

        //// Map value Child
        Assert.assertEquals(Schema.TypeName.MAP, schema.getField("intChildMapValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("intChildMapValue").getType().getMapKeyType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ROW, schema.getField("intChildMapValue").getType().getMapValueType().getTypeName());
        assertSchemaFields(
                schema.getField("intChildMapValue").getType().getMapValueType().getRowSchema(),
                descriptor.getFields().stream()
                        .filter(f -> f.getName().equals("intChildMapValue"))
                        .findAny()
                        .get()
                        .getMessageType()
                        .getFields().stream()
                        .filter(f -> f.getName().equals("value"))
                        .findAny()
                        .get()
                        .getMessageType());

        //// Repeated Child
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("children").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ROW, schema.getField("children").getType().getCollectionElementType().getTypeName());
        assertSchemaFields(
                schema.getField("children").getType().getCollectionElementType().getRowSchema(),
                descriptor.getFields().stream()
                        .filter(f -> f.getName().equals("children"))
                        .findAny()
                        .get()
                        .getMessageType());


        // Assert nested fields 2
        final Schema childSchema = schema.getField("child").getType().getRowSchema();
        final Descriptors.Descriptor childDescriptor = descriptor.getFields().stream()
                .filter(f -> f.getName().equals("child"))
                .findAny()
                .get()
                .getMessageType();

        //// Grand Child
        assertSchemaFields(
                childSchema.getField("grandchild").getType().getRowSchema(),
                childDescriptor.getFields().stream()
                        .filter(f -> f.getName().equals("grandchild"))
                        .findAny()
                        .get()
                        .getMessageType());
    }

    @Test
    public void testToRow() throws InvalidProtocolBufferException {
        testToRow("data/test.pb");
    }

    @Test
    public void testToRowNull() throws InvalidProtocolBufferException {
        testToRow("data/test_null.pb");
    }

    private void testToRow(final String protoPath) throws InvalidProtocolBufferException {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes(protoPath);

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToRowConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoSchemaUtil.convert(descriptor, protoBytes);

        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.forEach((k, v) -> builder.add(v));
        final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());

        final Row row = ProtoToRowConverter.convert(schema, descriptor, protoBytes, printer);
        assertRowValues(ProtoSchemaUtil.convert(descriptor, message.toByteArray()), row, printer);

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

    private void assertSchemaFields(final Schema schema, final Descriptors.Descriptor descriptor) {

        // Build-in types
        Assert.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("boolValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("stringValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BYTES, schema.getField("bytesValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("intValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("longValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.FLOAT, schema.getField("floatValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, schema.getField("doubleValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("uintValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("ulongValue").getType().getTypeName());

        // Google provided types
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("dateValue").getType().getTypeName());
        Assert.assertTrue(schema.getField("dateValue").getType().getLogicalType() instanceof Date);
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("timeValue").getType().getTypeName());
        Assert.assertTrue(schema.getField("timeValue").getType().getLogicalType() instanceof Time);
        Assert.assertEquals(Schema.TypeName.DATETIME, schema.getField("datetimeValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, schema.getField("timestampValue").getType().getTypeName());

        // Google provided types wrappedValues
        Assert.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("wrappedBoolValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("wrappedStringValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BYTES, schema.getField("wrappedBytesValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("wrappedInt32Value").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("wrappedInt64Value").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("wrappedUInt32Value").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("wrappedUInt64Value").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.FLOAT, schema.getField("wrappedFloatValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, schema.getField("wrappedDoubleValue").getType().getTypeName());

        // Any
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("anyValue").getType().getTypeName());

        // Enum
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("enumValue").getType().getTypeName());
        Assert.assertTrue(schema.getField("enumValue").getType().getLogicalType() instanceof EnumerationType);
        final EnumerationType enumerationType = (EnumerationType)schema.getField("enumValue").getType().getLogicalType();
        final List<String> expectedEnumValues = descriptor.getFields().stream()
                .filter(f -> f.getName().equals("enumValue"))
                .map(Descriptors.FieldDescriptor::getEnumType)
                .map(Descriptors.EnumDescriptor::getValues)
                .flatMap(List::stream)
                .map(Descriptors.EnumValueDescriptor::getName)
                .collect(Collectors.toList());
        Assert.assertEquals(expectedEnumValues, enumerationType.getValues());

        // OneOf
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("entityName").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("entityAge").getType().getTypeName());

        // Map
        Assert.assertEquals(Schema.TypeName.MAP, schema.getField("strIntMapValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("strIntMapValue").getType().getMapKeyType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("strIntMapValue").getType().getMapValueType().getTypeName());
        Assert.assertEquals(Schema.TypeName.MAP, schema.getField("longDoubleMapValue").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("longDoubleMapValue").getType().getMapKeyType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, schema.getField("longDoubleMapValue").getType().getMapValueType().getTypeName());

        // Repeated
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("boolValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("stringValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("bytesValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("intValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("longValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("floatValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("doubleValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("uintValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("ulongValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("dateValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("timeValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("datetimeValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("timestampValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedBoolValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedStringValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedBytesValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedInt32Values").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedInt64Values").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedUInt32Values").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedUInt64Values").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedFloatValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("wrappedDoubleValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("anyValues").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, schema.getField("enumValues").getType().getTypeName());

        Assert.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("boolValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("stringValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BYTES, schema.getField("bytesValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("intValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("longValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.FLOAT, schema.getField("floatValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, schema.getField("doubleValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("uintValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("ulongValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("dateValues").getType().getCollectionElementType().getTypeName());
        Assert.assertTrue(schema.getField("dateValues").getType().getCollectionElementType().getLogicalType() instanceof Date);
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("timeValues").getType().getCollectionElementType().getTypeName());
        Assert.assertTrue(schema.getField("timeValues").getType().getCollectionElementType().getLogicalType() instanceof Time);
        Assert.assertEquals(Schema.TypeName.DATETIME, schema.getField("datetimeValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, schema.getField("timestampValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("wrappedBoolValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("wrappedStringValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BYTES, schema.getField("wrappedBytesValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("wrappedInt32Values").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("wrappedInt64Values").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, schema.getField("wrappedUInt32Values").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT64, schema.getField("wrappedUInt64Values").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.FLOAT, schema.getField("wrappedFloatValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, schema.getField("wrappedDoubleValues").getType().getCollectionElementType().getTypeName());

        Assert.assertEquals(Schema.TypeName.STRING, schema.getField("anyValues").getType().getCollectionElementType().getTypeName());
        Assert.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("enumValues").getType().getCollectionElementType().getTypeName());

        final EnumerationType repeatedEnumerationType = (EnumerationType)schema.getField("enumValues").getType().getCollectionElementType().getLogicalType();
        Assert.assertEquals(expectedEnumValues, repeatedEnumerationType.getValues());
    }

    private void assertRowValues(final DynamicMessage message, final Row row, final JsonFormat.Printer printer) throws InvalidProtocolBufferException {

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

        // Any
        Assert.assertTrue(ProtoSchemaUtil.getValue(message, "anyValue", printer).equals(row.getString("anyValue")));

        // Enum
        if(ProtoSchemaUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(
                    ((Descriptors.EnumValueDescriptor) ProtoSchemaUtil.getFieldValue(message, "enumValue")).getIndex(),
                    ((EnumerationType.Value)row.getValue("enumValue")).getValue());
        } else {
            Assert.assertEquals(0, ((EnumerationType.Value)row.getValue("enumValue")).getValue());
        }

        // OneOf
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), row.getString("entityName"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), row.getInt32("entityAge"));

        // Map
        Map map = new HashMap();
        List<DynamicMessage> mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "strIntMapValue");
        if(ProtoSchemaUtil.hasField(message, "strIntMapValue")) {
            for(var mapMessage : mapMessages) {
                map.put(ProtoSchemaUtil.getFieldValue(mapMessage, "key"), ProtoSchemaUtil.getFieldValue(mapMessage, "value"));
            }
            Assert.assertEquals(map, row.getMap("strIntMapValue"));
        } else {
            Assert.assertEquals(new HashMap<String, Integer>(), row.getMap("strIntMapValue"));
        }

        map.clear();
        mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "longDoubleMapValue");
        if(ProtoSchemaUtil.hasField(message, "longDoubleMapValue")) {
            for (var mapMessage : mapMessages) {
                map.put(ProtoSchemaUtil.getFieldValue(mapMessage, "key"), ProtoSchemaUtil.getFieldValue(mapMessage, "value"));
            }
            Assert.assertEquals(map, row.getMap("longDoubleMapValue"));
        } else {
            Assert.assertEquals(new HashMap<Long, Double>(), row.getMap("longDoubleMapValue"));
        }

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

        if(ProtoSchemaUtil.getFieldValue(message, "anyValues") != null && row.getArray("anyValues") != null) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "anyValues");
            Assert.assertEquals(list.size(), row.getArray("anyValues").size());
            i = 0;
            for (var json : row.getArray("anyValues")) {
                Assert.assertEquals(
                        printer.print(list.get(i)),
                        (json));
                i++;
            }
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

}
