package com.mercari.solution.util.schema.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.ResourceUtil;
import org.apache.avro.LogicalTypes;
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
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToAvroConverterTest {

    @Test
    public void testToSchema() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);

        assertSchemaFields(schema, descriptor);

        final Schema childSchema = getFieldSchema(schema, "child");
        final Descriptors.Descriptor childDescriptor = ProtoSchemaUtil.getField(descriptor, "child").getMessageType();
        assertSchemaFields(childSchema, childDescriptor);

        final Schema grandchildSchema = getFieldSchema(childSchema, "grandchild");
        final Descriptors.Descriptor grandchildDescriptor = ProtoSchemaUtil.getField(childDescriptor, "grandchild").getMessageType();
        assertSchemaFields(grandchildSchema, grandchildDescriptor);
    }

    @Test
    public void testToRecord() throws Exception {
        testToRecord("data/test.pb");
    }

    @Test
    public void testToRecordNull() throws Exception {
        testToRecord("data/test_null.pb");
    }

    private void testToRecord(final String protoPath) throws Exception {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes(protoPath);

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoSchemaUtil.convert(descriptor, protoBytes);

        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);

        final GenericRecord record = ProtoToAvroConverter.convert(schema, descriptor, protoBytes, printer);
        assertRecordValues(message, record, printer);

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

    private void assertSchemaFields(final Schema schema, final Descriptors.Descriptor descriptor) {

        // Build-in types
        Assert.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "boolValue").getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "stringValue").getType());
        Assert.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "bytesValue").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "intValue").getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "longValue").getType());
        Assert.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "floatValue").getType());
        Assert.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "doubleValue").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "uintValue").getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "ulongValue").getType());

        // Google provided types
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "dateValue").getType());
        Assert.assertEquals(LogicalTypes.date(), getFieldSchema(schema, "dateValue").getLogicalType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timeValue").getType());
        Assert.assertEquals(LogicalTypes.timeMicros(), getFieldSchema(schema, "timeValue").getLogicalType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "datetimeValue").getType());
        Assert.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "datetimeValue").getLogicalType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timestampValue").getType());
        Assert.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "timestampValue").getLogicalType());

        // Google provided types wrappedValues
        Assert.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "wrappedBoolValue").getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "wrappedStringValue").getType());
        Assert.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "wrappedBytesValue").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedInt32Value").getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedInt64Value").getType());
        Assert.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "wrappedFloatValue").getType());
        Assert.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "wrappedDoubleValue").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedUInt32Value").getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedUInt64Value").getType());

        // Any
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "anyValue").getType());

        // Enum
        Assert.assertEquals(Schema.Type.ENUM, getFieldSchema(schema, "enumValue").getType());
        Assert.assertEquals(
                ProtoSchemaUtil.getField(descriptor, "enumValue").getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList()),
                getFieldSchema(schema, "enumValue").getEnumSymbols());

        // OneOf
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "entityName").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "entityAge").getType());

        // Map
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "strIntMapValue").getType());
        Assert.assertEquals(Schema.Type.RECORD, getFieldSchema(schema, "strIntMapValue").getElementType().getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(getFieldSchema(schema, "strIntMapValue").getElementType(), "key").getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(getFieldSchema(schema, "strIntMapValue").getElementType(), "value").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "longDoubleMapValue").getType());
        Assert.assertEquals(Schema.Type.RECORD, getFieldSchema(schema, "longDoubleMapValue").getElementType().getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(getFieldSchema(schema, "longDoubleMapValue").getElementType(), "key").getType());
        Assert.assertEquals(Schema.Type.DOUBLE, getFieldSchema(getFieldSchema(schema, "longDoubleMapValue").getElementType(), "value").getType());

        // Repeated
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "boolValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "stringValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "bytesValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "intValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "longValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "floatValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "doubleValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "uintValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "ulongValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "dateValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "timeValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "datetimeValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedBoolValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedStringValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedBytesValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedInt32Values").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedInt64Values").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedUInt32Values").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedUInt64Values").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedFloatValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "wrappedDoubleValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "anyValues").getType());
        Assert.assertEquals(Schema.Type.ARRAY, getFieldSchema(schema, "enumValues").getType());

        Assert.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "boolValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "stringValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "bytesValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "intValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "longValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "floatValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "doubleValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "uintValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "ulongValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "dateValues").getElementType().getType());
        Assert.assertEquals(LogicalTypes.date(), getFieldSchema(schema, "dateValues").getElementType().getLogicalType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "timeValues").getElementType().getType());
        Assert.assertEquals(LogicalTypes.timeMicros(), getFieldSchema(schema, "timeValues").getElementType().getLogicalType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "datetimeValues").getElementType().getType());
        Assert.assertEquals(LogicalTypes.timestampMicros(), getFieldSchema(schema, "datetimeValues").getElementType().getLogicalType());
        Assert.assertEquals(Schema.Type.BOOLEAN, getFieldSchema(schema, "wrappedBoolValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "wrappedStringValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.BYTES, getFieldSchema(schema, "wrappedBytesValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedInt32Values").getElementType().getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedInt64Values").getElementType().getType());
        Assert.assertEquals(Schema.Type.INT, getFieldSchema(schema, "wrappedUInt32Values").getElementType().getType());
        Assert.assertEquals(Schema.Type.LONG, getFieldSchema(schema, "wrappedUInt64Values").getElementType().getType());
        Assert.assertEquals(Schema.Type.FLOAT, getFieldSchema(schema, "wrappedFloatValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.DOUBLE, getFieldSchema(schema, "wrappedDoubleValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.STRING, getFieldSchema(schema, "anyValues").getElementType().getType());
        Assert.assertEquals(Schema.Type.ENUM, getFieldSchema(schema, "enumValues").getElementType().getType());

        Assert.assertEquals(
                ProtoSchemaUtil.getField(descriptor, "enumValues").getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList()),
                getFieldSchema(schema, "enumValues").getElementType().getEnumSymbols());
    }

    private void assertRecordValues(final DynamicMessage message, final GenericRecord record, final JsonFormat.Printer printer) throws InvalidProtocolBufferException {

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

        // Any
        Assert.assertTrue(ProtoSchemaUtil.getValue(message, "anyValue", printer).equals(record.get("anyValue")));

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
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityName", printer), record.get("entityName"));
        Assert.assertEquals(ProtoSchemaUtil.getValue(message, "entityAge", printer), record.get("entityAge"));

        // Map
        List<DynamicMessage> mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getValue(message, "strIntMapValue", printer);
        List<GenericRecord> mapRecords = (List<GenericRecord>)record.get("strIntMapValue");
        Assert.assertEquals(mapMessages.size(), mapRecords.size());
        for(int i=0; i<mapMessages.size(); i++) {
            Assert.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                    mapRecords.get(i).get("key"));
            Assert.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer),
                    mapRecords.get(i).get("value"));
        }

        mapRecords = (List<GenericRecord>)record.get("longDoubleMapValue");
        mapMessages = (List<DynamicMessage>) ProtoSchemaUtil.getValue(message, "longDoubleMapValue", printer);
        Assert.assertEquals(mapMessages.size(), mapRecords.size());
        for (int i = 0; i < mapMessages.size(); i++) {
            Assert.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "key", printer),
                    mapRecords.get(i).get("key"));
            Assert.assertEquals(
                    ProtoSchemaUtil.getValue(mapMessages.get(i), "value", printer),
                    mapRecords.get(i).get("value"));
        }

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

        if(ProtoSchemaUtil.getFieldValue(message, "anyValues") != null && record.get("anyValues") != null) {
            list = (List<DynamicMessage>) ProtoSchemaUtil.getFieldValue(message, "anyValues");
            Assert.assertEquals(list.size(), ((List)record.get("anyValues")).size());
            i = 0;
            for (var json : (List<String>)record.get("anyValues")) {
                Assert.assertEquals(
                        printer.print(list.get(i)),
                        (json));
                i++;
            }
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

    private Schema getFieldSchema(Schema schema, final String field) {
        final Schema fieldSchema = schema.getFields().stream()
                .filter(f -> f.name().equals(field))
                .map(Schema.Field::schema)
                .findAny()
                .orElse(null);
        if(fieldSchema == null) {
            return null;
        }
        return AvroSchemaUtil.unnestUnion(fieldSchema);
    }


}
