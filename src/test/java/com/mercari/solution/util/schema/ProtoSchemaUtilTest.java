package com.mercari.solution.util.schema;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.converter.ProtoToAvroConverter;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ProtoSchemaUtilTest {

    @Test
    public void testGetAsStandard() {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes("data/test.pb");

        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final JsonFormat.Printer printer = ProtoSchemaUtil.createJsonPrinter(descriptors);
        final Schema schema = ProtoToAvroConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoSchemaUtil.convert(descriptor, protoBytes);
        for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            final Object value = ProtoSchemaUtil.getAsStandard(entry.getKey(), entry.getValue());
        }
        final Object v = ProtoSchemaUtil.asStandardMap(message, null, printer);
        final Object c = ProtoSchemaUtil.asPrimitiveMap(message, List.of("latlngValue"), printer);
    }

}
