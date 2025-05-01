package com.mercari.solution.util.schema.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ElementToProtoConverterTest {

    @Test
    public void test() throws Exception {

        final byte[] bytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");

        final Schema schema = ProtoToElementConverter.convertSchema(descriptor);

        final Map<String, Object> values = new HashMap<>();
        values.put("stringValue", "text");

        final DynamicMessage message = ElementToProtoConverter.convert(schema, descriptor, values);
    }

}
