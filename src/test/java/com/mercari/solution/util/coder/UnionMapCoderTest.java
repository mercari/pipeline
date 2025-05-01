package com.mercari.solution.util.coder;

import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnionMapCoderTest {

    @Test
    public void testCoder() {
        final Map<String,Object> values = new HashMap<>();
        values.put("stringField", "stringValue");
        values.put("intField", 1000);
        values.put("longField", -1000L);
        values.put("floatField", 1000F);
        values.put("doubleField", 1000D);
        values.put("bytesField", ByteBuffer.wrap("OKOKOK".getBytes(StandardCharsets.UTF_8)));
        values.put("stringListField", List.of("a", "b", "c"));
        values.put("doubleListField", List.of(1D, 0.1D, 1000.1000D));

        final Map<String,Object> child = new HashMap<>();
        child.put("stringField", "stringValue");
        child.put("intField", 1000);
        child.put("longField", -1000L);
        child.put("stringListField", List.of("a", "b", "c"));
        child.put("doubleListField", List.of(1D, 0.1D, 1000.1000D));
        values.put("childField", child);

        final List<Map<String,Object>> children = new ArrayList<>();
        children.add(Map.of("stringField", "stringValue", "stringListField", List.of("a", "b", "c")));
        values.put("childrenField", children);


        final Coder<Map<String,Object>> coder = UnionMapCoder.mapCoder();
        try(ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
            coder.encode(values, writer);

            final byte[] serialized = writer.toByteArray();
            try(ByteArrayInputStream is = new ByteArrayInputStream(serialized)) {
                final Map<String, Object> deserialized = coder.decode(is);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
