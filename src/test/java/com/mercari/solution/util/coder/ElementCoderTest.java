package com.mercari.solution.util.coder;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElementCoderTest {

    @Test
    public void testCoder() {

        final Schema schema = Schema.builder()
                .withField(Schema.Field.of("stringField", Schema.FieldType.STRING))
                .withField(Schema.Field.of("intField", Schema.FieldType.INT32))
                .withField(Schema.Field.of("longField", Schema.FieldType.INT64))
                .withField(Schema.Field.of("stringListField", Schema.FieldType.array(Schema.FieldType.STRING)))
                .withField(Schema.Field.of("doubleListField", Schema.FieldType.array(Schema.FieldType.FLOAT64)))
                .build();

        final Map<String,Object> values = new HashMap<>();
        values.put("stringField", "stringValue");
        values.put("intField", 1000);
        values.put("longField", -1000L);
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

        final MElement element = MElement.of(values, Instant.parse("2024-01-01T00:00:00Z"));

        final Coder<MElement> coder = ElementCoder.of(schema);
        try(ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
            coder.encode(element, writer);

            final byte[] serialized = writer.toByteArray();
            try(ByteArrayInputStream is = new ByteArrayInputStream(serialized)) {
                final MElement deserialized = coder.decode(is);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
