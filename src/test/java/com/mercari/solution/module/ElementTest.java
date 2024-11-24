package com.mercari.solution.module;

import com.mercari.solution.util.coder.ElementCoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

public class ElementTest {

    @Test
    public void test() throws Exception {
        final MElement child = MElement.builder()
                .withBool("boolField", false)
                .withString("stringField", "NG")
                .withBytes("bytesField", Base64.getEncoder().encode("NgNgNg".getBytes(StandardCharsets.UTF_8)))
                .withDate("dateField", LocalDate.parse("2023-10-10"))
                .withTime("timeField", LocalTime.parse("23:16:54"))
                .withTimestamp("timestampField", Instant.parse("2020-01-23T12:23:45.678Z"))
                .build();

        final MElement element = MElement.builder()
                .withBool("boolField", true)
                .withString("stringField", "OK")
                .withBytes("bytesField", Base64.getEncoder().encode("OkOkOk".getBytes(StandardCharsets.UTF_8)))
                .withDate("dateField", LocalDate.parse("2021-10-10"))
                .withTime("timeField", LocalTime.parse("21:12:10"))
                .withTimestamp("timestampField", Instant.parse("2024-12-12T00:12:34.000Z"))
                .withElement("elementField", child)
                .build();


        final Schema schema = Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING)));
        final ElementCoder coder = ElementCoder.of(schema);

        try(final ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
            coder.encode(element, writer);
            final byte[] serialized = writer.toByteArray();
            try(ByteArrayInputStream is = new ByteArrayInputStream(serialized)) {
                final MElement deserialized = coder.decode(is);
            }
        }
    }

}
