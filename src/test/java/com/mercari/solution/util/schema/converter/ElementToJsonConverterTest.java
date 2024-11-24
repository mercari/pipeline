package com.mercari.solution.util.schema.converter;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

public class ElementToJsonConverterTest {

    @Test
    public void test() {

        final Schema schema = Schema
                .builder()
                .withField("booleanField", Schema.FieldType.BOOLEAN)
                .withField("stringField", Schema.FieldType.STRING)
                .withField("int32Field", Schema.FieldType.INT32)
                .withField("int64Field", Schema.FieldType.INT64)
                .withField("float32Field", Schema.FieldType.FLOAT32)
                .withField("float64Field", Schema.FieldType.FLOAT64)
                .withField("bytesField", Schema.FieldType.BYTES)
                .withField("dateField", Schema.FieldType.DATE)
                .withField("timeField", Schema.FieldType.TIME)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .withField("mapField", Schema.FieldType.map(Schema.FieldType.STRING))
                .withField("elementField", Schema.FieldType.element(Schema
                        .builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .build()))
                .withField("stringListField", Schema.FieldType.array(Schema.FieldType.STRING))
                .build();

        final MElement element = MElement.builder()
                .withBool("booleanField", true)
                .withString("stringField", "abc")
                .withInt32("int32Field", 10)
                .withInt64("int64Field", 10L)
                .withBytes("bytesField", "OKOKOK".getBytes(StandardCharsets.UTF_8))
                .withDate("dateField", LocalDate.parse("2024-01-01"))
                .withTime("timeField", LocalTime.parse("01:23:45"))
                .withTimestamp("timestampField", Instant.parse("2024-01-01T01:23:45.678Z"))
                .withElement("elementField", MElement
                        .builder()
                        .withString("stringField", "def")
                        .build())
                .withStringList("stringListField", List.of("a","b","c"))
                .build();

        final JsonObject jsonObject = ElementToJsonConverter.convert(schema, element.asPrimitiveMap());
    }

}
