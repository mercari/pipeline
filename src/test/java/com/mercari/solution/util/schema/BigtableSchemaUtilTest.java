package com.mercari.solution.util.schema;

import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class BigtableSchemaUtilTest {

    public static class Parameters {
        public String text;
        public List<BigtableSchemaUtil.ColumnFamilyProperties> columns;
    }

    @Test
    public void testCreateSchema() {



        final String config = """
                {
                  "text": "okok",
                  "columns": [
                    {
                      "family": "a",
                      "qualifiers": [
                        { "name": "f1", "field": "field1", "type": "element", "fields" :[{"name": "aa", "type": "string"}] }
                      ],
                      "format": "bytes"
                    }
                  ]
                }
                """;

        final Parameters parameters = new GsonBuilder().create().fromJson(config, Parameters.class);
        parameters.columns.forEach(BigtableSchemaUtil.ColumnFamilyProperties::setupSource);
        System.out.println(parameters.columns.getFirst().toString());
        final Schema schema = BigtableSchemaUtil.createSchema(parameters.columns);
        System.out.println(schema);

    }

    @Test
    public void testByteStringBytes() {
        ByteString byteString = BigtableSchemaUtil.toByteString("abc");
        Object value = BigtableSchemaUtil.toPrimitiveValueFromBytes(Schema.FieldType.STRING, byteString);
        Assert.assertEquals("abc", value);
    }

    @Test
    public void testToByteStringHadoop() {
        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .withField("intField", Schema.FieldType.INT32)
                .withField("longField", Schema.FieldType.INT64)
                .withField("floatField", Schema.FieldType.FLOAT32)
                .withField("doubleField", Schema.FieldType.FLOAT64)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .withField("dateField", Schema.FieldType.DATE)
                .withField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("intArrayField", Schema.FieldType.array(Schema.FieldType.INT32))
                .withField("elementField", Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .withField("longField", Schema.FieldType.STRING)
                        .withField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                        .build()))
                .build();

        final MElement element = MElement.builder()
                .withString("stringField", "stringValue")
                .withInt32("intField", 10)
                .withInt64("longField", 10L)
                .withFloat32("floatField", 10.10F)
                .withFloat64("doubleField", 10.10D)
                .withTimestamp("timestampField", Instant.parse("2024-11-01T00:00:00Z"))
                .withDate("dateField", LocalDate.parse("2024-10-10"))
                .withStringList("stringArrayField", List.of("a","b","c"))
                .withStringList("intArrayField", List.of())
                .withMap("elementField", Map.of(
                        "stringField", "childStringValue",
                        "longField", 10L,
                        "stringArrayField", List.of("A", "B", "C")))
                .build();

        final ByteString byteString = BigtableSchemaUtil.toByteStringHadoop(element.asPrimitiveMap());
        final Map<String, Object> values = (Map<String, Object>) BigtableSchemaUtil.toPrimitiveValueFromWritable(Schema.FieldType.element(schema), byteString);
        Assert.assertEquals("stringValue", values.get("stringField"));
        Assert.assertEquals(10, values.get("intField"));
        Assert.assertEquals(10L, values.get("longField"));
        Assert.assertEquals(10.10F, values.get("floatField"));
        Assert.assertEquals(10.10D, values.get("doubleField"));
        Assert.assertEquals(DateTimeUtil.toEpochMicroSecond(Instant.parse("2024-11-01T00:00:00Z")), values.get("timestampField"));
        Assert.assertEquals(Long.valueOf(LocalDate.of(2024,10,10).toEpochDay()).intValue(), values.get("dateField"));
        Assert.assertEquals(List.of("a","b","c"), values.get("stringArrayField"));
        final Map<String, Object> child = (Map<String, Object>) values.get("elementField");
        Assert.assertEquals("childStringValue", child.get("stringField"));
        Assert.assertEquals(10L, child.get("longField"));
        Assert.assertEquals(List.of("A", "B", "C"), child.get("stringArrayField"));
    }

}
