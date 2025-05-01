package com.mercari.solution.util.schema;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.junit.Test;

public class SchemaTest {

    @Test
    public void testParseFields() {

        final String schemaJson = """
                {
                  "fields": [
                    { "name": "stringField", "type": "string", "default": "ok", "mode": "nullable" },
                    { "name": "intField", "type": "int" },
                    { "name": "longField", "type": "long" },
                    { "name": "floatField", "type": "float" },
                    { "name": "doubleField", "type": "double" },
                    { "name": "booleanField", "type": "boolean" },
                    { "name": "bytesField", "type": "bytes" },
                    { "name": "dateField", "type": "date" },
                    { "name": "timeField", "type": "date" },
                    { "name": "timestampField", "type": "date" },
                    { "name": "jsonField", "type": "json" },
                    { "name": "enumerationField", "type": "enum", "symbols": ["red","blue","yellow"] },
                    { "name": "arrayStringField", "type": "string", "mode": "repeated" },
                    { "name": "mapElementField", "type": "map", "valueType": "element", "fields": [
                      { "name": "stringField", "type": "string" },
                      { "name": "intField", "type": "int" },
                      { "name": "longField", "type": "long" },
                      { "name": "floatField", "type": "float" },
                      { "name": "doubleField", "type": "double" },
                      { "name": "booleanField", "type": "boolean" },
                      { "name": "bytesField", "type": "bytes" },
                      { "name": "dateField", "type": "date" },
                      { "name": "timeField", "type": "date" },
                      { "name": "timestampField", "type": "date" },
                      { "name": "jsonField", "type": "json" },
                      { "name": "enumerationField", "type": "enum", "symbols": ["red","blue","yellow"] },
                      { "name": "arrayStringField", "type": "string", "mode": "repeated" }
                    ] }
                  ]
                }
                """;

        final Schema schema = Schema.parse(schemaJson);
        final org.apache.avro.Schema avroSchema = ElementToAvroConverter.convertSchema("root", schema.getFields());
    }

}
