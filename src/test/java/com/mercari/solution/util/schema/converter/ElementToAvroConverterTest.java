package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.time.Instant;

public class ElementToAvroConverterTest {

    @Test
    public void testConvert() {

        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .withField("timestampField", Schema.FieldType.TIMESTAMP)
                .build();

        final MElement element = MElement.builder()
                .withString("stringField", "stringValue")
                .withTimestamp("timestampField", Instant.now())
                .build();

        final GenericRecord record = ElementToAvroConverter.convert(schema, element);
        System.out.println(record.getSchema());
        System.out.println(record);

    }

}
