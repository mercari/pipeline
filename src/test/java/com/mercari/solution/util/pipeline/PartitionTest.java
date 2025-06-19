package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.List;

public class PartitionTest {

    @Test
    public void test() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.of(List.of(
                        Schema.Field.of("stringField", Schema.FieldType.STRING)
                ))))
        );

        final String partitionJsonArray = """
                [
                  {
                    "name": "partition1",
                    "filter": [
                      { "key": "nestedField.stringField", "op": "!=", "value": "" }
                    ],
                    "select": [
                      { "name": "stringField" },
                      { "name": "longField", "value": 1, "type": "int64" }
                    ]
                  }
                ]
                """;

        final String json = """
                {
                  "stringField": "",
                  "nestedField": {
                    "stringField": "a"
                  }
                }
                """;


        final JsonArray partitionJson = new Gson().fromJson(partitionJsonArray, JsonArray.class);
        final List<Partition> partitions = Partition.of(partitionJson, Schema.of(inputFields));
        for(final Partition partition : partitions) {
            partition.setup();
        }

        final MElement input1 = MElement.of(JsonToElementConverter.convert(inputFields, json), Instant.parse("2025-01-01T00:00:00Z"));

        for(final Partition partition : partitions) {
            final List<MElement> outputs = partition.execute(input1, Instant.parse("2025-01-01T00:00:00Z"));
            System.out.println(partition.getName() + " : " + outputs);
        }

    }

}
