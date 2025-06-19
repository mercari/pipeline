package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.DataType;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryTest {

    @Test
    public void testSingleQuery() {
        final List<Schema.Field> input1Fields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("nestedField", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("sField", Schema.FieldType.STRING),
                        Schema.Field.of("lField", Schema.FieldType.INT64),
                        Schema.Field.of("dField", Schema.FieldType.FLOAT64)
                ))))
        );
        final List<Schema.Field> input2Fields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("llField", Schema.FieldType.INT64),
                Schema.Field.of("ddField", Schema.FieldType.FLOAT64)
        );

        final String sql = """
                WITH Source AS (
                  SELECT
                    stringField,
                    SUM(nn.lField) AS l,
                    CURRENT_DATE_('Asia/Tokyo') AS cdate
                  FROM
                    TestTable, UNNEST(nestedField) AS nn
                  GROUP BY
                    stringField
                )
                
                SELECT
                  Source.*,
                  CAST(SubTestTable.llField AS VARCHAR) AS ss,
                  SubTestTable.ddField
                FROM
                  Source
                LEFT JOIN
                  SubTestTable
                ON
                  Source.stringField = SubTestTable.stringField
                
                """;

        final Map<String, Schema> inputSchemas = Map.of(
                "TestTable", Schema.of(input1Fields),
                "SubTestTable", Schema.of(input2Fields)
        );
        final Query query = Query.of(inputSchemas, sql);
        final Schema outputSchema = query.getOutputSchema();
        System.out.println(outputSchema);

        query.setup();
        final MElement input1 = MElement.of(Map.of(
                "stringField", "a",
                "longField", 1L,
                "doubleField", 10D,
                "nestedField", List.of(Map
                        .of(
                                "sField", "a",
                                "lField", 1L,
                                "dField", 10D
                        ),
                        Map
                                .of(
                                        "sField", "a",
                                        "lField", 2L,
                                        "dField", 10D
                                ),
                        Map
                                .of(
                                        "sField", "a",
                                        "lField", 3L,
                                        "dField", 10D
                                ))
        ), Instant.parse("2025-05-01T00:00:00Z"));

        final MElement input2 = MElement.of(Map.of(
                "stringField", "a",
                "llField", 1L,
                "ddField", 10D
        ), Instant.parse("2025-05-01T00:00:00Z"));

        org.apache.avro.Schema aschema1 = ElementToAvroConverter.convertSchema(input1Fields);
        org.apache.avro.Schema aschema2 = ElementToAvroConverter.convertSchema(input2Fields);

        final MElement input1_ = input1.convert(Schema.of(aschema1), DataType.AVRO);
        final MElement input2_ = input2.convert(Schema.of(aschema2), DataType.AVRO);
        final Map<String, List<MElement>> inputs = new HashMap<>();
        inputs.put("TestTable", List.of(input1_));
        inputs.put("SubTestTable", List.of(input2_));
        final List<MElement> outputs = query.execute(inputs, Instant.parse("2025-05-01T00:00:00Z"));
        System.out.println(outputs);

    }

}
