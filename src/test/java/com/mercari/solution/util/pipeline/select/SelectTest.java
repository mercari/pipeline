package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.select.stateful.StatefulFunction;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectTest {

    @Test
    public void testCreateStateAvroSchema() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("intField", Schema.FieldType.INT32),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("floatField", Schema.FieldType.FLOAT32),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("enumField", Schema.FieldType.enumeration(List.of("a","b","c"))),
                Schema.Field.of("timestampField", Schema.FieldType.TIMESTAMP),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .withField("longField", Schema.FieldType.INT64)
                        .build())),
                Schema.Field.of("arrayNestedField", Schema.FieldType.array(Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .withField("longField", Schema.FieldType.INT64)
                        .build())))
        );

        final String selectJsonArray = """
                [
                  { "name": "sumLongField", "func": "sum", "field": "longField" },
                  { "name": "sumNestedLongField", "func": "sum", "field": "nestedField.longField" },
                  { "name": "sumLongWithCountRangeField", "func": "sum", "field": "longField", "range": { "count": 3 } },
                  { "name": "sumLongWithDurationRangeField", "func": "sum", "field": "longField", "range": { "duration": 3, "unit": "second", "offset": 0 } },
                  { "name": "maxLongField", "func": "max", "field": "longField", "range": { "duration": 30, "unit": "second", "offset": 0 } },
                  { "name": "minLongField", "func": "min", "field": "longField", "range": { "count": 3, "offset": 1 }},
                  { "name": "avgLongField", "func": "avg", "field": "longField", "range": { "count": 5, "offset": 3 }},
                  { "name": "stdLongField", "func": "std", "field": "longField", "range": { "count": 20, "offset": 3 }},
                  { "name": "lastStringField", "func": "last", "field": "stringField" },
                  { "name": "maxAvgLongField", "func": "max", "field": "avgLongField" },
                  { "name": "lagLongField", "func": "lag", "expression": "(longField[2] - longField[0])/(1 + longField[0])"}
                ]
                """;

        final JsonArray array = new Gson().fromJson(selectJsonArray, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);
        final Schema inputSchema = Schema.builder().withFields(inputFields).build();

        final org.apache.avro.Schema stateAvroSchema = Select.createStateAvroSchema(inputSchema, selectFunctions);
        System.out.println(stateAvroSchema);
    }

    @Test
    public void testStatefulAggregate() {

        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("intField", Schema.FieldType.INT32),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("floatField", Schema.FieldType.FLOAT32),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("enumField", Schema.FieldType.enumeration(List.of("a","b","c"))),
                Schema.Field.of("timestampField", Schema.FieldType.TIMESTAMP),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .build())),
                Schema.Field.of("arrayNestedField", Schema.FieldType.array(Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .build())))
        );

        final String selectJsonArray = """
                [
                  { "name": "sumLongField", "func": "sum", "field": "longField" },
                  { "name": "sumLongWithCountRangeField", "func": "sum", "field": "longField", "range": { "count": 3 } },
                  { "name": "sumLongWithDurationRangeField", "func": "sum", "field": "longField", "range": { "duration": 3, "unit": "second", "offset": 0 } },
                  { "name": "maxLongField", "func": "max", "field": "longField", "range": { "duration": 30, "unit": "second", "offset": 0 } },
                  { "name": "minLongField", "func": "min", "field": "longField", "range": { "count": 3, "offset": 1 }},
                  { "name": "avgLongField", "func": "avg", "field": "longField", "range": { "count": 5, "offset": 3 }},
                  { "name": "stdLongField", "func": "std", "field": "longField", "range": { "count": 20, "offset": 3 }},
                  { "name": "lastStringField", "func": "last", "field": "stringField" }
                ]
                """;

        final JsonArray array = new Gson().fromJson(selectJsonArray, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);

        final Schema outputSchema = SelectFunction.createSchema(selectFunctions);
        //Assert.assertEquals(Schema.Type.int64, outputSchema.getField("sumLongField").getFieldType().getType());

        final Select select = Select.of(selectFunctions);
        select.setup();

        final List<TimestampedValue<MElement>> buffer = new ArrayList<>();
        buffer.add(MElement.builder()
                .withString("stringField", "a")
                .withInt64("longField", 0L)
                .withEventTime(Instant.parse("2025-04-01T00:00:00Z"))
                .build()
                .asTimestampedValue());
        buffer.add(MElement.builder()
                .withString("stringField", "b")
                .withInt64("longField", 10L)
                .withEventTime(Instant.parse("2025-04-01T00:00:01Z"))
                .build()
                .asTimestampedValue());
        buffer.add(MElement.builder()
                .withString("stringField", "c")
                .withInt64("longField", 20L)
                .withEventTime(Instant.parse("2025-04-01T00:00:02Z"))
                .build()
                .asTimestampedValue());
        buffer.add(MElement.builder()
                .withString("stringField", "d")
                .withInt64("longField", 30L)
                .withEventTime(Instant.parse("2025-04-01T00:00:03Z"))
                .build()
                .asTimestampedValue());
        buffer.add(MElement.builder()
                .withString("stringField", "e")
                .withInt64("longField", 40L)
                .withEventTime(Instant.parse("2025-04-01T00:00:04Z"))
                .build()
                .asTimestampedValue());
        buffer.add(MElement.builder()
                .withString("stringField", "f")
                .withInt64("longField", 50L)
                .withEventTime(Instant.parse("2025-04-01T00:00:05Z"))
                .build()
                .asTimestampedValue());

        final Instant eventTime = Instant.parse("2025-04-01T00:00:06Z");
        final MElement input = MElement
                .builder()
                .withString("stringField", "c")
                .withInt64("longField", 60L)
                .withEventTime(eventTime)
                .build();

        final StatefulFunction.RangeBound maxRange = StatefulFunction.calcMaxRange(selectFunctions);
        System.out.println(maxRange);

        final Map<String, Object> output = select.select(input, buffer, eventTime);
        System.out.println(output);

    }

}
