package com.mercari.solution.util.pipeline;

import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.StructToMapConverter;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;


public class FilterTest {

    @Test
    public void testLeafCompare() {

        // Number
        var leaf1 = new Filter.ConditionLeaf();
        leaf1.setKey("");
        leaf1.setValue(new Gson().fromJson("1", JsonElement.class));

        leaf1.setOp(Filter.Op.EQUAL);
        Assert.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is(null, leaf1));

        leaf1.setOp(Filter.Op.GREATER);
        Assert.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assert.assertTrue(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assert.assertTrue(Filter.is(12.312, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertTrue(Filter.is(2212310.12221, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assert.assertFalse(Filter.is(-10, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertFalse(Filter.is(-10, leaf1));

        leaf1.setOp(Filter.Op.LESSER);
        Assert.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.LESSER);
        Assert.assertFalse(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertFalse(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.LESSER);
        Assert.assertTrue(Filter.is(-10, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertTrue(Filter.is(-10, leaf1));

        // Number in, notin
        var leaf2 = new Filter.ConditionLeaf();
        leaf2.setKey("");
        leaf2.setValue(new Gson().fromJson("[1,2,3]", JsonArray.class));

        leaf2.setOp(Filter.Op.IN);
        Assert.assertTrue(Filter.is(1, leaf2));
        Assert.assertTrue(Filter.is(2, leaf2));
        Assert.assertTrue(Filter.is(3, leaf2));
        Assert.assertFalse(Filter.is(4, leaf2));
        Assert.assertFalse(Filter.is(-3, leaf2));
        Assert.assertFalse(Filter.is(-4.12, leaf2));

        leaf2.setOp(Filter.Op.NOT_IN);
        Assert.assertFalse(Filter.is(1, leaf2));
        Assert.assertFalse(Filter.is(2, leaf2));
        Assert.assertFalse(Filter.is(3, leaf2));
        Assert.assertTrue(Filter.is(-100, leaf2));

        // String
        var leaf3 = new Filter.ConditionLeaf();
        leaf3.setKey("");
        leaf3.setValue(new Gson().fromJson("a", JsonElement.class));

        leaf3.setOp(Filter.Op.EQUAL);
        Assert.assertTrue(Filter.is("a", leaf3));
        Assert.assertFalse(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is("a", leaf3));
        Assert.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.GREATER);
        Assert.assertFalse(Filter.is("a", leaf3));
        Assert.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertTrue(Filter.is("a", leaf3));
        Assert.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.LESSER);
        Assert.assertFalse(Filter.is("a", leaf3));
        Assert.assertFalse(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertTrue(Filter.is("a", leaf3));
        Assert.assertFalse(Filter.is("b", leaf3));

        // String in, notin
        var leaf4 = new Filter.ConditionLeaf();
        leaf4.setKey("");
        leaf4.setValue(new Gson().fromJson("['a','b','c']", JsonArray.class));

        leaf4.setOp(Filter.Op.IN);
        Assert.assertTrue(Filter.is("a", leaf4));
        Assert.assertTrue(Filter.is("b", leaf4));
        Assert.assertTrue(Filter.is("c", leaf4));
        Assert.assertFalse(Filter.is("d", leaf4));
        Assert.assertFalse(Filter.is("dsafa", leaf4));
        Assert.assertFalse(Filter.is("A", leaf4));

        leaf4.setOp(Filter.Op.NOT_IN);
        Assert.assertFalse(Filter.is("a", leaf4));
        Assert.assertFalse(Filter.is("b", leaf4));
        Assert.assertFalse(Filter.is("c", leaf4));
        Assert.assertTrue(Filter.is("dfa", leaf4));

        // Null
        var leaf5 = new Filter.ConditionLeaf();
        leaf5.setKey("");
        leaf5.setValue(new Gson().fromJson("null", JsonElement.class));

        leaf5.setOp(Filter.Op.EQUAL);
        Assert.assertTrue(Filter.is(null, leaf5));
        Assert.assertFalse(Filter.is("b", leaf5));
        leaf5.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is(null, leaf5));
        Assert.assertTrue(Filter.is("b", leaf5));

        // Date
        var leaf6 = new Filter.ConditionLeaf();
        leaf6.setKey("");
        leaf6.setValue(new Gson().fromJson("2021-08-21", JsonElement.class));

        leaf6.setOp(Filter.Op.EQUAL);
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.GREATER);
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.LESSER);
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assert.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assert.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));

        // Timestamp
        var leaf7 = new Filter.ConditionLeaf();
        leaf7.setKey("");
        leaf7.setValue(new Gson().fromJson("\"2021-08-21T10:30:45Z\"", JsonElement.class));

        leaf7.setOp(Filter.Op.EQUAL);
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.NOT_EQUAL);
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.GREATER);
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.LESSER);
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assert.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assert.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));

    }

    @Test
    public void testNodeCompare() {
        var leaf1 = new Filter.ConditionLeaf();
        leaf1.setKey("field1");
        leaf1.setValue(new Gson().fromJson("a", JsonElement.class));
        leaf1.setOp(Filter.Op.EQUAL);

        var leaf2 = new Filter.ConditionLeaf();
        leaf2.setKey("field2");
        leaf2.setValue(new Gson().fromJson("1", JsonElement.class));
        leaf2.setOp(Filter.Op.EQUAL);

        // AND
        var node1 = new Filter.ConditionNode();
        node1.setType(Filter.Type.AND);
        node1.setLeaves(Arrays.asList(leaf1, leaf2));

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.STRING)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        Struct struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        // OR
        var node2 = new Filter.ConditionNode();
        node2.setType(Filter.Type.OR);
        node2.setLeaves(Arrays.asList(leaf1, leaf2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        // NEST AND(AND,OR)
        var node3 = new Filter.ConditionNode();
        node3.setType(Filter.Type.AND);
        node3.setNodes(Arrays.asList(node1, node2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        // NEST OR(AND,OR)
        var node4 = new Filter.ConditionNode();
        node4.setType(Filter.Type.OR);
        node4.setNodes(Arrays.asList(node1, node2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        // NEST OR(AND,OR) with Leaves
        var leaf3 = new Filter.ConditionLeaf();
        leaf3.setKey("field2");
        leaf3.setValue(new Gson().fromJson("1", JsonElement.class));
        leaf3.setOp(Filter.Op.NOT_EQUAL);

        var node5 = new Filter.ConditionNode();
        node5.setType(Filter.Type.OR);
        node5.setNodes(Arrays.asList(node1, node2));
        node5.setLeaves(Arrays.asList(leaf3));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        // NEST AND(AND,OR) with Leaves
        var node6 = new Filter.ConditionNode();
        node6.setType(Filter.Type.AND);
        node6.setNodes(Arrays.asList(node1, node2));
        node6.setLeaves(Arrays.asList(leaf3));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assert.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assert.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assert.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));
    }

    @Test
    public void testNodeParseElement() {

        final String filter1String =
                "{\n" +
                        "  \"or\": [\n" +
                        "    { \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                        "      {\n" +
                        "        \"and\": [\n" +
                        "          { \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                        "          { \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        final JsonElement filter1 = new Gson().fromJson(filter1String, JsonElement.class);
        Struct struct = Struct.newBuilder()
                .set("field1").to(1L)
                .set("field2").to(2L)
                .set("field3").to(3L)
                .build();
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        Assert.assertFalse(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));


        // Simple case
        final String filter2String = "[\n" +
                "{ \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                "{ \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                "{ \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                "]\n";

        final JsonElement filter2 = new Gson().fromJson(filter2String, JsonElement.class);
        struct = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assert.assertTrue(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assert.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        Assert.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));
    }

    @Test
    public void testNodeParseObject() {

        final String filter1String =
                "{\n" +
                        "  \"or\": [\n" +
                        "    { \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                        "      {\n" +
                        "        \"and\": [\n" +
                        "          { \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                        "          { \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        final JsonObject filter1 = new Gson().fromJson(filter1String, JsonObject.class);
        Struct struct = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Map<String,Object> values = StructToMapConverter.convert(struct);
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), values));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct);
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assert.assertTrue(Filter.filter(Filter.parse(filter1), values));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        values = StructToMapConverter.convert(struct);
        Assert.assertFalse(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assert.assertFalse(Filter.filter(Filter.parse(filter1), values));

        final String filter2String =
                "{ \"key\": \"field1\", \"op\": \"=\", \"value\": 1 }";

        final JsonObject filter2 = new Gson().fromJson(filter2String, JsonObject.class);
        Struct struct2 = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct2);
        Assert.assertTrue(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct2, Instant.now())));
        Assert.assertTrue(Filter.filter(Filter.parse(filter2), values));

        struct2 = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(1)
                .set("field3").to(1)
                .build();
        values = StructToMapConverter.convert(struct2);
        Assert.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct2, Instant.now())));
        Assert.assertFalse(Filter.filter(Filter.parse(filter2), values));


        final String filter3String =
                "{ \"expression\": \"(field1 / field2) - field3\", \"op\": \">\", \"value\": 0 }";

        final JsonObject filter3 = new Gson().fromJson(filter3String, JsonObject.class);
        Struct struct3 = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct3);
        Assert.assertFalse(Filter.filter(Filter.parse(filter3), schema, MElement.of(struct3, Instant.now())));
        Assert.assertFalse(Filter.filter(Filter.parse(filter3), values));

        struct3 = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(1)
                .set("field3").to(1)
                .build();
        values = StructToMapConverter.convert(struct3);
        Assert.assertTrue(Filter.filter(Filter.parse(filter3), schema, MElement.of(struct3, Instant.now())));
        Assert.assertTrue(Filter.filter(Filter.parse(filter3), values));

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.DATE)
                    .withField("field2", Schema.FieldType.DATE)
                    .build();

            final String filterString =
                    "{ \"expression\": \"field1 - field2\", \"op\": \">\", \"value\": 0 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Date.parseDate("2023-12-31"))
                    .set("field2").to(com.google.cloud.Date.parseDate("2022-12-31"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));

            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            final String filterString =
                    "{ \"expression\": \"timestamp_diff_hour(field1, field2)\", \"op\": \"<\", \"value\": 24 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T00:00:00.000Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            final String filterString =
                    "{ \"expression\": \"timestamp_diff_day(field1, field2)\", \"op\": \"=\", \"value\": 365 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.998Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.INT64.withNullable(true))
                    .build();

            final String filterString =
                    "{ \"key\": \"field1\", \"op\": \"=\", \"value\": null }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to((Long) null)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(-10D)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.INT64.withNullable(true))
                    .build();

            final String filterString =
                    "{ \"key\": \"field1\", \"op\": \"!=\", \"value\": null }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to((Long) null)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertFalse(Filter.filter(Filter.parse(filter), values));

            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.FLOAT64.withNullable(true))
                    .build();

            struct = Struct.newBuilder()
                    .set("field1").to(10D)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.STRING.withNullable(true))
                    .build();

            final String filterString = """
                    [
                      { "key": "field1", "op": "match", "value": "/2024-11-(0[1-9]|[12][0-9]|3[01])/" },
                      { "key": "field1", "op": "match", "value": "\\\\.pdf$" }
                    ]
                    """;


            final JsonElement filter = new Gson().fromJson(filterString, JsonElement.class);
            struct = Struct.newBuilder()
                    .set("field1").to("gs://mybucket/2024-11-11/myfile.pdf")
                    .build();
            values = StructToMapConverter.convert(struct);
            System.out.println(Filter.parse(filter));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assert.assertTrue(Filter.filter(Filter.parse(filter), values));
        }

    }

}