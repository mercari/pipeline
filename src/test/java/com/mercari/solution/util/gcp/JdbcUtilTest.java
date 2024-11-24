package com.mercari.solution.util.gcp;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.sql.stmt.PreparedStatementTemplate;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JdbcUtilTest {

    private static int[] toIntArray(List<Integer> list) {
        return list.stream().mapToInt(x->x).toArray();
    }

    @Test
    public void testCreateSeekConditions() {

        // Test start conditions
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        final List<String> conditions1 = JdbcUtil.createSeekConditions(startOffsets1, true,true);
        final String result1 = "(" + String.join(" OR ", conditions1) + ")";
        Assert.assertEquals("(intField1 > ?)", result1);

        final List<JdbcUtil.IndexOffset> startOffsets2 = new ArrayList<>();
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        final List<String> conditions2 = JdbcUtil.createSeekConditions(startOffsets2, true, true);
        final String result2 = "(" + String.join(" OR ", conditions2) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 > ?) OR intField1 > ?)", result2);

        final List<JdbcUtil.IndexOffset> startOffsets3 = new ArrayList<>();
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));
        final List<String> conditions3 = JdbcUtil.createSeekConditions(startOffsets3, true, true);
        final String result3 = "(" + String.join(" OR ", conditions3) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 = ? AND intField3 > ?) OR (intField1 = ? AND intField2 > ?) OR intField1 > ?)", result3);

        // Test stop conditions
        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        final List<String> stopConditions1 = JdbcUtil.createSeekConditions(stopOffsets1, false, false);
        final String stopResult1 = "(" + String.join(" OR ", stopConditions1) + ")";
        Assert.assertEquals("(intField1 <= ?)", stopResult1);

        final List<JdbcUtil.IndexOffset> stopOffsets2 = new ArrayList<>();
        stopOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        stopOffsets2.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        final List<String> stopConditions2 = JdbcUtil.createSeekConditions(stopOffsets2, false, false);
        final String stopResult2 = "(" + String.join(" OR ", stopConditions2) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 <= ?) OR intField1 <= ?)", stopResult2);
    }

    @Test
    public void testCreateSeekPreparedQuery() {

        final String table = "mytable";
        final String fields = "intField1,intField2,intField3";

        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 7));

        final List<String> parameterFields1 = Arrays.asList("intField1", "intField2", "intField3");
        final String preparedQuery1 = JdbcUtil.createSeekPreparedQuery(JdbcUtil.IndexPosition.of(startOffsets1, true), JdbcUtil.IndexPosition.of(stopOffsets1, false), fields, table, parameterFields1, 1000);
        Assert.assertEquals("SELECT intField1,intField2,intField3 FROM mytable WHERE ((intField1 = ? AND intField2 = ? AND intField3 > ?) OR (intField1 = ? AND intField2 > ?) OR intField1 > ?) AND (intField1 <= ?) ORDER BY intField1, intField2, intField3 LIMIT 1000", preparedQuery1);

        //
        final List<JdbcUtil.IndexOffset> startOffsets2 = new ArrayList<>();
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, null));

        final List<JdbcUtil.IndexOffset> stopOffsets2 = new ArrayList<>();
        stopOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 7));

        final List<String> parameterFields2 = Arrays.asList("intField1", "intField2");
        final String preparedQuery2 = JdbcUtil.createSeekPreparedQuery(JdbcUtil.IndexPosition.of(startOffsets2, true), JdbcUtil.IndexPosition.of(stopOffsets2, false), fields, table, parameterFields2, 1000);
        Assert.assertEquals("SELECT intField1,intField2,intField3 FROM mytable WHERE ((intField1 = ? AND intField2 IS NOT NULL) OR intField1 > ?) AND (intField1 <= ?) ORDER BY intField1, intField2 LIMIT 1000", preparedQuery2);
    }

    @Test
    public void testSplitIndexRange() {
        // Split Integer
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 1));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 3));
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 70));

        final JdbcUtil.IndexPosition startPosition1 = JdbcUtil.IndexPosition.of(startOffsets1, true);
        final JdbcUtil.IndexPosition stopPosition1 = JdbcUtil.IndexPosition.of(stopOffsets1, false);
        final JdbcUtil.IndexRange originalRange = JdbcUtil.IndexRange.of(startPosition1, stopPosition1);
        final List<JdbcUtil.IndexRange> splittedRanges1 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange.getFrom().getOffsets(),
                        originalRange.getTo().getOffsets(),
                        4);

        Assert.assertEquals(2, splittedRanges1.size());
        Assert.assertEquals(1, (int)splittedRanges1.get(0).getFrom().getOffsets().get(0).getIntValue());
        Assert.assertEquals(2, (int)splittedRanges1.get(0).getTo().getOffsets().get(0).getIntValue());
        Assert.assertEquals(2, (int)splittedRanges1.get(1).getFrom().getOffsets().get(0).getIntValue());
        Assert.assertEquals(3, (int)splittedRanges1.get(1).getTo().getOffsets().get(0).getIntValue());
    }

    @Test
    public void testSplitIndexRangeString() {
        // isCaseSensitive
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "", true));
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "add", true));
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField3", Schema.Type.STRING, true, "aff", true));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "znbff", true));
        stopOffsets1.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "zdf", true));

        final JdbcUtil.IndexPosition startPosition1 = JdbcUtil.IndexPosition.of(startOffsets1, true);
        final JdbcUtil.IndexPosition stopPosition1 = JdbcUtil.IndexPosition.of(stopOffsets1, false);
        final JdbcUtil.IndexRange originalRange1 = JdbcUtil.IndexRange.of(startPosition1, stopPosition1);
        final List<JdbcUtil.IndexRange> splittedRanges1 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange1.getFrom().getOffsets(),
                        originalRange1.getTo().getOffsets(),
                        4);

        Assert.assertEquals(4, splittedRanges1.size());
        Assert.assertEquals(3, splittedRanges1.get(0).getFrom().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(1).getFrom().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(2).getFrom().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(3).getFrom().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(0).getTo().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(1).getTo().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(2).getTo().getOffsets().size());
        Assert.assertEquals(1, splittedRanges1.get(3).getTo().getOffsets().size());
        Assert.assertEquals("", splittedRanges1.get(0).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("add", splittedRanges1.get(0).getFrom().getOffsets().get(1).getStringValue());
        Assert.assertEquals("aff", splittedRanges1.get(0).getFrom().getOffsets().get(2).getStringValue());
        Assert.assertEquals("7", splittedRanges1.get(1).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("N", splittedRanges1.get(1).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("N", splittedRanges1.get(2).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("d", splittedRanges1.get(2).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("d", splittedRanges1.get(3).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("znbff", splittedRanges1.get(3).getTo().getOffsets().get(0).getStringValue());

        // isCaseInsensitive alphabet
        final List<JdbcUtil.IndexOffset> startOffsets2 = new ArrayList<>();
        startOffsets2.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "A", false));
        startOffsets2.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "add", false));

        final List<JdbcUtil.IndexOffset> stopOffsets2 = new ArrayList<>();
        stopOffsets2.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "d"));

        final JdbcUtil.IndexPosition startPosition2 = JdbcUtil.IndexPosition.of(startOffsets2, true);
        final JdbcUtil.IndexPosition stopPosition2 = JdbcUtil.IndexPosition.of(stopOffsets2, false);
        final JdbcUtil.IndexRange originalRange2 = JdbcUtil.IndexRange.of(startPosition2, stopPosition2);
        final List<JdbcUtil.IndexRange> splittedRanges2 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange2.getFrom().getOffsets(),
                        originalRange2.getTo().getOffsets(),
                        4);

        Assert.assertEquals(3, splittedRanges2.size());
        Assert.assertEquals("A", splittedRanges2.get(0).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("B", splittedRanges2.get(0).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("B", splittedRanges2.get(1).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("C", splittedRanges2.get(1).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("C", splittedRanges2.get(2).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("d", splittedRanges2.get(2).getTo().getOffsets().get(0).getStringValue());

        // isCaseInsensitive
        final List<JdbcUtil.IndexOffset> startOffsets3 = new ArrayList<>();
        startOffsets3.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "y", false));
        startOffsets3.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "xyz", false));

        final List<JdbcUtil.IndexOffset> stopOffsets3 = new ArrayList<>();
        stopOffsets3.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "}"));

        final JdbcUtil.IndexPosition startPosition3 = JdbcUtil.IndexPosition.of(startOffsets3, true);
        final JdbcUtil.IndexPosition stopPosition3 = JdbcUtil.IndexPosition.of(stopOffsets3, false);
        final JdbcUtil.IndexRange originalRange3 = JdbcUtil.IndexRange.of(startPosition3, stopPosition3);
        final List<JdbcUtil.IndexRange> splittedRanges3 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange3.getFrom().getOffsets(),
                        originalRange3.getTo().getOffsets(),
                        4);

        Assert.assertEquals(4, splittedRanges3.size());
        Assert.assertEquals("y", splittedRanges3.get(0).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("\\", splittedRanges3.get(0).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("\\", splittedRanges3.get(1).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("^", splittedRanges3.get(1).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("^", splittedRanges3.get(2).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("{", splittedRanges3.get(2).getTo().getOffsets().get(0).getStringValue());
        Assert.assertEquals("{", splittedRanges3.get(3).getFrom().getOffsets().get(0).getStringValue());
        Assert.assertEquals("}", splittedRanges3.get(3).getTo().getOffsets().get(0).getStringValue());
    }

    @Test
    public void testIsOverTo() throws Exception {
        byte[] start = Hex.decodeHex("28dd2d8a0de3c97e23982560e0d2b9b00408ae699c90bb2a2ab552286f6aff9f6a2b15951b0df34dffc2935c10030b47f39c16d05db5eee4280ac604aebb724f".toCharArray());
        byte[] end = Hex.decodeHex("85".toCharArray());
        JdbcUtil.IndexPosition startPosition = JdbcUtil.IndexPosition.of(Arrays.asList(JdbcUtil.IndexOffset.of("f1", Schema.Type.BYTES, true, ByteBuffer.wrap(start))), true);
        JdbcUtil.IndexPosition stopPosition = JdbcUtil.IndexPosition.of(Arrays.asList(JdbcUtil.IndexOffset.of("f1", Schema.Type.BYTES, true, ByteBuffer.wrap(end))), false);
        Assert.assertFalse(startPosition.isOverTo(stopPosition));
    }

    @Test
    public void testCreateMySQLStatementInsert() {
        Schema schema = SchemaBuilder.builder()
            .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
            .endRecord();

        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT, JdbcUtil.DB.MYSQL, null);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateMySQLStatementInsertOrUpdate() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();
        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_UPDATE, JdbcUtil.DB.MYSQL, keyFields);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?)" +
                " ON DUPLICATE KEY UPDATE " +
                "`name` = VALUES(`name`)," +
                "`age` = VALUES(`age`)," +
                "`created_at` = VALUES(`created_at`)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateMySQLStatementInsertOrDoNothing() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();
        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_DONOTHING, JdbcUtil.DB.MYSQL, keyFields);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?)" +
                " ON DUPLICATE KEY UPDATE " +
                "`id` = VALUES(`id`)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreatePostgreSQLStatementInsert() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT, JdbcUtil.DB.POSTGRESQL, null);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?::timestamp)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreatePostgreSQLStatementInsertOrUpdate() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();
        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_UPDATE, JdbcUtil.DB.POSTGRESQL, keyFields);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "MERGE INTO people " +
                "USING (VALUES (?,?,?,?::timestamp)) AS item (id,name,age,created_at) ON item.id = people.id" +
                " WHEN MATCHED THEN" +
                " UPDATE SET " +
                "name = item.name," +
                "age = item.age," +
                "created_at = item.created_at" +
                " WHEN NOT MATCHED THEN" +
                " INSERT (id,name,age,created_at)" +
                " VALUES (item.id,item.name,item.age,item.created_at)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreatePostgreSQLStatementInsertOrDoNothing() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();
        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_DONOTHING, JdbcUtil.DB.POSTGRESQL, keyFields);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "MERGE INTO people " +
                "USING (VALUES (?,?,?,?::timestamp)) AS item (id,name,age,created_at) ON item.id = people.id" +
                " WHEN MATCHED THEN" +
                " DO NOTHING" +
                " WHEN NOT MATCHED THEN" +
                " INSERT (id,name,age,created_at)" +
                " VALUES (item.id,item.name,item.age,item.created_at)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateSQLServerStatementInsert() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT, JdbcUtil.DB.SQLSERVER, null);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateSQLServerStatementInsertOrUpdate() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();

        Assert.assertThrows("SQLServer does not support INSERT_OR_UPDATE.", IllegalArgumentException.class, () -> {
           JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_UPDATE, JdbcUtil.DB.SQLSERVER, keyFields);
        });
    }

    @Test
    public void testCreateSQLServerStatementInsertOrDoNothing() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();

        Assert.assertThrows("SQLServer does not support INSERT_OR_DONOTHING.", IllegalArgumentException.class, () -> {
            JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_DONOTHING, JdbcUtil.DB.SQLSERVER, keyFields);
        });
    }

    @Test
    public void testCreateH2StatementInsert() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT, JdbcUtil.DB.H2, null);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "INSERT INTO people (id,name,age,created_at)" +
                " VALUES (?,?,?,?)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateH2StatementInsertOrUpdate() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();
        PreparedStatementTemplate template = JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_UPDATE, JdbcUtil.DB.H2, keyFields);
        List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        String expectedStatement =
                "MERGE INTO people (id,name,age,created_at) KEY (id)" +
                " VALUES (?,?,?,?)";

        Assert.assertEquals(expectedStatement, template.getStatementString());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testCreateH2StatementInsertOrDoNothing() {
        Schema schema = SchemaBuilder.builder()
                .record("root").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredInt("age")
                .name("created_at").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();
        List<String> keyFields = Arrays.stream(new String[]{"id"}).toList();

        Assert.assertThrows("H2 does not support INSERT_OR_DONOTHING.", IllegalArgumentException.class, () -> {
            JdbcUtil.createStatement("people", schema, JdbcUtil.OP.INSERT_OR_DONOTHING, JdbcUtil.DB.H2, keyFields);
        });
    }
}
