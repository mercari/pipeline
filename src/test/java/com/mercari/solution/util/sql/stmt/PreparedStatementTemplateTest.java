package com.mercari.solution.util.sql.stmt;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PreparedStatementTemplateTest {

    private static int[] toIntArray(List<Integer> list) {
        return list.stream().mapToInt(x->x).toArray();
    }

    @Test
    public void testSerializable() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ").appendPlaceholder(1);

        final PreparedStatementTemplate original = builder.build();
        final PreparedStatementTemplate copied = SerializationUtils.clone(original);

        Assert.assertEquals(original, copied);
    }

    @Test
    public void testPlaceholdersUsedOnce() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?", template.getStatementString());

        Assert.assertEquals(3 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
    }

    @Test
    public void testPlaceholdersUsedMultipleTimes() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(3);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?, ?, ?, ?", template.getStatementString());

        Assert.assertEquals(3 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2, 4}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3, 5, 6}, toIntArray(mappings.get(3)));
    }

    @Test
    public void testPlaceholdersUsedShuffledOrder() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(4);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?, ?", template.getStatementString());

        Assert.assertEquals(4 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }
}