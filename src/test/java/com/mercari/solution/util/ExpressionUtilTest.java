package com.mercari.solution.util;

import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.*;

public class ExpressionUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testEstimateVariables() {

        final Random random = new Random();

        final String expressionText1 = "(a - b) >= 1.5 * ((a - b_1) + (a_2 - b_2) + if(x > y, zz_aa_1, 0)) / 5";
        final Set<String> variables1 = ExpressionUtil.estimateVariables(expressionText1);

        Assert.assertEquals(8, variables1.size());
        Assert.assertTrue(variables1.containsAll(Arrays.asList("a","b","a_2","b_1","b_2","x","y","zz_aa_1")));

        final Map<String,Integer> bufferSizes1 = ExpressionUtil.extractBufferSizes(variables1, "_");
        Assert.assertEquals(5, bufferSizes1.size());
        Assert.assertTrue(bufferSizes1.keySet().containsAll(Arrays.asList("a","b","x","y","zz_aa")));
        Assert.assertEquals(2, bufferSizes1.get("a").intValue());
        Assert.assertEquals(2, bufferSizes1.get("b").intValue());
        Assert.assertEquals(0, bufferSizes1.get("x").intValue());
        Assert.assertEquals(0, bufferSizes1.get("y").intValue());
        Assert.assertEquals(1, bufferSizes1.get("zz_aa").intValue());

        final Map<String,Double> values1 = new HashMap<>();
        for(final String variable : variables1) {
            values1.put(variable, random.nextDouble());
        }

        final Expression expression1 = ExpressionUtil.createDefaultExpression(expressionText1, variables1);
        final double result1 = expression1.setVariables(values1).evaluate();
        Assert.assertTrue(Arrays.asList(0D, 1D).contains(result1));

        // only number formula
        final String expressionText2 = "(120 - 12) / 4.5";
        final Set<String> variables2 = ExpressionUtil.estimateVariables(expressionText2);
        Assert.assertEquals(0, variables2.size());

        final Map<String,Integer> bufferSizes2 = ExpressionUtil.extractBufferSizes(variables2, "_");
        Assert.assertEquals(0, bufferSizes2.size());

        final Expression expression2 = ExpressionUtil.createDefaultExpression(expressionText2, variables2);
        final Map<String,Double> values2 = new HashMap<>();
        final double result2 = expression2.setVariables(values2).evaluate();
        Assert.assertEquals((120 - 12) / 4.5, result2, DELTA);

    }

    @Test
    public void testTimestampToDate() {

        {
            final String expressionText = "timestamp_to_date(a, b)";
            final Set<String> variables = ExpressionUtil.estimateVariables(expressionText);
            Assert.assertEquals(2, variables.size());
            Assert.assertTrue(variables.containsAll(Arrays.asList("a","b")));

            final Map<String,Double> values = new HashMap<>();

            Instant a = Instant.parse("2023-01-15T14:59:59.999Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            Expression expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            double result = expression.setVariables(values).evaluate();
            Assert.assertEquals(LocalDate.of(2023,1,15).toEpochDay(), result, DELTA);

            a = Instant.parse("2023-01-15T15:00:00.000Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            result = expression.setVariables(values).evaluate();
            Assert.assertEquals(LocalDate.of(2023,1,16).toEpochDay(), result, DELTA);
        }

        {
            final String expressionText = "timestamp_to_date(a, b) - timestamp_to_date(c, d)";
            final Set<String> variables = ExpressionUtil.estimateVariables(expressionText);
            Assert.assertEquals(4, variables.size());
            Assert.assertTrue(variables.containsAll(Arrays.asList("a","b","c","d")));

            final Map<String,Double> values = new HashMap<>();

            Instant a = Instant.parse("2023-01-15T15:00:00.000Z");
            Instant b = Instant.parse("2023-01-14T14:59:59.999Z");
            values.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
            values.put("b", 9D);
            values.put("c", Long.valueOf(b.getMillis() * 1000L).doubleValue());
            values.put("d", 9D);
            final Expression expression = ExpressionUtil.createDefaultExpression(expressionText, variables);
            final double result = expression.setVariables(values).evaluate();
            Assert.assertEquals(2D, result, DELTA);
        }
    }

    @Test
    public void testTimestampDiff() {
        // millisecond
        final String expressionText1 = "timestamp_diff_millisecond(a,b)";
        final Set<String> variables1 = ExpressionUtil.estimateVariables(expressionText1);
        Assert.assertEquals(2, variables1.size());
        Assert.assertTrue(variables1.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values1 = new HashMap<>();
        Instant a = Instant.parse("2023-01-15T00:00:00.000Z");
        Instant b = Instant.parse("2023-01-17T12:32:12.543Z");
        values1.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values1.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression1 = ExpressionUtil.createDefaultExpression(expressionText1, variables1);
        final double result1 = expression1.setVariables(values1).evaluate();
        Assert.assertEquals((a.getMillis() - b.getMillis()), result1, DELTA);

        // second
        final String expressionText2 = "timestamp_diff_second(a,b)";
        final Set<String> variables2 = ExpressionUtil.estimateVariables(expressionText2);
        Assert.assertEquals(2, variables2.size());
        Assert.assertTrue(variables2.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values2 = new HashMap<>();
        values2.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values2.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression2 = ExpressionUtil.createDefaultExpression(expressionText2, variables2);
        final double result2 = expression2.setVariables(values2).evaluate();
        Assert.assertEquals((a.getMillis() - b.getMillis()) / 1000, result2, DELTA);

        // minute
        final String expressionText3 = "timestamp_diff_minute(a,b)";
        final Set<String> variables3 = ExpressionUtil.estimateVariables(expressionText3);
        Assert.assertEquals(2, variables2.size());
        Assert.assertTrue(variables3.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values3 = new HashMap<>();
        values3.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values3.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression3 = ExpressionUtil.createDefaultExpression(expressionText3, variables3);
        final double result3 = expression3.setVariables(values3).evaluate();
        Assert.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60), result3, DELTA);

        // hour
        final String expressionText4 = "timestamp_diff_hour(a,b)";
        final Set<String> variables4 = ExpressionUtil.estimateVariables(expressionText4);
        Assert.assertEquals(2, variables4.size());
        Assert.assertTrue(variables4.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values4 = new HashMap<>();
        values4.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values4.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression4 = ExpressionUtil.createDefaultExpression(expressionText4, variables4);
        final double result4 = expression4.setVariables(values4).evaluate();
        Assert.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60 * 60), result4, DELTA);

        // day
        final String expressionText5 = "timestamp_diff_day(a,b)";
        final Set<String> variables5 = ExpressionUtil.estimateVariables(expressionText5);
        Assert.assertEquals(2, variables5.size());
        Assert.assertTrue(variables5.containsAll(Arrays.asList("a","b")));
        final Map<String,Double> values5 = new HashMap<>();
        values5.put("a", Long.valueOf(a.getMillis() * 1000L).doubleValue());
        values5.put("b", Long.valueOf(b.getMillis() * 1000L).doubleValue());
        final Expression expression5 = ExpressionUtil.createDefaultExpression(expressionText5, variables5);
        final double result5 = expression5.setVariables(values5).evaluate();
        Assert.assertEquals((a.getMillis() - b.getMillis()) / (1000 * 60 * 60 * 24), result5, DELTA);

    }

}
