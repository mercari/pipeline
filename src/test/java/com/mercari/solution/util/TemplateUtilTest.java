package com.mercari.solution.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TemplateUtilTest {

    @Test
    public void testUtilsDatetime() {
        final Long currentTimestampMicros = DateTimeUtil.toEpochMicroSecond(Instant.now());
        final Map<String, Object> values = new HashMap<>();

        final String text1 = "${utils.datetime.currentTimestamp()}";
        final String output1 = TemplateUtil.executeStrictTemplate(text1, values);
        Assert.assertTrue( DateTimeUtil.toEpochMicroSecond(output1) - currentTimestampMicros < 10_000_000);

        final String text2 = "${utils.datetime.currentTimestamp(1)}";
        final String output2 = TemplateUtil.executeStrictTemplate(text2, values);
        Assert.assertTrue( DateTimeUtil.toEpochMicroSecond(output2) - currentTimestampMicros < 10_000_000 + 1_000_000);

        final String text3 = "${utils.datetime.currentTimestamp(1, 'DAYS')}";
        final String output3 = TemplateUtil.executeStrictTemplate(text3, values);
        Assert.assertTrue( DateTimeUtil.toEpochMicroSecond(output3) - currentTimestampMicros < 10_000_000 + 1_000_000L * 60 * 60 * 24);

        final String text4 = "${utils.datetime.currentTimestamp(1, 'DAYS', 'HOURS')}";
        final String output4 = TemplateUtil.executeStrictTemplate(text4, values);
    }

}
