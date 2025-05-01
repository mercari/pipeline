package com.mercari.solution.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

public class TemplateUtilTest {

    @Test
    public void testDateTimeUtils() {
        final Map<String, Object> values = new HashMap<>();
        TemplateUtil.setFunctions(values);

        // test currentDate
        final String text1 = "${__DateTimeUtils.currentDate('Asia/Tokyo', -20)}";
        final String text2 = "${__DateTimeUtils.currentDate('Asia/Tokyo',  40)}";
        final String output1 = TemplateUtil.executeStrictTemplate(text1, values);
        final String output2 = TemplateUtil.executeStrictTemplate(text2, values);
        final LocalDate localDate1 = LocalDate.parse(output1);
        final LocalDate localDate2 = LocalDate.parse(output2);
        Assert.assertEquals(60L, localDate2.toEpochDay() - localDate1.toEpochDay());

        final String text3 = "${__DateTimeUtils.currentDate('Asia/Tokyo', 0, 'yyyy')}-${__DateTimeUtils.currentDate('Asia/Tokyo', 0, 'MM')}-${__DateTimeUtils.currentDate('Asia/Tokyo', 0, 'dd')}";
        final String text4 = "${__DateTimeUtils.currentDate('Asia/Tokyo', 0, 'yyyy-MM-dd')}";
        final String output3 = TemplateUtil.executeStrictTemplate(text3, values);
        final String output4 = TemplateUtil.executeStrictTemplate(text4, values);
        final LocalDate localDate3 = LocalDate.parse(output3);
        final LocalDate localDate4 = LocalDate.parse(output4);
        Assert.assertEquals(localDate4.toEpochDay(), localDate3.toEpochDay());

        // test currentTime
        final String text5 = "${__DateTimeUtils.currentTime('UTC')}";
        final String text6 = "${__DateTimeUtils.currentTime('Asia/Tokyo')}";
        final String output5 = TemplateUtil.executeStrictTemplate(text5, values);
        final String output6 = TemplateUtil.executeStrictTemplate(text6, values);
        final LocalTime localTime5 = LocalTime.parse(output5);
        final LocalTime localTime6 = LocalTime.parse(output6);
        //Assert.assertEquals(9 * 3600, localTime6.toSecondOfDay() - localTime5.toSecondOfDay());

        final String text7 = "${__DateTimeUtils.currentTime('Asia/Tokyo', -3600)}";
        final String text8 = "${__DateTimeUtils.currentTime('Asia/Tokyo',  3600)}";
        final String output7 = TemplateUtil.executeStrictTemplate(text7, values);
        final String output8 = TemplateUtil.executeStrictTemplate(text8, values);
        final LocalTime localTime7 = LocalTime.parse(output7);
        final LocalTime localTime8 = LocalTime.parse(output8);
        //Assert.assertEquals(7200, localTime8.toSecondOfDay() - localTime7.toSecondOfDay());
    }

}
