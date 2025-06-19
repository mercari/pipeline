package com.mercari.solution.util.sql.calcite.udf;

import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.Multimap;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;

public class DateTimeFunctions {

    public static Multimap<? extends String, ? extends Function> functions() {
        return ImmutableMultimap.<String, Function>builder()
                .put("CURRENT_DATE_", ScalarFunctionImpl.create(Types.lookupMethod(DateTimeFunctions.class, "currentDate")))
                .put("CURRENT_DATE_", ScalarFunctionImpl.create(Types.lookupMethod(DateTimeFunctions.class, "currentDate", String.class)))
                .build();
    }

    public static Date currentDate() {
        return Date.valueOf(LocalDate.now());
    }

    public static Date currentDate(String zoneId) {
        return Date.valueOf(LocalDate.now(ZoneId.of(zoneId)));
    }

}
