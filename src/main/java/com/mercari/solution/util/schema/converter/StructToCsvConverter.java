package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StructToCsvConverter {

    public static String convert(final Struct struct, final List<String> fields) {
        final List<?> values = fields
                .stream()
                .map(f -> StructSchemaUtil.getCSVLineValue(struct, f))
                .collect(Collectors.toList());
        final StringBuilder sb = new StringBuilder();
        try(final CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(values);
            printer.flush();
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
