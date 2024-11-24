package com.mercari.solution.util.schema.converter;

import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RowToCsvConverter {

    private RowToCsvConverter() {}

    public static String convert(final Row row, final List<String> fields) {
        final List<?> values = fields
                .stream()
                .map(row::getValue)
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
