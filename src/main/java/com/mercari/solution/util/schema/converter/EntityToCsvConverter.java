package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class EntityToCsvConverter {

    public static String convert(final Entity entity, final List<String> fields) {
        final List<?> values = fields
                .stream()
                .map(f -> EntitySchemaUtil.getFieldValue(entity, f))
                .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        try(CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(values);
            printer.flush();
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
