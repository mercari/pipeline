package com.mercari.solution.util.schema.converter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MapToCsvConverter {

    public String line(Object ...values) {
        final StringBuilder sb = new StringBuilder();
        try(final CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(values);
            printer.flush();
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String lines(List<Map<String, Object>> list, String ...names) {
        final StringBuilder sb = new StringBuilder();
        try(final CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            for(final Map<String, Object> values : list) {
                for(final String name : names) {
                    final Object value = getValue(name, values);
                    printer.print(value);
                }
                printer.println();
            }
            printer.flush();
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getValue(final String name, final Map<String, Object> valueAsMap) {
        if(valueAsMap == null) {
            return null;
        }
        if(name.contains(".")) {
            String[] strs = name.split("\\.", 2);
            if(!valueAsMap.containsKey(strs[0])) {
                return null;
            }
            final Object child = valueAsMap.get(strs[0]);
            if(child instanceof Map) {
                return getValue(strs[1], (Map<String, Object>) child);
            } else {
                return null;
            }
        } else {
            return valueAsMap.getOrDefault(name, null);
        }

    }

}
