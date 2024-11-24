package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElementToCsvConverter {

    public static String convert(final Schema schema, final MElement element, final List<String> fields) {
        if(element == null || element.getValue() == null) {
            return null;
        }
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String, Object>) element.getValue(), fields);
            case AVRO -> AvroToCsvConverter.convert((GenericRecord) element.getValue(), fields);
            case ROW -> RowToCsvConverter.convert((Row) element.getValue(), fields);
            case STRUCT -> StructToCsvConverter.convert((Struct) element.getValue(), fields);
            case DOCUMENT -> DocumentToCsvConverter.convert(((Document) element.getValue()), fields);
            case ENTITY -> EntityToCsvConverter.convert((Entity) element.getValue(), fields);
            default -> throw new IllegalArgumentException();
        };
    }


    public static String convert(final Schema schema, final Map<String, Object> primitiveValues, final List<String> fields) {
        final List<?> values = fields
                .stream()
                .map(f -> MElement.getAsStandardValue(schema.getField(f).getFieldType(), primitiveValues.get(f)))
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
