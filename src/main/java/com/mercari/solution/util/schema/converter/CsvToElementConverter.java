package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvToElementConverter {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToElementConverter.class);

    public static Map<String, Object> convert(final Schema schema, final String text) {
        try(final CSVParser parser = CSVParser.parse(text, CSVFormat.DEFAULT)) {
            final List<CSVRecord> records = parser.getRecords();
            if(records.size() != 1) {
                return null;
            }
            final CSVRecord record = records.get(0);

            final List<Schema.Field> fields = schema.getFields();
            final Map<String, Object> values = new HashMap<>();
            for(int i=0; i<fields.size(); i++) {
                if(i >= record.size()) {
                    values.put(fields.get(i).getName(), null);
                    continue;
                }
                values.put(fields.get(i).getName(), convertValue(fields.get(i).getFieldType(), record.get(i)));
            }
            return values;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convertValue(final Schema.FieldType fieldType, final String str) {
        if(str == null) {
            return null;
        }
        try {
            return switch (fieldType.getType()) {
                case bool -> Boolean.valueOf(str.trim());
                case string, json, enumeration -> str;
                case bytes -> ByteBuffer.wrap(Base64.getDecoder().decode(str));
                case int32 -> Integer.parseInt(str);
                case int64 -> Long.parseLong(str);
                case float32 -> Float.parseFloat(str);
                case float64 -> Double.parseDouble(str);
                case time -> DateTimeUtil.toMicroOfDay(str);
                case date -> DateTimeUtil.toEpochDay(str);
                case timestamp -> DateTimeUtil.toEpochMicroSecond(str);
                default -> throw new IllegalArgumentException("CSV can not handle data type: " + fieldType);
            };
        } catch (Exception e) {
            LOG.warn("Failed to csv parse value: " + str + ", cause: " + e.getMessage());
            return null;
        }
    }

}
