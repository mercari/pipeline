package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

public class CsvToAvroConverter {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToAvroConverter.class);

    public static GenericRecord convert(final Schema schema, final String text) {
        try(final CSVParser parser = CSVParser.parse(text, CSVFormat.DEFAULT)) {
            final List<CSVRecord> records = parser.getRecords();
            if(records.size() != 1) {
                return null;
            }
            final CSVRecord record = records.get(0);

            final List<Schema.Field> fields = schema.getFields();
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for(int i=0; i<fields.size(); i++) {
                if(i >= record.size()) {
                    builder.set(fields.get(i), null);
                    continue;
                }
                builder.set(fields.get(i), convertValue(fields.get(i).schema(), record.get(i)));
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convertValue(final Schema schema, final String value) {
        if(value == null) {
            return null;
        }
        try {
            switch (schema.getType()) {
                case BOOLEAN:
                    return Boolean.valueOf(value.trim());
                case ENUM:
                case STRING:
                    return value;
                case FIXED:
                case BYTES:
                    if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                        final LogicalTypes.Decimal decimal = AvroSchemaUtil.getLogicalTypeDecimal(schema);
                        return new BigDecimal(value, new MathContext(decimal.getPrecision()));
                    }
                    return ByteBuffer.wrap(value.getBytes());
                case INT: {
                    if (LogicalTypes.date().equals(schema.getLogicalType())) {
                        final LocalDate localDate = DateTimeUtil.toLocalDate(value);
                        return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                    } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value);
                        return DateTimeUtil.toMilliOfDay(localTime);
                    }
                    return Integer.valueOf(value);
                }
                case LONG: {
                    final String pattern = schema.getProp("patternTimestamp");
                    if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                        if(pattern != null) {
                            return Instant.parse(value.trim(), DateTimeFormat.forPattern(pattern)).getMillis();
                        } else {
                            return Instant.parse(value.trim()).getMillis();
                        }
                    } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                        if(pattern != null) {
                            return Instant.parse(value.trim(), DateTimeFormat.forPattern(pattern)).getMillis() * 1000;
                        } else {
                            return Instant.parse(value.trim()).getMillis() * 1000;
                        }
                    } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value);
                        return DateTimeUtil.toMicroOfDay(localTime);
                    }
                    return Long.valueOf(value);
                }
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                case NULL:
                    return null;
                case UNION:
                    final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                    return convertValue(childSchema, value);
                case MAP:
                case RECORD:
                case ARRAY:
                default:
                    throw new IllegalArgumentException("CSV can not handle data type: " + schema.getType().name());
            }
        } catch (Exception e) {
            LOG.warn("Failed to csv parse value: " + value + ", cause: " + e.getMessage());
            return null;
        }
    }

}
