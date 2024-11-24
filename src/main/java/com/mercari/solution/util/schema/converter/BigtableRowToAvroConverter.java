package com.mercari.solution.util.schema.converter;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
//import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BigtableRowToAvroConverter {

    private static final String KEY_FIELD_NAME = "__key__";

    private static final Logger LOG = LoggerFactory.getLogger(BigtableRowToAvroConverter.class);

    public static GenericRecord convert(final Schema schema, final Row row) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for row: " + row);
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        final Schema.Field keyField = schema.getField(KEY_FIELD_NAME);
        if(keyField != null) {
            switch (AvroSchemaUtil.unnestUnion(keyField.schema()).getType()) {
                case STRING:
                    builder.set(KEY_FIELD_NAME, row.getKey().toStringUtf8());
                    break;
                case BYTES:
                    builder.set(KEY_FIELD_NAME, ByteBuffer.wrap(row.getKey().toByteArray()));
                    break;
            }
        }
        for(final Family family : row.getFamiliesList()) {
            final String familyName = family.getName();
            final Schema.Field familyField = schema.getField(familyName);
            if(familyField != null) {
                final Schema familySchema = AvroSchemaUtil.unnestUnion(familyField.schema());
                if(!Schema.Type.RECORD.equals(familySchema.getType())) {
                    continue;
                }
                final GenericRecordBuilder familyBuilder = new GenericRecordBuilder(familySchema);
                setBuilder(familySchema, familyBuilder, family.getColumnsList());
                final GenericRecord familyRecord = familyBuilder.build();
                builder.set(familyName, familyRecord);
            } else {
                setBuilder(schema, builder, family.getColumnsList());
            }
        }
        return builder.build();
    }

    private static void setBuilder(final Schema schema, final GenericRecordBuilder builder, final List<Column> columns) {
        for(final Column column : columns) {
            final ByteString qualifier = column.getQualifier();
            final String columnName = qualifier.toStringUtf8();
            final Schema.Field columnField = schema.getField(columnName);
            if(columnField == null) {
                continue;
            }
            final Schema columnSchema = AvroSchemaUtil.unnestUnion(columnField.schema());
            if(Schema.Type.ARRAY.equals(columnSchema.getType())) {
                final List<Object> list = new ArrayList<>();
                for(final Cell cell : column.getCellsList()) {
                    final Object value = getBytesValue(columnSchema, cell.getValue());
                    list.add(value);
                }
                builder.set(columnName, list);
            } else {
                if(column.getCellsCount() > 0) {
                    final Object value = getBytesValue(columnSchema, column.getCells(0).getValue());
                    builder.set(columnName, value);
                }
            }
        }
    }

    private static Object getBytesValue(Schema fieldSchema, final ByteString value) {
        return null;
    }

    /*
    private static Object getBytesValue(Schema fieldSchema, final ByteString value) {
        if(value == null || value.isEmpty()) {
            return null;
        }
        switch (fieldSchema.getType()) {
            case UNION:
                return getBytesValue(AvroSchemaUtil.unnestUnion(fieldSchema), value);
            case BOOLEAN:
                return Bytes.toBoolean(value.toByteArray());
            case ENUM:
            case STRING:
                return Bytes.toString(value.toByteArray());
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                    final BigDecimal decimal = Bytes.toBigDecimal(value.toByteArray());
                    final LogicalTypes.Decimal decimalSchema = AvroSchemaUtil.getLogicalTypeDecimal(fieldSchema);
                    final int scale = decimalSchema.getScale();
                    if(decimal.scale() <= scale) {
                        return ByteBuffer.wrap(decimal.scaleByPowerOfTen(scale).toBigInteger().toByteArray());
                    } else {
                        final String newDecimalString = decimal.scaleByPowerOfTen(scale).toPlainString();
                        final BigDecimal newDecimal = new BigDecimal(newDecimalString.substring(0, newDecimalString.length() - (decimal.scale() - scale)));
                        return ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray());
                    }
                } else {
                    return ByteBuffer.wrap(value.toByteArray());
                }
            }
            case INT: {
                try {
                    return Bytes.toInt(value.toByteArray());
                } catch (Throwable e) {
                    final String string = Bytes.toString(value.toByteArray());
                    if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                        return Long.valueOf(LocalDate.parse(string).toEpochDay()).intValue();
                    } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                        return Long.valueOf(LocalTime.parse(string).toNanoOfDay() / 1000_000L).intValue();
                    } else {
                        return Bytes.toInt(value.toByteArray());
                    }
                }
            }
            case LONG: {
                try {
                    return Bytes.toLong(value.toByteArray());
                } catch (Throwable e) {
                    final String string = Bytes.toString(value.toByteArray());
                    if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                        return DateTimeUtil.toEpochMicroSecond(string) / 1000L;
                    } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                        return DateTimeUtil.toEpochMicroSecond(string);
                    } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                        return LocalTime.parse(string).toNanoOfDay() / 1000L;
                    } else {
                        return null;
                    }
                }
            }
            case FLOAT:
                return Bytes.toFloat(value.toByteArray());
            case DOUBLE:
                return Bytes.toDouble(value.toByteArray());
            case ARRAY: {

            }
            case RECORD:
            case MAP:
            case NULL:
            default:
                return null;
        }
    }

     */

}
