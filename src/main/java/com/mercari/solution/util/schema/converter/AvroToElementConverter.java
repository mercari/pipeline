package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import java.util.ArrayList;
import java.util.List;

public class AvroToElementConverter {

    public static List<Schema.Field> convertFields(final List<org.apache.avro.Schema.Field> avroSchemaFields) {
        return convertFields(avroSchemaFields, true);
    }

    public static List<Schema.Field> convertFields(final List<org.apache.avro.Schema.Field> avroSchemaFields, final boolean nullable) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final org.apache.avro.Schema.Field avroSchemaField : avroSchemaFields) {
            final Schema.FieldType fieldType = convertFieldType(avroSchemaField.schema(), nullable);
            fields.add(Schema.Field
                    .of(avroSchemaField.name(), fieldType)
                    .withDescription(avroSchemaField.doc()));
        }
        return fields;
    }

    private static Schema.FieldType convertFieldType(final org.apache.avro.Schema avroFieldSchema, final boolean nullable) {
        final Schema.FieldType fieldType = switch (avroFieldSchema.getType()) {
            case BOOLEAN -> Schema.FieldType.BOOLEAN;
            case STRING -> Schema.FieldType.STRING;
            case BYTES, FIXED -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(avroFieldSchema)) {
                    yield Schema.FieldType.DECIMAL;
                } else {
                    yield Schema.FieldType.BYTES;
                }
            }
            case FLOAT -> Schema.FieldType.FLOAT32;
            case DOUBLE -> Schema.FieldType.FLOAT64;
            case ENUM -> Schema.FieldType.enumeration(avroFieldSchema.getEnumSymbols());
            case MAP -> Schema.FieldType.map(convertFieldType(avroFieldSchema.getValueType(), nullable));
            case INT -> {
                if (LogicalTypes.date().equals(avroFieldSchema.getLogicalType())) {
                    yield Schema.FieldType.DATE;
                } else if (LogicalTypes.timeMillis().equals(avroFieldSchema.getLogicalType())) {
                    yield Schema.FieldType.TIME;
                } else {
                    yield Schema.FieldType.INT32;
                }
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(avroFieldSchema.getLogicalType())) {
                    yield Schema.FieldType.TIMESTAMP;
                } else if (LogicalTypes.timestampMicros().equals(avroFieldSchema.getLogicalType())) {
                    yield Schema.FieldType.TIMESTAMP;
                } else if (LogicalTypes.timeMicros().equals(avroFieldSchema.getLogicalType())) {
                    yield Schema.FieldType.TIME;
                } else {
                    yield Schema.FieldType.INT64;
                }
            }
            case RECORD -> Schema.FieldType.element(convertFields(avroFieldSchema.getFields(), nullable));
            case ARRAY -> Schema.FieldType.array(convertFieldType(avroFieldSchema.getElementType(), nullable));
            case UNION -> convertFieldType(AvroSchemaUtil.unnestUnion(avroFieldSchema), true);
            default -> throw new IllegalArgumentException();
        };

        return fieldType.withNullable(nullable);
    }
}
