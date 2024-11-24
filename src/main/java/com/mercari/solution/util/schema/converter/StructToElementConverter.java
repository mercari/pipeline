package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Type;
import com.mercari.solution.module.Schema;

import java.util.ArrayList;
import java.util.List;

public class StructToElementConverter {

    public static List<Schema.Field> convertFields(final List<Type.StructField> structFields) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final Type.StructField structField : structFields) {
            Schema.Field field = Schema.Field.of(structField.getName(), convertFieldType(structField.getType()));
            fields.add(field);
        }
        return fields;
    }

    private static Schema.FieldType convertFieldType(final Type type) {
        final Schema.FieldType fieldType = switch (type.getCode()) {
            case BOOL -> Schema.FieldType.BOOLEAN;
            case STRING -> Schema.FieldType.STRING;
            case JSON, PG_JSONB -> Schema.FieldType.JSON;
            case BYTES -> Schema.FieldType.BYTES;
            case INT64 -> Schema.FieldType.INT64;
            case FLOAT32 -> Schema.FieldType.FLOAT32;
            case FLOAT64 -> Schema.FieldType.FLOAT64;
            case NUMERIC, PG_NUMERIC -> Schema.FieldType.DECIMAL;
            case DATE -> Schema.FieldType.DATE;
            case TIMESTAMP -> Schema.FieldType.TIMESTAMP;
            case STRUCT -> Schema.FieldType.element(convertFields(type.getStructFields()));
            case ARRAY -> Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            default -> throw new IllegalArgumentException("Spanner type: " + type + " not supported!");
        };

        return fieldType.withNullable(true);
    }
}
