package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowToElementConverter {

    public static Schema convertSchema(final org.apache.beam.sdk.schemas.Schema schema) {
        final List<Schema.Field> fields = convertFields(schema.getFields());
        return Schema.builder()
                .withFields(fields)
                .build();
    }

    public static List<Schema.Field> convertFields(final List<org.apache.beam.sdk.schemas.Schema.Field> rowFields) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final org.apache.beam.sdk.schemas.Schema.Field rowField : rowFields) {
            final Schema.FieldType type = convertFieldType(rowField.getType());
            final Map<String, String> options = convertOptions(rowField);
            final Schema.Field field = Schema.Field
                    .of(rowField.getName(), type)
                    .withOptions(options)
                    .withDescription(rowField.getDescription());
            fields.add(field);
        }
        return fields;
    }

    public static Schema.FieldType convertFieldType(final org.apache.beam.sdk.schemas.Schema.FieldType fieldType) {
        final Schema.FieldType result = switch (fieldType.getTypeName()) {
            case BOOLEAN -> Schema.FieldType.type(Schema.Type.bool);
            case STRING -> Schema.FieldType.type(Schema.Type.string);
            case BYTES -> Schema.FieldType.type(Schema.Type.bytes);
            case BYTE -> Schema.FieldType.type(Schema.Type.int8);
            case INT16 -> Schema.FieldType.type(Schema.Type.int16);
            case INT32 -> Schema.FieldType.type(Schema.Type.int32);
            case INT64 -> Schema.FieldType.type(Schema.Type.int64);
            case FLOAT -> Schema.FieldType.type(Schema.Type.float32);
            case DOUBLE -> Schema.FieldType.type(Schema.Type.float64);
            case DECIMAL -> Schema.FieldType.decimal(38, 9);
            case DATETIME -> Schema.FieldType.type(Schema.Type.timestamp);
            case LOGICAL_TYPE -> {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield Schema.FieldType.type(Schema.Type.date);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield Schema.FieldType.type(Schema.Type.time);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    yield Schema.FieldType.enumeration(fieldType.getLogicalType(EnumerationType.class).getValues());
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case MAP -> Schema.FieldType.map(convertFieldType(fieldType.getMapValueType()));
            case ROW -> Schema.FieldType.element(convertFields(fieldType.getRowSchema().getFields()));
            case ARRAY, ITERABLE -> Schema.FieldType.array(convertFieldType(fieldType.getCollectionElementType()));
        };

        return result.withNullable(fieldType.getNullable());
    }

    private static Map<String, String> convertOptions(final org.apache.beam.sdk.schemas.Schema.Field rowField) {
        final Map<String, String> options = new HashMap<>();
        final org.apache.beam.sdk.schemas.Schema.Options rowOptions = rowField.getOptions();
        if(rowOptions != null) {
            for(final String optionName : rowOptions.getOptionNames()) {
                final Object optionObjectValue = rowOptions.getValue(optionName);
                final String optionValue = switch (optionObjectValue) {
                    case String s -> s;
                    case Boolean b -> b.toString();
                    case Number i -> i.toString();
                    case null -> null;
                    default -> throw new IllegalArgumentException();
                };
                options.put(optionName, optionValue);
            }
        }
        return options;
    }

}
