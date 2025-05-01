package com.mercari.solution.util.schema.converter;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ElementToRowConverter {

    public static org.apache.beam.sdk.schemas.Schema convertSchema(final List<Schema.Field> fields) {
        if(fields == null || fields.isEmpty()) {
            return null;
        }
        final org.apache.beam.sdk.schemas.Schema.Builder builder = org.apache.beam.sdk.schemas.Schema.builder();
        for(final Schema.Field field : fields) {
            final List<org.apache.beam.sdk.schemas.Schema.Options> optionsList = new ArrayList<>();
            final String fieldName;
            if(field.getAlterName() == null) {
                fieldName = field.getName();
            } else {
                fieldName = field.getAlterName();
                optionsList.add(org.apache.beam.sdk.schemas.Schema.Options.builder()
                        .setOption(Schema.OPTION_ORIGINAL_FIELD_NAME, org.apache.beam.sdk.schemas.Schema.FieldType.STRING, field.getName())
                        .build());
            }
            if(field.getOptions() != null) {
                for(final Map.Entry<String, String> entry : field.getOptions().entrySet()) {
                    optionsList.add(org.apache.beam.sdk.schemas.Schema.Options.builder()
                            .setOption(entry.getKey(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING, entry.getValue())
                            .build());
                }
            }
            if(Schema.Type.json.equals(field.getFieldType().getType())) {
                optionsList.add(org.apache.beam.sdk.schemas.Schema.Options.builder()
                        .setOption("sqlType", org.apache.beam.sdk.schemas.Schema.FieldType.STRING, "JSON")
                        .build());
            }
            if(field.getFieldType().getDefaultValue() != null) {
                optionsList.add(RowSchemaUtil.createDefaultValueOptions(field.getFieldType().getDefaultValue()));
            }

            final org.apache.beam.sdk.schemas.Schema.FieldType rowFieldType = convertFieldType(field.getFieldType());
            org.apache.beam.sdk.schemas.Schema.Field rowField = org.apache.beam.sdk.schemas.Schema.Field.of(fieldName, rowFieldType);
            for(final org.apache.beam.sdk.schemas.Schema.Options fieldOptions : optionsList) {
                rowField = rowField.withOptions(fieldOptions);
            }
            if(field.getDescription() != null) {
                rowField = rowField.withDescription(field.getDescription());
            }
            builder.addField(rowField);
        }
        return builder.build();
    }

    public static Row convert(final Schema schema, final MElement element) {
        final org.apache.beam.sdk.schemas.Schema rowSchema = schema.getRowSchema();
        final Map<String, Object> primitives = element.asPrimitiveMap();
        return RowSchemaUtil.convertPrimitives(rowSchema, primitives);
    }

    public static Row convert(final org.apache.beam.sdk.schemas.Schema schema, final Map<String, Object> primitiveValues) {
        return RowSchemaUtil.convertPrimitives(schema, primitiveValues);
    }

    public static org.apache.beam.sdk.schemas.Schema.FieldType convertFieldType(final Schema.FieldType fieldType) {
        final org.apache.beam.sdk.schemas.Schema.FieldType rowFieldType = switch (fieldType.getType()) {
            case bytes -> org.apache.beam.sdk.schemas.Schema.FieldType.BYTES;
            case string, json -> org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
            case int8 -> org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
            case int16 -> org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
            case int32 -> org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
            case int64 -> org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
            case float32 -> org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
            case float64 -> org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
            case decimal -> org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL;
            case bool -> org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
            case time -> CalciteUtils.TIME;
            case date -> CalciteUtils.DATE;
            case datetime -> org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(SqlTypes.DATETIME);
            case timestamp -> org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
            case enumeration -> {
                if(fieldType.getSymbols() == null || fieldType.getSymbols().isEmpty()) {
                    throw new IllegalArgumentException("enum type field requires symbols attribute");
                }
                yield org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(EnumerationType.create(fieldType.getSymbols()));
            }
            case map -> {
                if(fieldType.getMapValueType() != null) {
                    yield org.apache.beam.sdk.schemas.Schema.FieldType.map(
                            org.apache.beam.sdk.schemas.Schema.FieldType.STRING,
                            convertFieldType(fieldType.getMapValueType()));
                } else {
                    final Schema.Field keyField = fieldType.getElementSchema().getFields().stream()
                            .filter(f -> "key".equals(f.getName()))
                            .findAny().orElse(Schema.Field.of("key", Schema.FieldType.STRING));
                    final Schema.Field valueField = fieldType.getElementSchema().getFields().stream()
                            .filter(f -> "value".equals(f.getName()))
                            .findAny().orElseThrow(() -> new IllegalArgumentException("Map schema must contain value field."));
                    yield org.apache.beam.sdk.schemas.Schema.FieldType.map(
                            convertFieldType(keyField.getFieldType()),
                            convertFieldType(valueField.getFieldType()));
                }
            }
            case element -> org.apache.beam.sdk.schemas.Schema.FieldType.row(convertSchema(fieldType.getElementSchema().getFields()));
            case array -> org.apache.beam.sdk.schemas.Schema.FieldType.array(convertFieldType(fieldType.getArrayValueType()));
            default -> throw new IllegalArgumentException("Not supported fieldType: " + fieldType + " for schema conversion from row to element");
        };

        return rowFieldType.withNullable(fieldType.getNullable());
    }

}
