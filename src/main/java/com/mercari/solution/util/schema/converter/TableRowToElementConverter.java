package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;

import java.util.ArrayList;
import java.util.List;

public class TableRowToElementConverter {

    public static Schema convertSchema(final TableSchema tableSchema) {
        final List<Schema.Field> fields = convertFields(tableSchema.getFields());
        return Schema.of(fields);
    }

    public static List<Schema.Field> convertFields(final List<TableFieldSchema> tableFields) {
        final List<Schema.Field> fields = new ArrayList<>();
        for(final TableFieldSchema fieldSchema : tableFields) {
            Schema.FieldType fieldType = switch (fieldSchema.getType()) {
                case "BOOLEAN" -> Schema.FieldType.BOOLEAN;
                case "BYTES" -> Schema.FieldType.BYTES;
                case "STRING" -> Schema.FieldType.STRING;
                case "INTEGER" -> {
                    final Object avroSchema = fieldSchema.get("avroSchema");
                    if(avroSchema != null && avroSchema.toString().contains("INT")) {
                        yield Schema.FieldType.INT32;
                    } else {
                        yield Schema.FieldType.INT64;
                    }
                }
                case "FLOAT" -> {
                    final Object avroSchema = fieldSchema.get("avroSchema");
                    if(avroSchema != null && avroSchema.toString().contains("FLOAT")) {
                        yield Schema.FieldType.FLOAT32;
                    } else {
                        yield Schema.FieldType.FLOAT64;
                    }
                }
                case "TIME" -> Schema.FieldType.TIME;
                case "DATE" -> Schema.FieldType.DATE;
                case "TIMESTAMP" -> Schema.FieldType.TIMESTAMP;
                case "RECORD" -> Schema.FieldType.element(convertFields(fieldSchema.getFields()));
                default -> throw new IllegalArgumentException("Not supported fieldType: " + fieldSchema.getType());
            };
            fieldType = switch (fieldSchema.getMode().toUpperCase()) {
                case "REQUIRED" -> fieldType;
                case "NULLABLE" -> fieldType.withNullable(true);
                case "REPEATED" -> Schema.FieldType.array(fieldType).withNullable(true);
                default -> throw new IllegalArgumentException();
            };
            final Schema.Field field = Schema.Field.of(fieldSchema.getName(), fieldType);
            fields.add(field);
        }
        return fields;
    }

    public static MElement convert(final Schema schema, final TableRow tableRow) {
        return null;
    }

}
