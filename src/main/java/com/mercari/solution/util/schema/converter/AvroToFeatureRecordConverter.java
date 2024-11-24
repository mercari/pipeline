package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class AvroToFeatureRecordConverter {

    public static Schema convertSchema(final Schema schema) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Schema.Field field : schema.getFields()) {
            final Map<String, Schema> fieldSchemas = convertSchema(field.name(), field.schema());
            for(final Map.Entry<String, Schema> entry : fieldSchemas.entrySet()) {
                schemaFields.name(entry.getKey()).type(entry.getValue()).noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord convert(final Schema schema, final GenericRecord record) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        final Schema originalSchema = record.getSchema();
        for(final Schema.Field field : originalSchema.getFields()) {
            final Map<String, Object> values = convertValue(field.name(), field.schema(), record.get(field.name()));
            for(final Map.Entry<String, Object> entry : values.entrySet()) {
                try {
                    builder.set(entry.getKey(), entry.getValue());
                } catch (NullPointerException e) {
                    throw new RuntimeException("NULLVALUE: " + entry.getKey() + " : " + entry.getValue(), e);
                }
            }
        }
        return builder.build();
    }

    private static Map<String, Schema> convertSchema(final String name, final Schema schema) {
        final Map<String, Schema> schemas = new HashMap<>();
        switch (schema.getType()) {
            case MAP:
            case INT:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            case LONG:
            case FIXED:
            case ENUM:
            case BYTES:
            case STRING:
            case RECORD:
            case NULL:
            case UNION:
                schemas.put(name, schema);
                break;
            case ARRAY:
                final Schema elementSchema = AvroSchemaUtil.unnestUnion(schema.getElementType());
                if(Schema.Type.RECORD.equals(elementSchema.getType())) {
                    for(final Schema.Field field : elementSchema.getFields()) {
                        final String childName = name + "_" + field.name();
                        schemas.put(childName, Schema.createArray(field.schema()));
                        //schemas.put(field.name(), SchemaBuilder.builder(name).nullable().array().items(field.schema()));
                    }
                } else {
                    schemas.put(name, elementSchema);
                }
                break;
            default:
                break;
        }
        return schemas;
    }

    private static Map<String, Object> convertValue(final String name, final Schema schema, final Object value) {
        final Map<String, Object> values = new HashMap<>();
        if(value == null) {
            //values.put(name, null);
            return values;
        }
        switch (schema.getType()) {
            case MAP:
            case INT:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            case LONG:
            case FIXED:
            case ENUM:
            case BYTES:
            case STRING:
            case RECORD:
            case NULL:
                values.put(name, value);
                break;
            case UNION:
                final org.apache.avro.Schema unnested = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                values.putAll(convertValue(name, unnested, value));
                break;
            case ARRAY:
                final Schema elementSchema = AvroSchemaUtil.unnestUnion(schema.getElementType());
                if(Schema.Type.RECORD.equals(elementSchema.getType())) {
                    final List<GenericRecord> records = (List<GenericRecord>) value;
                    for(final Schema.Field field : elementSchema.getFields()) {
                        final String childName = name + "_" + field.name();
                        final List<Object> fieldValue = records.stream()
                                .filter(Objects::nonNull)
                                .map(record -> record.get(field.name()))
                                .map(v -> {
                                    if(v == null) {
                                        switch (AvroSchemaUtil.unnestUnion(field.schema()).getType()) {
                                            case INT:
                                            case LONG:
                                                return 0L;
                                            case FLOAT:
                                            case DOUBLE:
                                                return 0D;
                                            case ENUM:
                                            case STRING:
                                                return "";
                                            default:
                                                return 0;
                                        }
                                    } else {
                                        return v;
                                    }
                                })
                                .collect(Collectors.toList());
                        values.put(childName, fieldValue);
                    }
                } else {
                    values.put(name, value);
                }
                break;
        }
        return values;
    }

}
