package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElementToAvroConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ElementToAvroConverter.class);

    public static org.apache.avro.Schema convertSchema(final List<Schema.Field> fields) {
        return convertSchema("root", fields);
    }

    public static org.apache.avro.Schema convertSchema(final String name, final List<Schema.Field> fields) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaFields = SchemaBuilder.record(name).fields();
        for(final Schema.Field field : fields) {
            SchemaBuilder.FieldBuilder<org.apache.avro.Schema> fieldBuilder = schemaFields
                    .name(field.getName())
                    .doc(field.getDescription())
                    .orderIgnore();

            // set altName
            if(field.getOptions().containsKey(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)) {
                fieldBuilder = fieldBuilder.prop(SourceConfig.OPTION_ORIGINAL_FIELD_NAME, field.getOptions().get(SourceConfig.OPTION_ORIGINAL_FIELD_NAME));
            }

            final org.apache.avro.Schema fieldSchema = convertFieldSchema(field.getFieldType(), field.getName(), null);

            // set default
            final SchemaBuilder.GenericDefault<org.apache.avro.Schema> fieldAssembler = fieldBuilder.type(fieldSchema);
            if(field.getFieldType().getDefaultValue() != null) {
                final String defaultValue = field.getFieldType().getDefaultValue();
                schemaFields = fieldAssembler.withDefault(AvroSchemaUtil.convertDefaultValue(fieldSchema, defaultValue));
            } else {
                schemaFields = fieldAssembler.noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord convert(final AvroWriteRequest<MElement> writeRequest) {
        return convert(writeRequest.getSchema(), writeRequest.getElement());
    }

    public static GenericRecord convert(final Schema schema, final MElement element) {
        if(element == null || element.getValue() == null) {
            return null;
        }
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String,Object>)element.getValue());
            case AVRO -> (GenericRecord) element.getValue();
            case ROW -> RowToAvroConverter.convert(schema.getAvroSchema(), (Row) element.getValue());
            case STRUCT -> (GenericRecord) element.getValue();
            case DOCUMENT -> (GenericRecord) element.getValue();
            case ENTITY -> (GenericRecord) element.getValue();
            default -> throw new IllegalArgumentException();
        };
    }

    public static GenericRecord convert(final org.apache.avro.Schema schema, final MElement element) {
        if(element == null || element.getValue() == null) {
            return null;
        }
        final Object value = element.getValue();
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String,Object>) value);
            case AVRO -> (GenericRecord) element.getValue();
            case ROW -> RowToAvroConverter.convert(schema, (Row) value);
            case STRUCT -> StructToAvroConverter.convert(schema, (Struct) value);
            case DOCUMENT -> DocumentToAvroConverter.convert(schema, (Document) value);
            case ENTITY -> EntityToAvroConverter.convert(schema, (Entity) value);
            default -> throw new IllegalArgumentException();
        };
    }

    public static GenericRecord convert(final Schema schema, final Map<String, Object> values) {
        return convert(schema.getAvroSchema(), values);
    }

    public static GenericRecord convert(final org.apache.avro.Schema schema, final Map<String, Object> values) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final org.apache.avro.Schema.Field field : schema.getFields()) {
            builder.set(field, convertValue(field.name(), field.schema(), values.get(field.name())));
        }
        return builder.build();
    }

    private static org.apache.avro.Schema convertFieldSchema(
            final Schema.FieldType fieldType,
            final String fieldName,
            final String parentNamespace) {

        final org.apache.avro.Schema fieldSchema = switch (fieldType.getType()) {
            case bool -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
            case string -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
            case bytes -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
            case decimal -> LogicalTypes.decimal(38, 9).addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
            case int8, int16, int32 -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
            case int64 -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
            case float8, float16, float32 -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT);
            case float64 -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
            case timestamp -> LogicalTypes.timestampMicros().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
            case date -> LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
            case time -> LogicalTypes.timeMicros().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
            case json -> {
                final org.apache.avro.Schema jsonSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
                jsonSchema.addProp("sqlType", "JSON");
                yield jsonSchema;
            }
            case enumeration -> org.apache.avro.Schema.createEnum(fieldName, null, parentNamespace, fieldType.getSymbols());
            case element -> convertSchema(fieldName, fieldType.getElementSchema().getFields());
            case array -> {
                org.apache.avro.Schema arrayElementSchema = convertFieldSchema(fieldType.getArrayValueType(), fieldName, parentNamespace);
                if(AvroSchemaUtil.isNullable(arrayElementSchema)) {
                    arrayElementSchema = AvroSchemaUtil.unnestUnion(arrayElementSchema);
                }
                yield org.apache.avro.Schema.createArray(arrayElementSchema);
            }
            case map -> {
                org.apache.avro.Schema mapValueSchema = convertFieldSchema(fieldType.getMapValueType(), fieldName, parentNamespace);
                yield org.apache.avro.Schema.createMap(mapValueSchema);
            }
            default -> throw new IllegalArgumentException(fieldType.getType() + " type is not supported for avro.");
        };

        /*
        if(fieldOptions != null
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY.equals(fieldType.getTypeName())
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ITERABLE.equals(fieldType.getTypeName())) {
            for(final String optionName : fieldOptions.getOptionNames()) {
                final Object optionValue = fieldOptions.getValue(optionName);
                fieldSchema.addProp(optionName, optionValue);
            }
        }
         */

        if(fieldType.getNullable()) {
            return org.apache.avro.Schema.createUnion(fieldSchema, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
        } else {
            return fieldSchema;
        }
    }

    private static Object convertValue(
            final String name,
            final org.apache.avro.Schema fieldSchema,
            final Object value) {

        if(value == null) {
            return null;
        }

        return switch (fieldSchema.getType()) {
            case UNION -> convertValue(name, AvroSchemaUtil.unnestUnion(fieldSchema), value);
            case BOOLEAN -> switch (value) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (value) {
                case String s -> s;
                case ByteBuffer b -> new String(b.array(), StandardCharsets.UTF_8);
                default -> value.toString();
            };
            case BYTES -> switch (value) {
                case byte[] b -> b;
                case ByteBuffer b -> b;
                case String s -> Base64.getDecoder().decode(s);
                default -> throw new IllegalArgumentException();
            };
            case INT -> switch (value) {
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                default -> throw new IllegalArgumentException();
            };
            case LONG -> switch (value) {
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                default -> throw new IllegalArgumentException();
            };
            case FLOAT -> switch (value) {
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (value) {
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                default -> throw new IllegalArgumentException();
            };
            case ENUM -> {
                final int index = (Integer) value;
                yield AvroSchemaUtil.createEnumSymbol(name, fieldSchema.getEnumSymbols(), fieldSchema.getEnumSymbols().get(index));
            }
            case MAP -> {
                final Map<String,Object> results = new HashMap<>();
                final Map<String,Object> map = (Map<String,Object>) value;
                for(final Map.Entry<String,Object> entry : map.entrySet()) {
                    results.put(entry.getKey(), convertValue(name, fieldSchema.getValueType(), entry.getValue()));
                }
                yield results;
            }
            case RECORD -> convert(fieldSchema, (Map<String,Object>) value);
            case ARRAY -> ((List<Object>) value).stream()
                    .map(v -> convertValue(name, fieldSchema.getElementType(), v))
                    .toList();
            default -> throw new IllegalArgumentException();
        };
    }

}
