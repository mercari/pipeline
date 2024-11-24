package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.Timestamp;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityToAvroConverter {

    private static final String KEY_FIELD_NAME = "__key__";
    private static final Logger LOG = LoggerFactory.getLogger(EntityToAvroConverter.class);

    private static final Schema KEY_SCHEMA = SchemaBuilder
            .record(KEY_FIELD_NAME).fields()
            .requiredString("namespace")
            .requiredString("app")
            .requiredString("path")
            .requiredString("kind")
            .optionalString("name")
            .optionalLong("id")
            .endRecord();
    private EntityToAvroConverter() {}

    private static Schema addKeyToSchema(final String schemaString) {
        try {
            final Schema schema = new Schema.Parser().parse(schemaString);
            return addKeyToSchema(schema);
        } catch (SchemaParseException e) {
            LOG.error(String.format("Failed to parse schema: %s", schemaString));
            throw e;
        }
    }

    public static Schema addKeyToSchema(final Schema schema) {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
                .record(schema.getName())
                .namespace(schema.getNamespace())
                .fields().name(KEY_FIELD_NAME).type(KEY_SCHEMA).noDefault();
        schema.getFields().stream().forEach(f -> {
            if(f.defaultVal() == null) {
                builder.name(f.name()).type(f.schema()).noDefault();
            } else if(f.defaultVal().equals(JsonProperties.NULL_VALUE)) {
                builder.name(f.name()).type(f.schema()).withDefault(null);
            } else {
                builder.name(f.name()).type(f.schema()).withDefault(f.defaultVal());
            }
        });
        return builder.endRecord();
    }

    public static GenericRecord convert(final AvroWriteRequest<Entity> request) {
        return convert(request.getSchema(), request.getElement());
    }


    public static GenericRecord convert(final Schema schema, final Entity entity) {
        return convert(schema, entity, 0);
    }

    private static GenericRecord convert(final Schema schema, final Entity entity, final int depth) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null! " + entity.getKey());
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        if(depth == 0) {
            try {
                if (schema.getField(KEY_FIELD_NAME) != null) {
                    builder.set(KEY_FIELD_NAME, convertKeyRecord(entity.getKey()));
                }
            } catch (NullPointerException e) {
                throw new RuntimeException(convertKeyRecord(entity.getKey()).toString(), e);
            }
        }
        for(final Schema.Field field : schema.getFields()) {
            if(field.name().equals(KEY_FIELD_NAME)) {
                continue;
            }
            final Value value = getValue(field, entity);
            builder.set(field, convertEntityValue(value, field.schema(), depth + 1));
        }
        return builder.build();
    }

    private static Value getValue(final Schema.Field field, final Entity entity) {
        if (entity == null) {
            return null;
        }

        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(field.name()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private static Object convertEntityValue(final Value value, final Schema schema, final int depth) {
        if (value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {

            if(Schema.Type.ARRAY.equals(AvroSchemaUtil.unnestUnion(schema).getType())) {
                return new ArrayList<>();
            }
            return null;
        }

        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return value.getStringValue();
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final BigDecimal bigDecimal;
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE:
                            bigDecimal = new BigDecimal(value.getStringValue());
                            break;
                        case INTEGER_VALUE:
                            bigDecimal = BigDecimal.valueOf(value.getIntegerValue());
                            break;
                        case DOUBLE_VALUE:
                            bigDecimal = BigDecimal.valueOf(value.getDoubleValue());
                            break;
                        default:
                            throw new IllegalStateException("Not supported decimal value: " + value);
                    }
                    final int scale = schema.getObjectProp("scale") != null ?
                            Integer.parseInt(schema.getObjectProp("scale").toString()) : 0;
                    return ByteBuffer.wrap(bigDecimal.setScale(scale).unscaledValue().toByteArray());
                }
                return value.getBlobValue().asReadOnlyByteBuffer();
            }
            case BOOLEAN:
                return value.getBooleanValue();
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final LocalDate localDate = DateTimeUtil.toLocalDate(value.getStringValue(), true);
                            if(localDate == null) {
                                throw new IllegalArgumentException("Illegal date string: " + value.getStringValue());
                            }
                            return Long.valueOf(localDate.toEpochDay()).intValue();
                        }
                        case INTEGER_VALUE:
                            return Long.valueOf(value.getIntegerValue()).intValue();
                        default:
                            throw new IllegalArgumentException();
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final LocalTime localTime = DateTimeUtil.toLocalTime(value.getStringValue(), true);
                            if(localTime == null) {
                                throw new IllegalArgumentException("Illegal time string: " + value.getStringValue());
                            }
                            return Long.valueOf(localTime.toNanoOfDay()).intValue() / 1000_000;
                        }
                        case INTEGER_VALUE:
                            return Long.valueOf(value.getIntegerValue()).intValue();
                        default:
                            throw new IllegalArgumentException();
                    }
                } else {
                    return Long.valueOf(value.getIntegerValue()).intValue();
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    final Timestamp timestamp = value.getTimestampValue();
                    return timestamp.getSeconds() * 1000;
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Timestamp timestamp = value.getTimestampValue();
                    return timestamp.getSeconds() * 1000000;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    switch (value.getValueTypeCase()) {
                        case STRING_VALUE: {
                            final LocalTime localTime = DateTimeUtil.toLocalTime(value.getStringValue(), true);
                            if(localTime == null) {
                                throw new IllegalArgumentException("Illegal time string: " + value.getStringValue());
                            }
                            return localTime.toNanoOfDay() / 1000;
                        }
                        case INTEGER_VALUE:
                            return value.getIntegerValue();
                        default:
                            throw new IllegalArgumentException();
                    }
                } else {
                    return value.getIntegerValue();
                }
            case FLOAT:
                return Double.valueOf(value.getDoubleValue()).floatValue();
            case DOUBLE:
                return value.getDoubleValue();
            case RECORD:
                return convert(schema, value.getEntityValue(), depth);
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> convertEntityValue(v, schema.getElementType(), depth))
                        .collect(Collectors.toList());
            case UNION:
                final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                return convertEntityValue(value, childSchema, depth);
            case MAP:
            case NULL:
            default:
                return null;
        }
    }

    private static GenericRecord convertKeyRecord(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key.toString() + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        final String path = key.getPathList().stream()
                .map(e -> String.format("\"%s\", %s", e.getKind(), (e.getName() == null || e.getName().length() == 0) ?
                        Long.toString(e.getId()) : String.format("\"%s\"", e.getName())))
                .collect(Collectors.joining(", "));
        return new GenericRecordBuilder(KEY_SCHEMA)
                .set("namespace", key.getPartitionId().getNamespaceId())
                .set("app", key.getPartitionId().getProjectId())
                .set("path", path)
                .set("kind", lastPath.getKind())
                .set("name", lastPath.getName() == null || lastPath.getName().length() == 0 ? null : lastPath.getName())
                .set("id", lastPath.getId() == 0 ? null : lastPath.getId())
                .build();
    }

}
