package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElementToEntityConverter {

    private static final String KEY_FIELD_NAME = "__key__";
    private static final Logger LOG = LoggerFactory.getLogger(ElementToEntityConverter.class);

    private ElementToEntityConverter() {}

    public static Entity.Builder convertBuilder(final Schema schema, final MElement element, final List<String> excludeFromIndexFields) {
        return switch (element.getType()) {
            case ELEMENT -> convertBuilder(schema, element.asPrimitiveMap(), excludeFromIndexFields);
            case AVRO -> AvroToEntityConverter.convertBuilder(schema.getAvroSchema(), (GenericRecord) element.getValue(), excludeFromIndexFields);
            case ROW -> RowToEntityConverter.convertBuilder(schema.getRowSchema(), (Row) element.getValue(), excludeFromIndexFields);
            //case STRUCT -> StructToEntityConverter.convertBuilder(schema.getRowSchema(), (Struct) element.getValue(), excludeFromIndexFields);
            default -> throw new IllegalArgumentException();
        };
    }

    public static Entity.Builder convertBuilder(final Schema schema, final Map<String, Object> values, final List<String> excludeFromIndexFields) {
        final Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            if(KEY_FIELD_NAME.equals(field.getName()) && Schema.Type.element.equals(field.getFieldType().getType())) {
                final Map<String, Object> keyRecord = (Map<String, Object>) values.get(KEY_FIELD_NAME);
                final Key key = createPathElement(KEY_FIELD_NAME, keyRecord);
                builder.setKey(key);
            } else {
                final Object value = values.getOrDefault(field.getName(), null);
                if (excludeFromIndexFields.isEmpty()) {
                    builder.putProperties(field.getName(), convertValue(field.getFieldType(), value, false));
                } else {
                    final boolean excludeFromIndexes = excludeFromIndexFields.contains(field.getName());
                    builder.putProperties(field.getName(), convertValue(field.getFieldType(), value, excludeFromIndexes));
                }
            }
        }
        return builder;
    }

    private static Value convertValue(final Schema.FieldType fieldType, final Object value, final boolean excludeFromIndexes) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        final Value.Builder builder = switch (fieldType.getType()) {
            case bool -> Value.newBuilder().setBooleanValue((Boolean) value);
            case float32 -> Value.newBuilder().setDoubleValue((Float) value);
            case float64 -> Value.newBuilder().setDoubleValue((Double) value);
            case bytes -> {
                final ByteString byteString = ByteString.copyFrom((ByteBuffer) value);
                if(byteString.size() >= DatastoreUtil.QUOTE_VALUE_SIZE) {
                    yield Value.newBuilder().setBlobValue(byteString).setExcludeFromIndexes(true);
                } else {
                    yield Value.newBuilder().setBlobValue(byteString);
                }
            }
            case string, enumeration -> {
                final String stringValue = value.toString();
                if(stringValue.getBytes().length >= DatastoreUtil.QUOTE_VALUE_SIZE) {
                    yield Value.newBuilder().setStringValue(stringValue).setExcludeFromIndexes(true);
                } else {
                    yield Value.newBuilder().setStringValue(stringValue);
                }
            }
            case int32 -> Value.newBuilder().setIntegerValue((Integer) value);
            case int64 -> Value.newBuilder().setIntegerValue((Long) value);
            case date -> Value.newBuilder()
                    .setStringValue(LocalDate
                            .ofEpochDay((Integer) value)
                            .format(DateTimeFormatter.ISO_LOCAL_DATE));
            case time -> Value.newBuilder()
                    .setStringValue(LocalTime
                            .ofNanoOfDay(((long) value) * 1000)
                            .format(DateTimeFormatter.ISO_LOCAL_TIME));
            case timestamp -> Value.newBuilder()
                    .setTimestampValue(com.google.cloud.Timestamp.ofTimeMicroseconds((long) value).toProto());
            case element -> {
                final Map<String, Object> childRecord = (Map<String, Object>) value;
                final Entity.Builder entityBuilder = Entity.newBuilder();
                for (final Schema.Field field : fieldType.getElementSchema().getFields()) {
                    final Object childValue = childRecord.getOrDefault(field.getName(), null);
                    entityBuilder.putProperties(field.getName(), convertValue(field.getFieldType(), childValue, excludeFromIndexes));
                }
                yield Value.newBuilder()
                        .setEntityValue(entityBuilder.build())
                        .setExcludeFromIndexes(true);
            }
            case array -> Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues(((List<Object>) value).stream()
                                        .map(o -> convertValue(fieldType.getArrayValueType(), o, excludeFromIndexes))
                                        .collect(Collectors.toList()))
                                .build());
            default -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE);
        };
        if (excludeFromIndexes) {
            return builder.setExcludeFromIndexes(true).build();
        }
        return builder.build();
    }

    private static Key createPathElement(final String kind, final Map<String, Object> keyRecord) {

        Key.Builder keyBuilder = Key.newBuilder();
        final String path = keyRecord.get("path").toString();
        final String[] paths = path.split(",");
        for(int i=0; i<paths.length - 2; i+=2) {
            final String k = paths[i];
            final String v = paths[i+1];
            if(v.contains("\"")) {
                keyBuilder = keyBuilder.addPath(Key.PathElement.newBuilder()
                        .setKind(k.replaceAll("\"", ""))
                        .setName(v.replaceAll("\"", "")));
            } else {
                keyBuilder = keyBuilder.addPath(Key.PathElement.newBuilder()
                        .setKind(k.replaceAll("\"", ""))
                        .setId(Long.valueOf(v)));
            }
        }

        Key.PathElement.Builder lastPathBuilder = Key.PathElement.newBuilder()
                .setKind(kind == null ? keyRecord.get("kind").toString() : kind);
        if(keyRecord.get("id") != null && (long)keyRecord.get("id") != 0) {
            lastPathBuilder = lastPathBuilder.setId((long) keyRecord.get("id"));
        } else if(keyRecord.get("name") != null) {
            lastPathBuilder = lastPathBuilder.setName(keyRecord.get("name").toString());
        } else {
            throw new IllegalArgumentException("Entity field value must not be null id or name.");
        }

        return keyBuilder.addPath(lastPathBuilder).build();
    }


    private static final Schema KEY_SCHEMA = Schema.builder()
            .withField("namespace", Schema.FieldType.STRING)
            .withField("app", Schema.FieldType.STRING)
            .withField("path", Schema.FieldType.STRING)
            .withField("kind", Schema.FieldType.STRING)
            .withField("name", Schema.FieldType.STRING)
            .withField("id", Schema.FieldType.INT64)
            .build();

}
