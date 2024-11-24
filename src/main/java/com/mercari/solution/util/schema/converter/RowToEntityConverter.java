package com.mercari.solution.util.schema.converter;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RowToEntityConverter {

    public static Entity.Builder convertBuilder(final Schema schema, final Row row, final List<String> excludeFromIndexFields) {

        final Entity.Builder builder = Entity.newBuilder();
        for(Schema.Field field : schema.getFields()) {
            if (excludeFromIndexFields.size() == 0) {
                builder.putProperties(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
            } else {
                final boolean excludeFromIndexes = excludeFromIndexFields.contains(field.getName());
                builder.putProperties(field.getName(), convertValue(field.getType(), row.getValue(field.getName()), excludeFromIndexes));
            }
        }
        return builder;
    }

    public static Entity convert(final Schema schema, final Row row,
                                 final String kind, final Iterable<String> keyFields, final String splitter) {

        final Key.PathElement pathElement;
        if(keyFields != null) {
            final StringBuilder sb = new StringBuilder();
            for (final String keyField : keyFields) {
                final String keyValue = RowSchemaUtil.getAsString(row, keyField);
                sb.append(keyValue);
                sb.append(splitter);
            }
            sb.deleteCharAt(sb.length() - splitter.length());
            pathElement = Key.PathElement.newBuilder().setKind(kind).setName(sb.toString()).build();
        } else {
            pathElement = Key.PathElement.newBuilder().setKind(kind).setName(UUID.randomUUID().toString()).build();
        }

        final Key key = Key.newBuilder()
                //.setPartitionId(PartitionId.newBuilder().setProjectId("").setNamespaceId("").build())
                .addPath(pathElement)
                .build();

        Entity.Builder builder = Entity.newBuilder();
        builder.setKey(key);
        for(Schema.Field field : schema.getFields()) {
            builder.putProperties(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
        }
        return builder.build();
    }

    private static Value convertValue(final Schema.FieldType fieldType, final Object value) {
        return convertValue(fieldType, value, false);
    }

    private static Value convertValue(final Schema.FieldType fieldType, final Object value, final boolean excludeFromIndexes) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        final Value.Builder builder;
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                builder = Value.newBuilder().setBooleanValue((Boolean) value);
                break;
            case BYTES: {
                final ByteString byteString = ByteString.copyFrom((ByteBuffer) value);
                if(byteString.size() > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setBlobValue(byteString).setExcludeFromIndexes(true).build();
                }
                builder = Value.newBuilder().setBlobValue(byteString);
                break;
            }
            case DECIMAL:
            case STRING: {
                final String string = value.toString();
                if(string.getBytes().length > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setStringValue(value.toString()).setExcludeFromIndexes(true).build();
                }
                builder = Value.newBuilder().setStringValue(value.toString());
                break;
            }
            case BYTE:
                builder = Value.newBuilder().setIntegerValue((Byte) value);
                break;
            case INT16:
                builder = Value.newBuilder().setIntegerValue((Short) value);
                break;
            case INT32:
                builder = Value.newBuilder().setIntegerValue((Integer) value);
                break;
            case INT64:
                builder = Value.newBuilder().setIntegerValue((Long) value);
                break;
            case FLOAT:
                builder = Value.newBuilder().setDoubleValue((Float) value);
                break;
            case DOUBLE:
                builder = Value.newBuilder().setDoubleValue((Double) value);
                break;
            case DATETIME:
                builder = Value.newBuilder().setTimestampValue(Timestamp
                        .parseTimestamp(((ReadableDateTime) value)
                                .toDateTime()
                                .toString(ISODateTimeFormat.dateTime())).toProto());
                break;
            case LOGICAL_TYPE: {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    builder = Value.newBuilder().setStringValue(value.toString());
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    builder = Value.newBuilder().setStringValue(value.toString());
                } else if (RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    builder = Value.newBuilder().setStringValue(value.toString());
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                    builder = Value.newBuilder().setStringValue(RowSchemaUtil.toString(fieldType, enumValue));
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
                break;
            }
            case ROW: {
                final Row childRow = (Row) value;
                final Entity.Builder entityBuilder = Entity.newBuilder();
                for (Schema.Field field : fieldType.getRowSchema().getFields()) {
                    entityBuilder.putProperties(field.getName(), convertValue(field.getType(), childRow.getValue(field.getName())));
                }
                return Value.newBuilder()
                        .setEntityValue(entityBuilder.build())
                        .setExcludeFromIndexes(true)
                        .build();
            }
            case ITERABLE:
            case ARRAY:
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(((List<Object>) value).stream()
                                .map(o -> convertValue(fieldType.getCollectionElementType(), o, excludeFromIndexes))
                                .collect(Collectors.toList()))
                        .build())
                        .build();
            case MAP:
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        if (excludeFromIndexes) {
            return builder.setExcludeFromIndexes(true).build();
        }
        return builder.build();
    }

}
