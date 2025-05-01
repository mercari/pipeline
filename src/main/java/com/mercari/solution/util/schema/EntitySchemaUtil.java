package com.mercari.solution.util.schema;

import com.google.cloud.Date;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.EntityToMapConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class EntitySchemaUtil {

    public static Object getFieldValue(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(EntitySchemaUtil::getValue)
                .findAny()
                .orElse(null);
    }

    public static String getFieldValueAsString(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(EntitySchemaUtil::getValue)
                .map(v -> v == null ? null : v.toString())
                .findAny()
                .orElse(null);
    }

    public static Object getKeyFieldValue(final Entity entity, String fieldName) {
        final Key.PathElement pe = entity.getKey().getPath(entity.getKey().getPathCount()-1);
        return pe.getName() == null ? pe.getId() : pe.getName();
    }

    public static Object getValue(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case KEY_VALUE -> value.getKeyValue();
            case STRING_VALUE -> value.getStringValue();
            case BLOB_VALUE -> value.getBlobValue().toByteArray();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case TIMESTAMP_VALUE -> Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            case ENTITY_VALUE -> value.getEntityValue();
            case ARRAY_VALUE ->
                value.getArrayValue().getValuesList()
                        .stream()
                        .map(v -> {
                            if (v == null) {
                                return null;
                            }
                            return switch (v.getValueTypeCase()) {
                                case KEY_VALUE -> v.getKeyValue();
                                case BOOLEAN_VALUE -> v.getBooleanValue();
                                case INTEGER_VALUE -> v.getIntegerValue();
                                case BLOB_VALUE -> v.getBlobValue().toByteArray();
                                case STRING_VALUE -> v.getStringValue();
                                case DOUBLE_VALUE -> v.getDoubleValue();
                                case GEO_POINT_VALUE -> v.getGeoPointValue();
                                case TIMESTAMP_VALUE ->
                                        Instant.ofEpochMilli(Timestamps.toMillis(v.getTimestampValue()));
                                case ENTITY_VALUE -> v.getEntityValue();
                                default -> null;
                            };
                        })
                        .collect(Collectors.toList());
            case GEO_POINT_VALUE -> value.getGeoPointValue();
            case VALUETYPE_NOT_SET, NULL_VALUE -> null;
            default -> throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        };
    }

    public static Object getValue(Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case KEY_VALUE -> value.getKeyValue();
            case STRING_VALUE -> value.getStringValue();
            case BLOB_VALUE -> value.getBlobValue();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case TIMESTAMP_VALUE -> value.getTimestampValue();
            case ENTITY_VALUE -> value.getEntityValue();
            case ARRAY_VALUE -> value.getArrayValue();
            case VALUETYPE_NOT_SET, NULL_VALUE -> null;
            default -> throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        };
    }

    public static String getAsString(final Value value) {
        final Object object = getValue(value);
        if(object == null) {
            return null;
        }
        return object.toString();
    }

    public static String getAsString(final Object entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        return getAsString((Entity) entity, fieldName);
    }

    public static String getAsString(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case KEY_VALUE -> toStringKey(value.getKeyValue());
            case STRING_VALUE -> value.getStringValue();
            case BLOB_VALUE -> Base64.getEncoder().encodeToString(value.getBlobValue().toByteArray());
            case INTEGER_VALUE -> Long.toString(value.getIntegerValue());
            case DOUBLE_VALUE -> Double.toString(value.getDoubleValue());
            case BOOLEAN_VALUE -> Boolean.toString(value.getBooleanValue());
            case TIMESTAMP_VALUE -> Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString();
            default -> null;
        };
    }

    public static Long getAsLong(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> {
                return value.getBooleanValue() ? 1L : 0L;
            }
            case INTEGER_VALUE -> {
                return value.getIntegerValue();
            }
            case DOUBLE_VALUE -> {
                return Double.valueOf(value.getDoubleValue()).longValue();
            }
            case STRING_VALUE -> {
                try {
                    return Long.valueOf(value.getStringValue());
                } catch (Exception e) {
                    return null;
                }
            }
            default -> {
                return null;
            }
        }
    }

    public static Double getAsDouble(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> {
                return value.getBooleanValue() ? 1D : 0D;
            }
            case INTEGER_VALUE -> {
                return Long.valueOf(value.getIntegerValue()).doubleValue();
            }
            case DOUBLE_VALUE -> {
                return value.getDoubleValue();
            }
            case STRING_VALUE -> {
                try {
                    return Double.valueOf(value.getStringValue());
                } catch (Exception e) {
                    return null;
                }
            }
            default -> {
                return null;
            }
        }
    }

    public static BigDecimal getAsBigDecimal(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> {
                return BigDecimal.valueOf(value.getBooleanValue() ? 1D : 0D);
            }
            case INTEGER_VALUE -> {
                return BigDecimal.valueOf(value.getIntegerValue());
            }
            case DOUBLE_VALUE -> {
                return BigDecimal.valueOf(value.getDoubleValue());
            }
            case STRING_VALUE -> {
                try {
                    return BigDecimal.valueOf(Double.valueOf(value.getStringValue()));
                } catch (Exception e) {
                    return null;
                }
            }
            default -> {
                return null;
            }
        }
    }

    public static ByteString getAsByteString(final Entity entity, final String fieldName) {
        if(entity == null || fieldName == null) {
            return null;
        }

        if(!entity.getPropertiesMap().containsKey(fieldName)) {
            return null;
        }

        final Value value = entity.getPropertiesOrThrow(fieldName);
        return switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> BigtableSchemaUtil.toByteString(value.getBooleanValue());
            case STRING_VALUE -> BigtableSchemaUtil.toByteString(value.getStringValue());
            case BLOB_VALUE -> BigtableSchemaUtil.toByteString(value.getBlobValue().asReadOnlyByteBuffer());
            case INTEGER_VALUE -> BigtableSchemaUtil.toByteString(value.getIntegerValue());
            case DOUBLE_VALUE -> BigtableSchemaUtil.toByteString(value.getDoubleValue());
            case TIMESTAMP_VALUE -> BigtableSchemaUtil.toByteString(Timestamps.toMicros(value.getTimestampValue()));
            case ENTITY_VALUE -> BigtableSchemaUtil.toByteString(asPrimitiveMap(value.getEntityValue()));
            default -> null;
        };
    }

    public static List<Float> getAsFloatList(final Entity entity, final String fieldName) {
        if(entity == null || fieldName == null) {
            return new ArrayList<>();
        }

        if(!entity.getPropertiesMap().containsKey(fieldName)) {
            return new ArrayList<>();
        }

        final Value value = entity.getPropertiesOrThrow(fieldName);
        return switch (value.getValueTypeCase()) {
            case ARRAY_VALUE -> {
                final List<Value> arrayList = value.getArrayValue().getValuesList();
                if(arrayList.isEmpty()) {
                    yield new ArrayList<>();
                }
                yield arrayList.stream().map(v -> switch (v.getValueTypeCase()) {
                        case DOUBLE_VALUE -> Double.valueOf(v.getDoubleValue()).floatValue();
                        case INTEGER_VALUE -> Long.valueOf(v.getIntegerValue()).floatValue();
                        case STRING_VALUE -> Float.valueOf(v.getStringValue());
                        case BOOLEAN_VALUE -> v.getBooleanValue() ? 1F :  0F;
                        default -> null;
                    }).collect(Collectors.toList());
            }
            case NULL_VALUE, VALUETYPE_NOT_SET -> new ArrayList<>();
            default -> new ArrayList<>();
        };
    }

    public static Object getAsPrimitive(Object object, Schema.FieldType fieldType, String field) {
        if(object == null) {
            return null;
        }
        if(!(object instanceof Entity)) {
            return null;
        }
        final Entity entity = (Entity) object;
        final Value value = entity.getPropertiesOrDefault(field, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        return getAsPrimitive(fieldType, value);
    }

    private static Object getAsPrimitive(final Schema.FieldType fieldType, final Value value) {
        if(Value.ValueTypeCase.NULL_VALUE.equals(value.getValueTypeCase())) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case INT32 -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> Integer.valueOf(value.getStringValue());
                    case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).intValue();
                    case DOUBLE_VALUE -> Double.valueOf(value.getDoubleValue()).intValue();
                    case BOOLEAN_VALUE -> value.getBooleanValue() ? 1 : 0;
                    default -> throw new IllegalStateException();
                };
            }
            case INT64 -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> Long.valueOf(value.getStringValue());
                    case INTEGER_VALUE -> value.getIntegerValue();
                    case DOUBLE_VALUE -> Double.valueOf(value.getDoubleValue()).longValue();
                    case BOOLEAN_VALUE -> value.getBooleanValue() ? 1L : 0L;
                    default -> throw new IllegalStateException();
                };
            }
            case FLOAT -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> Float.valueOf(value.getStringValue());
                    case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).floatValue();
                    case DOUBLE_VALUE -> Double.valueOf(value.getDoubleValue()).floatValue();
                    case BOOLEAN_VALUE -> value.getBooleanValue() ? 1F : 0F;
                    default -> throw new IllegalStateException();
                };
            }
            case DOUBLE -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> Double.valueOf(value.getStringValue());
                    case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).doubleValue();
                    case DOUBLE_VALUE -> value.getDoubleValue();
                    case BOOLEAN_VALUE -> value.getBooleanValue() ? 1D : 0D;
                    default -> throw new IllegalStateException();
                };
            }
            case BOOLEAN -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> Boolean.valueOf(value.getStringValue());
                    case INTEGER_VALUE -> value.getIntegerValue() > 0;
                    case DOUBLE_VALUE -> value.getDoubleValue() > 0;
                    case BOOLEAN_VALUE -> value.getBooleanValue();
                    default -> throw new IllegalStateException();
                };
            }
            case STRING -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> value.getStringValue();
                    case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).toString();
                    case DOUBLE_VALUE -> Double.valueOf(value.getDoubleValue()).toString();
                    case BOOLEAN_VALUE -> Boolean.valueOf(value.getBooleanValue()).toString();
                    case TIMESTAMP_VALUE -> value.getTimestampValue().toString();
                    default -> throw new IllegalStateException();
                };
            }
            case DATETIME -> {
                return switch (value.getValueTypeCase()) {
                    case STRING_VALUE -> DateTimeUtil.toEpochMicroSecond(value.getStringValue());
                    case INTEGER_VALUE -> value.getIntegerValue();
                    case DOUBLE_VALUE -> Double.valueOf(value.getDoubleValue()).longValue();
                    case TIMESTAMP_VALUE -> DateTimeUtil.toEpochMicroSecond(value.getTimestampValue());
                    default -> throw new IllegalStateException();
                };
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return switch (value.getValueTypeCase()) {
                        case STRING_VALUE -> Long.valueOf(DateTimeUtil.toLocalDate(value.getStringValue()).toEpochDay()).intValue();
                        case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).intValue();
                        case NULL_VALUE, VALUETYPE_NOT_SET -> null;
                        default -> throw new IllegalStateException();
                    };
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return switch (value.getValueTypeCase()) {
                        case STRING_VALUE -> Long.valueOf(DateTimeUtil.toLocalTime(value.getStringValue()).toSecondOfDay()).intValue();
                        case INTEGER_VALUE -> Long.valueOf(value.getIntegerValue()).intValue();
                        case NULL_VALUE, VALUETYPE_NOT_SET -> null;
                        default -> throw new IllegalStateException();
                    };
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return value.getStringValue();
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> {
                return value.getArrayValue().getValuesList()
                        .stream()
                        .map((Value v) -> getAsPrimitive(fieldType.getCollectionElementType(), v))
                        .collect(Collectors.toList());
            }
            default -> throw new IllegalStateException();
        }
    }


    public static Object getAsPrimitive(final Schema.FieldType fieldType, final Object fieldValue) {
        if(fieldValue == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING, INT64, DOUBLE, BOOLEAN -> {
                return fieldValue;
            }
            case INT32 -> {
                return ((Long) fieldValue).intValue();
            }
            case FLOAT -> {
                return ((Double) fieldValue).floatValue();
            }
            case DATETIME -> {
                return DateTimeUtil.toEpochMicroSecond((com.google.protobuf.Timestamp) fieldValue);
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toEpochDay((Date)fieldValue);
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime((String) fieldValue).toNanoOfDay() / 1000L;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return fieldValue;
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT64, DOUBLE, BOOLEAN, STRING -> {
                        return fieldValue;
                    }
                    case INT32 -> {
                        return ((List<Long>) fieldValue).stream()
                                .map(Long::intValue)
                                .collect(Collectors.toList());
                    }
                    case FLOAT -> {
                        return ((List<Double>) fieldValue).stream()
                                .map(Double::floatValue)
                                .collect(Collectors.toList());
                    }
                    case DATETIME -> {
                        return ((List<com.google.protobuf.Timestamp>) fieldValue).stream()
                                .map(DateTimeUtil::toEpochMicroSecond)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        return ((List<Object>) fieldValue).stream()
                                .map(o -> {
                                    if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                                        return DateTimeUtil.toEpochDay((Date)o);
                                    } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                                        return DateTimeUtil.toLocalTime((String) o).toNanoOfDay() / 1000L;
                                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                                        return o;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    default -> throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object getAsPrimitive(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case STRING_VALUE -> value.getStringValue();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BLOB_VALUE -> value.getBlobValue().toByteArray();
            case TIMESTAMP_VALUE -> DateTimeUtil.toEpochMicroSecond(value.getTimestampValue());
            case GEO_POINT_VALUE -> value.getGeoPointValue().toString();
            case ENTITY_VALUE -> EntityToMapConverter.convert(value.getEntityValue());
            case NULL_VALUE, VALUETYPE_NOT_SET -> null;
            case ARRAY_VALUE -> value.getArrayValue().getValuesList().stream()
                    .map(EntitySchemaUtil::getAsPrimitive)
                    .collect(Collectors.toList());
            default -> throw new IllegalStateException();
        };
    }

    public static Object getAsStandard(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case STRING_VALUE -> value.getStringValue();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BLOB_VALUE -> ByteBuffer.wrap(value.getBlobValue().toByteArray());
            case TIMESTAMP_VALUE -> DateTimeUtil.toEpochMicroSecond(value.getTimestampValue());
            case GEO_POINT_VALUE -> value.getGeoPointValue().toString();
            case ENTITY_VALUE -> EntityToMapConverter.convert(value.getEntityValue());
            case ARRAY_VALUE -> value.getArrayValue().getValuesList().stream()
                    .map(EntitySchemaUtil::getAsPrimitive)
                    .collect(Collectors.toList());
            case KEY_VALUE -> value.getKeyValue().toString();
            case NULL_VALUE, VALUETYPE_NOT_SET -> null;
        };
    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        if (primitiveValue == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEAN:
                return primitiveValue;
            case DATETIME: {
                return DateTimeUtil.toProtoTimestamp((Long)primitiveValue);
            }
            case LOGICAL_TYPE: {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return LocalDate.ofEpochDay((Integer) primitiveValue).toString();
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return LocalTime.ofNanoOfDay((Long) primitiveValue).toString();
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final int index = (Integer) primitiveValue;
                    return fieldType.getLogicalType(EnumerationType.class).valueOf(index);
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE:
            case ARRAY: {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> {
                        return primitiveValue;
                    }
                    case DATETIME -> {
                        return ((List<Long>) primitiveValue).stream()
                                .map(DateTimeUtil::toProtoTimestamp)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            return ((List<Integer>) primitiveValue).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                            return ((List<Long>) primitiveValue).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                            return ((List<Integer>) primitiveValue).stream()
                                    .map(index -> fieldType.getLogicalType(EnumerationType.class).valueOf(index))
                                    .collect(Collectors.toList());
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            default:
                throw new IllegalStateException();
        }
    }

    public static Map<String, Object> asPrimitiveMap(final Entity entity) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(entity == null) {
            return primitiveMap;
        }
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            final Object value = getAsPrimitive(entry.getValue());
            primitiveMap.put(entry.getKey(), value);
        }
        return primitiveMap;
    }

    public static Map<String, Object> asStandardMap(final Entity entity, final Collection<String> fields) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(entity == null) {
            return primitiveMap;
        }
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            if(fields == null || fields.isEmpty() || fields.contains(entry.getKey())) {
                final Object value = getAsPrimitive(entry.getValue());
                primitiveMap.put(entry.getKey(), value);
            }
        }
        return primitiveMap;
    }

    public static Date convertDate(final Value value) {
        if(Value.ValueTypeCase.STRING_VALUE.equals(value.getValueTypeCase())) {
            final String datestr = value.getStringValue();
            final LocalDate localDate = DateTimeUtil.toLocalDate(datestr);
            return Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
        } else if(Value.ValueTypeCase.INTEGER_VALUE.equals(value.getValueTypeCase())) {
            final LocalDate localDate = LocalDate.ofEpochDay(value.getIntegerValue());
            return Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
        } else {
            throw new IllegalArgumentException();
        }

    }

    public static Entity.Builder toBuilder(final Schema schema, final Entity entity) {
        return toBuilder(schema, entity, new HashMap<>());
    }

    public static Entity.Builder toBuilder(final Schema schema, final Entity entity, final Map<String, String> renameFields) {
        final Entity.Builder builder = Entity.newBuilder();
        builder.setKey(entity.getKey());
        final Map<String,Value> values = entity.getPropertiesMap();
        for(final Schema.Field field : schema.getFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();

            if(values.containsKey(getFieldName)) {
                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Entity> children = new ArrayList<>();
                            for(final Value child : values.get(getFieldName).getArrayValue().getValuesList()) {
                                if(child == null || child.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child.getEntityValue()).build());
                                }
                            }
                            final List<Value> entityValues = children.stream()
                                    .map(e -> {
                                        if(e == null) {
                                            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                                        }
                                        return Value.newBuilder().setEntityValue(e).build();
                                    })
                                    .collect(Collectors.toList());
                            builder.putProperties(field.getName(), Value.newBuilder()
                                    .setArrayValue(ArrayValue.newBuilder().addAllValues(entityValues))
                                    .build());
                        } else {
                            builder.putProperties(field.getName(), values.get(getFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Entity child = toBuilder(field.getType().getRowSchema(), values.get(getFieldName).getEntityValue()).build();
                        builder.putProperties(field.getName(), Value.newBuilder().setEntityValue(child).build());
                        break;
                    }
                    default:
                        builder.putProperties(field.getName(), values.get(getFieldName));
                        break;
                }
            } else if(renameFields.containsValue(setFieldName)) {
                final String getOuterFieldName = renameFields.entrySet().stream()
                        .filter(e -> e.getValue().equals(setFieldName))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .orElse(setFieldName);
                if(!values.containsKey(getOuterFieldName) || values.get(getOuterFieldName) == null) {
                    builder.putProperties(field.getName(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Value> children = new ArrayList<>();
                            for(final Value child : values.get(getOuterFieldName).getArrayValue().getValuesList()) {
                                if(child != null && child.getNullValue() != null && child.getEntityValue() != null) {
                                    Entity.Builder childBuilder = toBuilder(field.getType().getCollectionElementType().getRowSchema(), child.getEntityValue());
                                    children.add(Value.newBuilder().setEntityValue(childBuilder).build());
                                }
                            }
                            builder.putProperties(setFieldName, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(children).build()).build());
                        } else {
                            builder.putProperties(setFieldName, values.get(getOuterFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Entity child = toBuilder(field.getType().getRowSchema(), values.get(getOuterFieldName).getEntityValue()).build();
                        builder.putProperties(setFieldName, Value.newBuilder().setEntityValue(child).build());
                        break;
                    }
                    default:
                        builder.putProperties(setFieldName, values.get(getOuterFieldName));
                        break;
                }
            } else {
                builder.putProperties(field.getName(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            }
        }
        return builder;
    }

    public static Entity.Builder toBuilder(final Entity entity) {
        return Entity.newBuilder(entity);
    }

    public static Entity.Builder toBuilder(final Entity entity,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Entity.Builder builder = Entity.newBuilder();
        for(var entry : entity.getPropertiesMap().entrySet()) {
            if(includeFields != null && !includeFields.contains(entry.getKey())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(entry.getKey())) {
                continue;
            }
            builder.putProperties(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static Entity.Builder convertBuilder(final Schema schema, final Entity entity, final List<String> excludeFromIndexFields) {
        if (excludeFromIndexFields == null || excludeFromIndexFields.size() == 0) {
            return entity.toBuilder();
        }
        final Entity.Builder builder = Entity.newBuilder();
        builder.setKey(entity.getKey());
        for(Schema.Field field : schema.getFields()) {
            final Value value = entity.getPropertiesOrDefault(field.getName(),
                    Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            final boolean excludeFromIndexes = excludeFromIndexFields.contains(field.getName());
            if (excludeFromIndexes) {
                builder.putProperties(field.getName(), value.toBuilder()
                        .setExcludeFromIndexes(true)
                        .build());
            } else {
                builder.putProperties(field.getName(), value);
            }
        }
        return builder;
    }

    public static Entity merge(final Schema schema, Entity entity, final Map<String, ? extends Object> values) {
        final Entity.Builder builder = Entity.newBuilder(entity);
        for(final Schema.Field field : schema.getFields()) {
            final Value value;
            if(!values.containsKey(field.getName()) || values.get(field.getName()) == null) {
                value = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            } else {
                final Object object = values.get(field.getName());
                value = switch (field.getType().getTypeName()) {
                    case BOOLEAN -> Value.newBuilder().setBooleanValue((Boolean)object).build();
                    case STRING -> Value.newBuilder().setStringValue(object.toString()).build();
                    case BYTES -> Value.newBuilder().setBlobValue(ByteString.copyFrom((byte[]) object)).build();
                    case BYTE -> Value.newBuilder().setIntegerValue((Byte) object).build();
                    case INT16 -> Value.newBuilder().setIntegerValue((Short) object).build();
                    case INT32 -> Value.newBuilder().setIntegerValue((Integer) object).build();
                    case INT64 -> Value.newBuilder().setIntegerValue((Long) object).build();
                    case FLOAT -> Value.newBuilder().setDoubleValue((Float) object).build();
                    case DOUBLE -> Value.newBuilder().setDoubleValue((Double) object).build();
                    case DATETIME -> Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Long) object)).build();
                    case DECIMAL -> Value.newBuilder().setStringValue(object.toString()).build();
                    case ROW -> Value.newBuilder().setEntityValue((Entity) object).build();
                    default -> throw new IllegalArgumentException("Not supported type: " + field.getName() + ", type: " + field.getType());
                };
            }
            builder.putProperties(field.getName(), value);
        }
        return builder.build();
    }

    public static Entity create(final Schema schema, final Map<String, Object> values) {
        final Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            final Value value;
            if(!values.containsKey(field.getName()) || values.get(field.getName()) == null) {
                value = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            } else {
                final Object object = values.get(field.getName());
                switch (field.getType().getTypeName()) {
                    case BOOLEAN:
                        value = Value.newBuilder().setBooleanValue((Boolean)object).build();
                        break;
                    case STRING:
                        value = Value.newBuilder().setStringValue(object.toString()).build();
                        break;
                    case BYTES:
                        value = Value.newBuilder().setBlobValue(ByteString.copyFrom((byte[]) object)).build();
                        break;
                    case INT32:
                        value = Value.newBuilder().setIntegerValue((Integer) object).build();
                        break;
                    case INT64:
                        value = Value.newBuilder().setIntegerValue((Long) object).build();
                        break;
                    case FLOAT:
                        value = Value.newBuilder().setDoubleValue((Float) object).build();
                        break;
                    case DOUBLE:
                        value = Value.newBuilder().setDoubleValue((Double) object).build();
                        break;
                    case DECIMAL:
                        value = Value.newBuilder().setStringValue(object.toString()).build();
                        break;
                    case DATETIME:
                        value = Value.newBuilder().setTimestampValue((Timestamp) object).build();
                        break;
                    case LOGICAL_TYPE: {
                        if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                            value = Value.newBuilder().setStringValue((String) object).build();
                        } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                            value = Value.newBuilder().setStringValue((String) object).build();
                        } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                            value = Value.newBuilder().setStringValue(object.toString()).build();
                        } else {
                            throw new IllegalStateException();
                        }
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Not supported type: " + field.getName() + ", type: " + field.getType());
                    }
                }
            }
            builder.putProperties(field.getName(), value);
        }
        return builder.build();
    }

    public static byte[] getBytes(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesMap().getOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        return switch (value.getValueTypeCase()) {
            case STRING_VALUE -> Base64.getDecoder().decode(value.getStringValue());
            case BLOB_VALUE -> value.getBlobValue().toByteArray();
            default -> null;
        };
    }

    public static ByteBuffer getAsBytes(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesMap().getOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        final byte[] bytes = switch (value.getValueTypeCase()) {
            case STRING_VALUE -> Base64.getDecoder().decode(value.getStringValue());
            case BLOB_VALUE -> value.getBlobValue().toByteArray();
            default -> null;
        };
        return Optional
                .ofNullable(bytes)
                .map(ByteBuffer::wrap)
                .orElse(null);
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName) {
        return getTimestamp(entity, fieldName, Instant.ofEpochSecond(0L));
    }

    public static Timestamp toProtoTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        final java.time.Instant jinstant = java.time.Instant.ofEpochMilli(instant.getMillis());
        return Timestamp.newBuilder().setSeconds(jinstant.getEpochSecond()).setNanos(jinstant.getNano()).build();
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName, final Instant timestampDefault) {
        final Value value = entity.getPropertiesMap().get(fieldName);
        if(value == null) {
            return timestampDefault;
        }
        switch (value.getValueTypeCase()) {
            case STRING_VALUE: {
                final String stringValue = value.getStringValue();
                try {
                    final java.time.Instant instant = DateTimeUtil.toInstant(stringValue);
                    if(instant == null) {
                        return timestampDefault;
                    }
                    return DateTimeUtil.toJodaInstant(instant);
                } catch (Exception e) {
                    return timestampDefault;
                }
            }
            case INTEGER_VALUE: {
                try {
                    return Instant.ofEpochMilli(value.getIntegerValue());
                } catch (Exception e){
                    return Instant.ofEpochMilli(value.getIntegerValue() / 1000);
                }
            }
            case TIMESTAMP_VALUE: {
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            }
            case KEY_VALUE:
            case BOOLEAN_VALUE:
            case DOUBLE_VALUE:
            case BLOB_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
            default:
                return timestampDefault;
        }
    }

    public static Schema convertSchema(final String prefix, final List<Entity> entities) {
        final Schema.Builder builder = Schema.builder();
        final Map<String, List<Entity>> embeddedFields = new HashMap<>();
        for (final Entity entity : entities) {
            String fieldName = entity
                    .getPropertiesOrThrow("property_name")
                    .getStringValue();
            if(!prefix.isEmpty() && fieldName.startsWith(prefix)) {
                fieldName = fieldName.replaceFirst(prefix + "\\.", "");
            }

            if(fieldName.contains(".")) {
                final String embeddedFieldName = fieldName.split("\\.", 2)[0];
                if(!embeddedFields.containsKey(embeddedFieldName)) {
                    embeddedFields.put(embeddedFieldName, new ArrayList<>());
                }
                embeddedFields.get(embeddedFieldName).add(entity);
                continue;
            }

            final String fieldType = entity.getPropertiesOrThrow("property_type").getStringValue();
            if("EmbeddedEntity".equals(fieldType) || "NULL".equals(fieldType)) {
                continue;
            }

            builder.addField(Schema.Field.of(
                    fieldName,
                    convertFieldType(fieldType))
                    .withNullable(true));
        }

        if(!embeddedFields.isEmpty()) {
            for(final Map.Entry<String, List<Entity>> child : embeddedFields.entrySet()) {
                final String childPrefix = (prefix.isEmpty() ? "" : prefix + ".") + child.getKey();
                final Schema childSchema = convertSchema(childPrefix, child.getValue());
                builder.addField(Schema.Field.of(
                        child.getKey(),
                        Schema.FieldType.row(childSchema).withNullable(true)));
            }
        }

        return builder.build();
    }

    private static Schema.FieldType convertFieldType(final String type) {
        return switch (type) {
            case "Blob", "ShortBlobg" -> Schema.FieldType.BYTES.withNullable(true);
            case "IM", "Link", "Email", "User", "PhoneNumber", "PostalAddress", "Category", "Text", "String" ->
                    Schema.FieldType.STRING;
            case "Rating", "Integer" -> Schema.FieldType.INT64;
            case "Float" -> Schema.FieldType.DOUBLE;
            case "Boolean" -> Schema.FieldType.BOOLEAN;
            case "Date/Time" -> Schema.FieldType.DATETIME;
            //return Schema.FieldType.row(convertSchema(type));
            //return Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            case "EmbeddedEntity", "Key", "NULL" -> Schema.FieldType.STRING;
            default -> throw new IllegalArgumentException("Spanner type: " + type + " not supported!");
        };
    }

    private static String toStringKey(Key key) {
        final List<String> names = new ArrayList<>();
        for(final Key.PathElement path : key.getPathList()) {
            if(path.getName() == null) {
                names.add(path.getKind() + "," + path.getId());
            } else {
                names.add(path.getKind() + "," + path.getName());
            }
        }
        return String.join(",", names);
    }

}
