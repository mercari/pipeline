package com.mercari.solution.util.schema.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RowToJsonConverter {

    private RowToJsonConverter() {}

    public static String convert(final Row row, final List<String> fields) {
        return convert(row);
    }

    public static String convert(final Row row) {
        return convertObject(row).toString();

    }

    public static JsonObject convertObject(final Row row) {
        final JsonObject obj = new JsonObject();
        if(row == null) {
            return obj;
        }
        row.getSchema().getFields().forEach(f -> setValue(obj, f, row));
        return obj;
    }

    private static void setValue(final JsonObject obj, final Schema.Field field, final Row row) {
        final String fieldName = field.getName();
        final boolean isNullField = row.getValue(fieldName) == null;
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                obj.addProperty(fieldName, isNullField ? null : row.getBoolean(fieldName));
                break;
            case BYTE:
                obj.addProperty(fieldName, isNullField ? null : row.getByte(fieldName));
                break;
            case INT16:
                obj.addProperty(fieldName, isNullField ? null : row.getInt16(fieldName));
                break;
            case INT32:
                obj.addProperty(fieldName, isNullField ? null : row.getInt32(fieldName));
                break;
            case INT64:
                obj.addProperty(fieldName, isNullField ? null : row.getInt64(fieldName));
                break;
            case FLOAT: {
                final Float floatValue = row.getFloat(fieldName);
                if (floatValue == null || Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                    obj.addProperty(fieldName, (Float) null);
                } else {
                    obj.addProperty(fieldName, floatValue);
                }
                break;
            }
            case DOUBLE: {
                final Double doubleValue = row.getDouble(fieldName);
                if (doubleValue == null || Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    obj.addProperty(fieldName, (Double) null);
                } else {
                    obj.addProperty(fieldName, doubleValue);
                }
                break;
            }
            case STRING:
                obj.addProperty(fieldName, isNullField ? null : row.getString(fieldName));
                break;
            case BYTES:
                obj.addProperty(fieldName, isNullField ? null : java.util.Base64.getEncoder().encodeToString(row.getBytes(fieldName)));
                break;
            case DATETIME:
                obj.addProperty(fieldName, isNullField ? null : row.getDateTime(fieldName).toString());
                break;
            case DECIMAL:
                obj.addProperty(fieldName, isNullField ? null : row.getDecimal(fieldName).toString());
                break;
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    final LocalDate localDate = row.getLogicalTypeValue(fieldName, LocalDate.class);
                    obj.addProperty(fieldName, isNullField ? null : localDate.toString());
                } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                    final LocalTime localTime = row.getLogicalTypeValue(fieldName, LocalTime.class);
                    obj.addProperty(fieldName, isNullField ? null : localTime.toString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    final Instant instant = row.getLogicalTypeValue(fieldName, Instant.class);
                    obj.addProperty(fieldName, isNullField ? null : instant.toString());
                } else if(RowSchemaUtil.isLogicalTypeEnum(field.getType())) {
                    final EnumerationType.Value enumValue = row.getLogicalTypeValue(fieldName, EnumerationType.Value.class);
                    obj.addProperty(fieldName, isNullField ? null : RowSchemaUtil.toString(field.getType(), enumValue));
                } else {
                    obj.addProperty(fieldName, isNullField ? null : row.getValue(fieldName).toString());
                }
                break;
            }
            case MAP: {
                final JsonObject mapObj = convertMap(field.getType(), row.getMap(fieldName));
                obj.add(field.getName(), mapObj);
                break;
            }
            case ROW:
                obj.add(fieldName, isNullField ? null : convertObject(row.getRow(fieldName)));
                break;
            case ITERABLE:
            case ARRAY:
                obj.add(field.getName(), convertArray(field.getType(), row.getArray(fieldName)));
                break;
            default:
                break;
        }
    }

    private static JsonArray convertArray(final Schema.FieldType fieldType, final Collection<?> arrayValue) {
        final JsonArray array = new JsonArray();
        if(arrayValue == null || arrayValue.size() == 0) {
            return array;
        }
        switch (fieldType.getCollectionElementType().getTypeName()) {
            case BOOLEAN:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Boolean) v)
                        .forEach(array::add);
                break;
            case BYTE:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Byte) v)
                        .forEach(array::add);
                break;
            case INT16:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Short) v)
                        .forEach(array::add);
                break;
            case INT32:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Integer) v)
                        .forEach(array::add);
                break;
            case INT64:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Long) v)
                        .forEach(array::add);
                break;
            case FLOAT:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> {
                            final Float floatValue = (Float) v;
                            if (Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                                return null;
                            } else {
                                return floatValue;
                            }
                        })
                        .forEach(array::add);
                break;
            case DOUBLE:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> {
                            final Double doubleValue = (Double) v;
                            if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                                return null;
                            } else {
                                return doubleValue;
                            }
                        })
                        .forEach(array::add);
                break;
            case STRING:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            case BYTES:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map((bytes) -> java.util.Base64.getEncoder().encodeToString((byte[])bytes))
                        .forEach(array::add);
                break;
            case DATETIME:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (ReadableInstant) v)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            case DECIMAL:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (BigDecimal) v)
                        .map(BigDecimal::doubleValue)
                        .forEach(array::add);
                break;
            case LOGICAL_TYPE: {
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            }
            case MAP:
                arrayValue.stream()
                        .map(o -> (Map)o)
                        .map(m -> convertMap(fieldType.getCollectionElementType(), m))
                        .forEach(array::add);
                break;
            case ROW:
                arrayValue.stream()
                        .map(o -> (Row)o)
                        .map(RowToJsonConverter::convertObject)
                        .forEach(array::add);
                break;
            case ITERABLE:
            case ARRAY:
            default:
                break;
        }
        return array;
    }

    private static JsonObject convertMap(final Schema.FieldType fieldType, final Map<?,?> map) {
        final JsonObject mapObject = new JsonObject();
        for(Map.Entry<?,?> entry : map.entrySet()) {
            if(entry.getValue() == null) {
                mapObject.addProperty(entry.getKey().toString(), (String) null);
                continue;
            }
            final String name = entry.getKey().toString();
            switch (fieldType.getMapValueType().getTypeName()) {
                case BYTE:
                    mapObject.addProperty(name, (Byte)entry.getValue());
                    break;
                case INT16:
                    mapObject.addProperty(name, (Short)entry.getValue());
                    break;
                case INT32:
                    mapObject.addProperty(name, (Integer)entry.getValue());
                    break;
                case INT64:
                    mapObject.addProperty(name, (Long)entry.getValue());
                    break;
                case FLOAT:
                    mapObject.addProperty(name, (Float)entry.getValue());
                    break;
                case DOUBLE:
                    mapObject.addProperty(name, (Double)entry.getValue());
                    break;
                case BOOLEAN:
                    mapObject.addProperty(name, (Boolean) entry.getValue());
                    break;
                case STRING:
                    mapObject.addProperty(name, (String)entry.getValue());
                    break;
                case DATETIME:
                    mapObject.addProperty(name, (entry.getValue()).toString());
                    break;
                case ROW: {
                    final JsonObject childObject = convertObject((Row) entry.getValue());
                    mapObject.add(name, childObject);
                    break;
                }
                case MAP: {
                    final JsonObject childObject = convertMap(fieldType.getMapValueType(),(Map) entry.getValue());
                    mapObject.add(name, childObject);
                    break;
                }
                case ITERABLE:
                case ARRAY: {
                    final JsonArray jsonArray = convertArray(fieldType.getCollectionElementType(), (Collection<?>) entry.getValue());
                    mapObject.add(name, jsonArray);
                    break;
                }
            }
        }
        return mapObject;
    }

}
