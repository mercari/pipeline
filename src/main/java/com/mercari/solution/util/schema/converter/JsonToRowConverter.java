package com.mercari.solution.util.schema.converter;

import com.google.gson.*;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class JsonToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToRowConverter.class);

    public static Row convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonElement jsonElement = new Gson().fromJson(text, JsonElement.class);
        return convert(schema, jsonElement);
    }

    public static Row convert(final Schema schema, final JsonElement jsonElement) {
        if(jsonElement.isJsonObject()) {
            return convert(schema, jsonElement.getAsJsonObject());
        } else if(jsonElement.isJsonArray()) {
            final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
            final JsonArray array = jsonElement.getAsJsonArray();
            for(int i=0; i<schema.getFieldCount(); i++) {
                final Schema.Field field = schema.getField(i);
                if(i < array.size()) {
                    final JsonElement arrayElement = array.get(i);
                    builder.withFieldValue(field.getName(), convertValue(field.getType(), field.getOptions(), arrayElement));
                } else {
                    builder.withFieldValue(field.getName(), null);
                }
            }
            return builder.build();
        } else {
            return null;
        }
    }

    public static Row convert(final Schema schema, final JsonObject jsonObject) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final String fieldName;
            if(field.getOptions().hasOption(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)) {
                fieldName = field.getOptions().getValue(SourceConfig.OPTION_ORIGINAL_FIELD_NAME);
            } else {
                fieldName = field.getName();
            }
            builder.withFieldValue(field.getName(), convertValue(field.getType(), field.getOptions(), jsonObject.get(fieldName)));
        }
        return builder.build();
    }

    public static boolean validateSchema(final Schema schema, final String json) {
        if(json == null || json.trim().length() < 2) {
            return false;
        }
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        return validateSchema(schema, jsonObject);
    }

    public static boolean validateSchema(final Schema schema, final JsonObject jsonObject) {
        for(final Map.Entry<String,JsonElement> entry : jsonObject.entrySet()) {
            if(!schema.hasField(entry.getKey())) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is not present in schema.");
                return false;
            }
            final Schema.Field field = schema.getField(entry.getKey());
            final JsonElement element = entry.getValue();
            if(field.getType().getNullable() != null && !field.getType().getNullable() && element.isJsonNull()) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is null but schema is not nullable");
                return false;
            }
            if(element.isJsonNull()) {
                continue;
            }
            switch (field.getType().getTypeName()) {
                case ROW: {
                    if(!element.isJsonObject()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonObject. element: " + element);
                        return false;
                    }
                    if(!validateSchema(field.getType().getRowSchema(), element.getAsJsonObject())) {
                        return false;
                    }
                    break;
                }
                case ITERABLE:
                case ARRAY: {
                    if(!element.isJsonArray()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonArray. element: " + element);
                        return false;
                    }
                    break;
                }
                default: {
                    if(!element.isJsonPrimitive()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonPrimitive. element: " + element);
                        return false;
                    }
                    break;
                }
            }
        }
        return true;
    }

    private static Object convertValue(final Schema.FieldType fieldType, final Schema.Options fieldOptions, final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            if(Schema.TypeName.ARRAY.equals(fieldType.getTypeName())) {
                return new ArrayList<>();
            }
            return RowSchemaUtil.getDefaultValue(fieldType, fieldOptions);
        }
        switch (fieldType.getTypeName()) {
            case STRING:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
            case BYTES:
                return jsonElement.isJsonPrimitive() ? Base64.getUrlDecoder().decode(jsonElement.getAsString()) : null;
            case INT16:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsShort() : null;
            case INT32:
                return jsonElement.isJsonPrimitive() ? Integer.valueOf(jsonElement.getAsString()) : null;
            case INT64:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsLong() : null;
            case FLOAT:
                return jsonElement.isJsonPrimitive() ? Float.valueOf(jsonElement.getAsString()) : null;
            case DOUBLE:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsDouble() : null;
            case BOOLEAN:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsBoolean() : null;
            case DATETIME: {
                if(jsonElement.isJsonPrimitive()) {
                    final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                    if(primitive.isString()) {
                        return DateTimeUtil.toJodaInstant(jsonElement.getAsString());
                    } else if(primitive.isNumber()) {
                        if(primitive.getAsString().contains(".")) {
                            return DateTimeUtil.toJodaInstant(primitive.getAsDouble());
                        } else {
                            return DateTimeUtil.toJodaInstant(primitive.getAsLong());
                        }
                    } else {
                        final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to timestamp";
                        LOG.warn(message);
                        throw new IllegalStateException(message);
                    }
                } else {
                    final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to timestamp";
                    LOG.warn(message);
                    throw new IllegalStateException(message);
                }
            }
            case DECIMAL: {
                if(!jsonElement.isJsonPrimitive()) {
                    final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to decimal";
                    LOG.warn(message);
                    throw new IllegalStateException(message);
                }
                final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                if(jsonPrimitive.isString()) {
                    return new BigDecimal(jsonPrimitive.getAsString());
                } else if(jsonPrimitive.isNumber()) {
                    return new BigDecimal(jsonPrimitive.getAsString());
                } else {
                    throw new IllegalStateException("Can not convert Decimal type from jsonElement: " + jsonElement.toString());
                }
            }
            case LOGICAL_TYPE: {
                if(!jsonElement.isJsonPrimitive()) {
                    final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to logicalType";
                    LOG.warn(message);
                    throw new IllegalStateException(message);
                }
                final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    if(primitive.isString()) {
                        return DateTimeUtil.toLocalDate(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        return LocalDate.ofEpochDay(primitive.getAsLong());
                    } else {
                        throw new IllegalStateException("json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to date");
                    }
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    if(primitive.isString()) {
                        return DateTimeUtil.toLocalTime(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        return LocalTime.ofSecondOfDay(primitive.getAsLong());
                    } else {
                        throw new IllegalStateException("json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to time");
                    }
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    if(primitive.isString()) {
                        return DateTimeUtil.toJodaInstant(jsonElement.getAsString());
                    } else if(primitive.isNumber()) {
                        return DateTimeUtil.toJodaInstant(primitive.getAsLong());
                    } else {
                        final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + jsonElement + " could not be convert to timestamp";
                        LOG.warn(message);
                        throw new IllegalStateException(message);
                    }
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String enumString = primitive.getAsString();
                    return RowSchemaUtil.toEnumerationTypeValue(fieldType, enumString);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW:
                if(!jsonElement.isJsonObject() && !jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                return convert(fieldType.getRowSchema(), jsonElement);
            case MAP: {
                if(!jsonElement.isJsonObject()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is map, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                return jsonElement.getAsJsonObject().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> convertValue(fieldType.getMapKeyType(), fieldOptions, new JsonPrimitive(e.getKey())),
                                e -> convertValue(fieldType.getMapValueType(), fieldOptions, e.getValue())));
            }
            case ITERABLE:
            case ARRAY:
                if(!jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                // for maprecord
                if(fieldType.getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                    final Schema.FieldType elementFieldType = fieldType.getCollectionElementType();
                    final Schema.Options options = elementFieldType.getRowSchema().getOptions();
                    if(options.hasOption("extension") && options.getValue("extension").equals("maprecord")) {
                        final Schema schema = Schema.builder()
                                .addField("key", elementFieldType.getMapKeyType())
                                .addField("value", elementFieldType.getMapValueType())
                                .build();
                        return jsonElement.getAsJsonObject().entrySet().stream()
                                .map(e -> Row.withSchema(schema)
                                            .withFieldValue("key", convertValue(elementFieldType.getMapKeyType(), fieldOptions, new JsonPrimitive(e.getKey())))
                                            .withFieldValue("value", convertValue(elementFieldType.getMapValueType(), fieldOptions, e.getValue()))
                                            .build())
                                .collect(Collectors.toList());
                    }
                }

                final List<Object> childValues = new ArrayList<>();
                for(final JsonElement childJsonElement : jsonElement.getAsJsonArray()) {
                    if(!Schema.TypeName.ROW.equals(fieldType.getCollectionElementType().getTypeName())
                            && childJsonElement.isJsonArray()) {
                        throw new IllegalArgumentException("Not supported Array in Array field");
                    }
                    final Object arrayValue = convertValue(fieldType.getCollectionElementType(), fieldOptions, childJsonElement);
                    if(arrayValue != null) {
                        childValues.add(arrayValue);
                    }
                }
                return childValues;
            case BYTE:
            default:
                return null;
        }
    }

}
