package com.mercari.solution.util.schema.converter;

import com.google.gson.*;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class JsonToAvroConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToAvroConverter.class);

    public static GenericRecord convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonObject jsonObject = new Gson().fromJson(text, JsonObject.class);
        return convert(schema, jsonObject);
    }

    public static GenericRecord convert(final Schema schema, final JsonElement jsonElement) {
        if(jsonElement == null) {
            return null;
        }
        if(jsonElement.isJsonObject()) {
            return convert(schema, jsonElement.getAsJsonObject());
        } else if(jsonElement.isJsonArray()) {
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            final JsonArray array = jsonElement.getAsJsonArray();
            for(int i=0; i<schema.getFields().size(); i++) {
                final Schema.Field field = schema.getFields().get(i);
                final String fieldName = Optional.ofNullable(field.getProp(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)).orElse(field.name());
                if(i < array.size()) {
                    final JsonElement element = array.get(i);
                    builder.set(field.name(), convertValue(field.schema(), element));
                } else {
                    builder.set(field.name(), null);
                }
            }
            return builder.build();
        } else {
            return null;
        }
    }

    public static GenericRecord convert(final Schema schema, final JsonObject jsonObject) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final String fieldName = Optional.ofNullable(field.getProp(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)).orElse(field.name());
            builder.set(field.name(), convertValue(field.schema(), jsonObject.get(fieldName)));
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
            if(schema.getFields().stream().noneMatch(f -> f.name().equals(entry.getKey()))) {
                LOG.error("Validation error: json field: {} is not present in schema.", entry.getKey());
                return false;
            }
            final Schema.Field field = schema.getField(entry.getKey());
            final JsonElement element = entry.getValue();
            if(!AvroSchemaUtil.isNullable(field.schema()) && element.isJsonNull()) {
                LOG.error("Validation error: json field: {} is null but schema is not nullable", entry.getKey());
                return false;
            }
            if(element.isJsonNull()) {
                continue;
            }
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            switch (fieldSchema.getType()) {
                case RECORD -> {
                    if(!element.isJsonObject()) {
                        LOG.error("Validation error: json field: {} is not JsonObject. element: {}", entry.getKey(), element);
                        return false;
                    }
                    if(!validateSchema(fieldSchema, element.getAsJsonObject())) {
                        return false;
                    }
                }
                case ARRAY -> {
                    if(!element.isJsonArray()) {
                        LOG.error("Validation error: json field: {} is not JsonArray. element: {}", entry.getKey(), element);
                        return false;
                    }
                }
                default -> {
                    if(!element.isJsonPrimitive()) {
                        LOG.error("Validation error: json field: {} is not JsonPrimitive. element: {}", entry.getKey(), element);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static Object convertValue(final Schema schema, final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            if(Schema.Type.ARRAY.equals(schema.getType())) {
                return new ArrayList<>();
            }
            return AvroSchemaUtil.convertDefaultValue(AvroSchemaUtil.unnestUnion(schema), AvroSchemaUtil.unnestUnion(schema).getProp("default"));
        }
        try {
            return switch (schema.getType()) {
                case ENUM -> {
                    final String str = jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
                    yield new GenericData.EnumSymbol(schema, str);
                }
                case STRING -> jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
                case FIXED, BYTES -> {
                    if (AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                        final LogicalTypes.Decimal decimalType = AvroSchemaUtil.getLogicalTypeDecimal(schema);
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final BigDecimal decimal = new BigDecimal(jsonPrimitive.getAsString(), new MathContext(decimalType.getPrecision()));
                                yield ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
                            } else if (jsonPrimitive.isNumber()) {
                                final BigDecimal decimal = jsonPrimitive.getAsBigDecimal();
                                yield ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
                            } else {
                                throw new IllegalStateException();
                            }
                        }
                    }
                    yield ByteBuffer.wrap(jsonElement.isJsonPrimitive() ? jsonElement.getAsString().getBytes() : jsonElement.toString().getBytes());
                }
                case INT -> {
                    if (LogicalTypes.date().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalDate localDate = DateTimeUtil.toLocalDate(jsonPrimitive.getAsString());
                                yield localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                            } else if (jsonPrimitive.isNumber()) {
                                yield jsonPrimitive.getAsInt();
                            } else {
                                yield null;
                            }
                        } else if(jsonElement.isJsonObject()) {
                            final JsonObject jsonObject = jsonElement.getAsJsonObject();
                            if(jsonObject.has("year") && jsonObject.has("month") && jsonObject.has("day")) {
                                int year = jsonObject.get("year").getAsInt();
                                int month = jsonObject.get("month").getAsInt();
                                int day = jsonObject.get("day").getAsInt();
                                yield Long.valueOf(LocalDate.of(year, month, day).toEpochDay()).intValue();
                            } else {
                                throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to date");
                            }
                        } else {
                            throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to date");
                        }
                    } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalTime localTime = DateTimeUtil.toLocalTime(jsonPrimitive.getAsString());
                                yield DateTimeUtil.toMilliOfDay(localTime);
                            } else if (jsonPrimitive.isNumber()) {
                                yield jsonPrimitive.getAsInt();
                            } else {
                                throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to time");
                            }
                        } else {
                            throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to time");
                        }
                    }
                    yield jsonElement.isJsonPrimitive() ? Integer.valueOf(jsonElement.getAsString()) : null;
                }
                case LONG -> {
                    if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                            || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final String pattern = schema.getProp("patternTimestamp");
                                final Instant instant;
                                if (pattern != null) {
                                    instant = Instant.parse(jsonPrimitive.getAsString(), DateTimeFormat.forPattern(pattern));
                                } else {
                                    instant = DateTimeUtil.toJodaInstant(jsonPrimitive.getAsString());
                                }
                                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                                    yield instant.getMillis();
                                } else {
                                    yield instant.getMillis() * 1000;
                                }
                            } else if (jsonPrimitive.isNumber()) {
                                yield DateTimeUtil.assumeEpochMilliSecond(jsonPrimitive.getAsLong());
                            } else {
                                throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp millis");
                            }
                        } else if(jsonElement.isJsonObject()) {
                            final JsonObject jsonObject = jsonElement.getAsJsonObject();
                            if(jsonObject.has("seconds") && jsonObject.has("nanos")) {
                                try {
                                    final Long seconds = jsonObject.get("seconds").getAsLong();
                                    final Integer nanos = jsonObject.get("nanos").getAsInt();
                                    if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                                        yield DateTimeUtil.toEpochMicroSecond(seconds, nanos) / 1000L;
                                    } else {
                                        yield DateTimeUtil.toEpochMicroSecond(seconds, nanos);
                                    }
                                } catch (Throwable e) {
                                    throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp millis", e);
                                }
                            } else {
                                throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp millis");
                            }
                        } else {
                            throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp millis");
                        }
                    } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalTime localTime = DateTimeUtil.toLocalTime(jsonPrimitive.getAsString());
                                yield DateTimeUtil.toMicroOfDay(localTime);
                            } else if (jsonPrimitive.isNumber()) {
                                yield DateTimeUtil.assumeEpochMicroSecond(jsonPrimitive.getAsLong());
                            } else {
                                throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp micros");
                            }
                        } else {
                            throw new IllegalStateException("json fieldType: " + schema.getType() + ", value: " + jsonElement + " could not be convert to timestamp micros");
                        }
                    }
                    yield jsonElement.isJsonPrimitive() ? jsonElement.getAsLong() : null;
                }
                case FLOAT -> jsonElement.isJsonPrimitive() ? jsonElement.getAsFloat() : null;
                case DOUBLE -> jsonElement.isJsonPrimitive() ? jsonElement.getAsDouble() : null;
                case BOOLEAN -> jsonElement.isJsonPrimitive() ? jsonElement.getAsBoolean() : null;
                case RECORD -> {
                    if (!jsonElement.isJsonObject()) {
                        throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is %s",
                                schema.getType(), jsonElement));
                    }
                    yield convert(schema, jsonElement.getAsJsonObject());
                }
                case ARRAY -> {
                    if (!jsonElement.isJsonArray()) {
                        throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is %s",
                                schema.getType(), jsonElement));
                    }
                    final List<Object> childValues = new ArrayList<>();
                    for (final JsonElement childJsonElement : jsonElement.getAsJsonArray()) {
                        if (childJsonElement.isJsonArray()) {
                            throw new IllegalArgumentException("JsonElement is not JsonArray: " + childJsonElement);
                        }
                        final Object arrayValue = convertValue(schema.getElementType(), childJsonElement);
                        if (arrayValue != null) {
                            childValues.add(arrayValue);
                        }
                    }
                    yield childValues;
                }
                case UNION -> {
                    final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                    yield convertValue(childSchema, jsonElement);
                }
                default -> null;
            };
        } catch (final Exception e) {
            return null;
        }
    }

}
