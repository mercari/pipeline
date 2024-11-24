package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ElementToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ElementToTableRowConverter.class);

    public static TableSchema convertSchema(final Schema schema) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field.getName(), field.getFieldType()));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableRow convert(final Schema schema, final MElement element) {
        final TableRow row = new TableRow();
        for(final Schema.Field field : schema.getFields()) {
            final Object fieldValue = element.getPrimitiveValue(field.getName());
            final Object convertedFieldValue = convertTableRowValue(field.getFieldType(), fieldValue);
            row.set(field.getName(), convertedFieldValue);
        }
        return row;
    }

    public static TableRow convert(final Schema schema, final Map<String, Object> values) {
        final TableRow row = new TableRow();
        for(final Schema.Field field : schema.getFields()) {
            final Object fieldValue = values.get(field.getName());
            final Object convertedFieldValue = convertTableRowValue(field.getFieldType(), fieldValue);
            row.set(field.getName(), convertedFieldValue);
        }
        return row;
    }

    private static Object convertTableRowValue(final Schema.FieldType fieldType, final Object value) {
        if(fieldType == null) {
            throw new IllegalArgumentException(String.format("Schema of fieldValue: %s must not be null!", value));
        }
        if(value == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool -> switch (value) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case string, json -> switch (value) {
                case String s -> s;
                default -> value.toString();
            };
            case int8 -> switch (value) {
                case Number n -> n.byteValue();
                case String s -> Byte.parseByte(s);
                default -> throw new IllegalArgumentException();
            };
            case int16 -> switch (value) {
                case Number n -> n.shortValue();
                case String s -> Short.parseShort(s);
                default -> throw new IllegalArgumentException();
            };
            case int32 -> switch (value) {
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                default -> throw new IllegalArgumentException();
            };
            case int64 -> switch (value) {
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                default -> throw new IllegalArgumentException();
            };
            case float32 -> switch (value) {
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                default -> throw new IllegalArgumentException();
            };
            case float64 -> switch (value) {
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                default -> throw new IllegalArgumentException();
            };
            case enumeration -> switch (value) {
                case Number n -> fieldType.getSymbols().get(n.intValue());
                case String s -> fieldType.getSymbols().contains(s) ? s : null;
                default -> throw new IllegalArgumentException();
            };
            case bytes -> switch (value) {
                case byte[] b -> Base64.getEncoder().encodeToString(b);
                case ByteBuffer b -> Base64.getEncoder().encodeToString(b.array());
                case String s -> s;
                default -> throw new IllegalArgumentException();
            };
            case decimal -> {
                byte[] bytes = ((ByteBuffer) value).array();
                yield BigDecimal.valueOf(new BigInteger(bytes).longValue(), 18).toString();
            }
            case date -> LocalDate
                    .ofEpochDay((Integer) value)
                    .format(DateTimeFormatter.ISO_LOCAL_DATE);
            case time -> LocalTime
                    .ofNanoOfDay((Long) value * 1000L)
                    .format(DateTimeFormatter.ISO_LOCAL_TIME);
            case timestamp -> switch (value) {
                case Number n -> DateTimeUtil.toInstant(n.longValue()).toString();
                case String s -> DateTimeUtil.toInstant(s).toString();
                default -> throw new IllegalArgumentException();
            };
            case element -> {
                if(value instanceof Map map) {
                    yield convert(fieldType.getElementSchema(), map);
                } else {
                    throw new IllegalArgumentException("Illegal element value: " + value);
                }
            }
            case map -> {
                if(value instanceof Map<?,?> v) {
                    final Map<String, Object> map = (Map<String, Object>) v;
                    yield map.entrySet().stream()
                            .map(entry -> new TableRow()
                                    .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                    .set("value", convertTableRowValue(fieldType.getMapValueType(), entry.getValue())))
                            .collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Illegal map value: " + value);
                }
            }
            case array -> {
                if(value instanceof List<?> list) {
                    yield list.stream()
                            .map(v -> convertTableRowValue(fieldType.getArrayValueType(), v))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                } else {
                    yield List.of(convertTableRowValue(fieldType.getArrayValueType(), value));
                }
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static TableFieldSchema convertTableFieldSchema(final Schema.Field field) {
        return convertTableFieldSchema(field.getName(), field.getFieldType());
    }


    private static TableFieldSchema convertTableFieldSchema(final String name, final Schema.FieldType fieldType) {
        final String mode = fieldType.getNullable() ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setMode(mode);
        return switch (fieldType.getType()) {
            case bool -> tableFieldSchema.setName(name).setType("BOOLEAN");
            case string, enumeration -> tableFieldSchema.setName(name).setType("STRING");
            case geography -> tableFieldSchema.setName(name).setType("GEOGRAPHY");
            case json -> tableFieldSchema.setName(name).setType("JSON");
            case bytes -> tableFieldSchema.setName(name).setType("BYTES");
            case decimal -> tableFieldSchema.setName(name).setType("NUMERIC");
            case int32 -> tableFieldSchema.setName(name).setType("INTEGER").set("avroSchema", "INT");
            case int64 -> tableFieldSchema.setName(name).setType("INTEGER");
            case float32 -> tableFieldSchema.setName(name).setType("FLOAT").set("avroSchema", "FLOAT");
            case float64 -> tableFieldSchema.setName(name).setType("FLOAT");
            case date -> tableFieldSchema.setName(name).setType("DATE");
            case time -> tableFieldSchema.setName(name).setType("TIME");
            case timestamp -> tableFieldSchema.setName(name).setType("TIMESTAMP");
            case datetime ->  tableFieldSchema.setName(name).setType("DATETIME");
            case element -> {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType.getElementSchema().getFields().stream()
                        .map(ElementToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                yield tableFieldSchema.setName(name).setType("RECORD").setFields(childTableFieldSchemas);
            }
            case array -> {
                final TableFieldSchema elementSchema = convertTableFieldSchema(name, fieldType.getArrayValueType());
                if (elementSchema.getType().equals("RECORD")) {
                    yield tableFieldSchema
                            .setName(name)
                            .setType(elementSchema.getType())
                            .setFields(elementSchema.getFields())
                            .setMode("REPEATED");
                } else {
                    yield tableFieldSchema
                            .setName(name)
                            .setType(elementSchema.getType())
                            .setMode("REPEATED");
                }
            }
            case map -> {
                final List<TableFieldSchema> fields = new ArrayList<>();
                fields.add(new TableFieldSchema()
                        .setName("key")
                        .setMode("REQUIRED")
                        .setType("STRING"));
                fields.add(convertTableFieldSchema("value", fieldType.getMapValueType()));
                yield tableFieldSchema
                        .setName(name)
                        .setType("RECORD")
                        .setFields(fields)
                        .setMode("REPEATED");
            }
            default -> throw new IllegalArgumentException(fieldType.getType() + " is not supported for bigquery.");
        };
    }

}
