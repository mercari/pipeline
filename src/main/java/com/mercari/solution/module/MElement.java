package com.mercari.solution.module;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class MElement implements Serializable {

    private final int index;
    private final DataType type;
    private final Object value;
    private final long epochMillis;

    public int getIndex() {
        return index;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public long getEpochMillis() {
        return epochMillis;
    }

    private MElement(int index, DataType dataType, Object value, long epochMillis) {
        this.index = index;
        this.type = dataType;
        this.value = value;
        this.epochMillis = epochMillis;
    }

    public static MElement of(Schema schema, Map<String, Object> values, org.joda.time.Instant timestamp) {
        return of(schema, values, timestamp.getMillis());
    }

    public static MElement of(Schema schema, Map<String, Object> values, long epochMillis) {
        final Map<String, Object> output = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final Object value = getValue(values, field.getName(), field.getFieldType());
            output.put(field.getName(), value);
        }
        return of(output, epochMillis);
    }

    public static MElement of(Map<String, Object> values, org.joda.time.Instant timestamp) {
        return new MElement(0, DataType.ELEMENT, values, timestamp.getMillis());
    }

    public static MElement of(Map<String, Object> values, long epochMillis) {
        return new MElement(0, DataType.ELEMENT, values, epochMillis);
    }

    public static MElement of(Schema schema, JsonObject jsonObject, long epochMillis) {
        final Row row = JsonToRowConverter.convert(schema.getRow().getSchema(), jsonObject);
        return new MElement(0, DataType.ROW, row, epochMillis);
    }

    public static MElement of(Row row, org.joda.time.Instant timestamp) {
        return of(row, timestamp.getMillis());
    }

    public static MElement of(Row row, long epochMillis) {
        return new MElement(0, DataType.ROW, row, epochMillis);
    }

    public static MElement of(GenericRecord record, org.joda.time.Instant timestamp) {
        return of(record, timestamp.getMillis());
    }

    public static MElement of(GenericRecord record, long epochMillis) {
        return new MElement(0, DataType.AVRO, record, epochMillis);
    }

    public static MElement of(Struct struct, org.joda.time.Instant timestamp) {
        return of(struct, timestamp.getMillis());
    }

    public static MElement of(Struct struct, long epochMillis) {
        return new MElement(0, DataType.STRUCT, struct, epochMillis);
    }

    public static MElement of(Document document, org.joda.time.Instant timestamp) {
        return new MElement(0, DataType.DOCUMENT, document, timestamp.getMillis());
    }

    public static MElement of(Document document, long epochMillis) {
        return new MElement(0, DataType.DOCUMENT, document, epochMillis);
    }

    public static MElement of(Entity entity, long epochMillis) {
        return new MElement(0, DataType.ENTITY, entity, epochMillis);
    }

    public static MElement of(Entity entity, org.joda.time.Instant timestamp) {
        return new MElement(0, DataType.ENTITY, entity, timestamp.getMillis());
    }

    public static MElement of(PubsubMessage message, org.joda.time.Instant timestamp) {
        return of(message, timestamp.getMillis());
    }

    public static MElement of(PubsubMessage message, long epochMillis) {
        return new MElement(0, DataType.MESSAGE, message, epochMillis);
    }

    public static MElement of(int index, DataType dataType, Object value, long epochMillis) {
        return new MElement(index, dataType, value, epochMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final MElement element) {
        return new Builder(element);
    }

    public static Builder builder(final Map<String,Object> primitiveValues) {
        return new Builder(primitiveValues);
    }

    public MElement withEventTime(long epochMillis) {
        return new MElement(index, type, value, epochMillis);
    }

    public MElement withEventTime(Instant timestamp) {
        return new MElement(index, type, value, timestamp.toEpochMilli());
    }

    public MElement withEventTime(org.joda.time.Instant timestamp) {
        return new MElement(index, type, value, timestamp.getMillis());
    }

    //
    public MElement convert(final Schema schema, final DataType dataType) {
        return switch (dataType) {
            case ELEMENT -> switch (type) {
                case ELEMENT -> this;
                default -> throw new IllegalArgumentException("Convert from: " + type + " to Element is not yet supported");
            };
            case AVRO -> switch (type) {
                case ELEMENT -> of(ElementToAvroConverter.convert(schema, this), epochMillis);
                case AVRO -> this;
                case ROW -> of(RowToRecordConverter.convert(schema.getAvroSchema(), (Row)value), epochMillis);
                case STRUCT -> of(StructToAvroConverter.convert(schema.getAvroSchema(), (Struct) value), epochMillis);
                case DOCUMENT -> of(DocumentToAvroConverter.convert(schema.getAvroSchema(), (Document) value), epochMillis);
                case ENTITY -> of(EntityToAvroConverter.convert(schema.getAvroSchema(), (Entity) value), epochMillis);
                default -> throw new IllegalArgumentException("Convert from: " + type + " to Avro is not yet supported");
            };
            case ROW -> switch (type) {
                case ELEMENT -> of(ElementToRowConverter.convert(schema, this), epochMillis);
                case AVRO -> of(AvroToRowConverter.convert(schema.getRowSchema(), (GenericRecord) value), epochMillis);
                case ROW -> this;
                case DOCUMENT -> of(DocumentToRowConverter.convert(schema.getRowSchema(), (Document) value), epochMillis);
                case ENTITY -> of(EntityToRowConverter.convert(schema.getRowSchema(), (Entity) value), epochMillis);
                default -> throw new IllegalArgumentException("Convert from: " + type + " to Row is not yet supported");
            };
            case DOCUMENT -> switch (type) {
                case ELEMENT -> of(ElementToDocumentConverter.convertBuilder(schema, this).build(), epochMillis);
                case AVRO -> of(AvroToDocumentConverter.convertBuilder(schema.getAvroSchema(), (GenericRecord) value).build(), epochMillis);
                case ROW -> of(RowToDocumentConverter.convertBuilder(schema.getRowSchema(), (Row) value).build(), epochMillis);
                case DOCUMENT -> this;
                case ENTITY -> of(EntityToDocumentConverter.convert(schema.getRowSchema(), (Entity) value).build(), epochMillis);
                default -> throw new IllegalArgumentException("Convert from: " + type + " to Document is not yet supported");
            };
            case ENTITY -> switch (type) {
                case ELEMENT -> of(ElementToEntityConverter.convertBuilder(schema, this).build(), epochMillis);
                case AVRO -> of(AvroToEntityConverter.convertBuilder(schema.getAvroSchema(), (GenericRecord) value).build(), epochMillis);
                case ROW -> of(RowToEntityConverter.convertBuilder(schema.getRowSchema(), (Row) value).build(), epochMillis);
                case ENTITY -> this;
                default -> throw new IllegalArgumentException("Convert from: " + type + " to Entity is not yet supported");
            };
            default -> throw new IllegalArgumentException("Convert from: " + type + " to: " + dataType + " is not yet supported");
        };
    }

    // getter
    public Object getPrimitiveValue(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> ElementSchemaUtil.getValue((Map<String, Object>) value, field);
            case ROW -> RowSchemaUtil.getAsPrimitive((Row) value, field);
            case AVRO -> AvroSchemaUtil.getAsPrimitive((GenericRecord) value, field);
            case STRUCT -> StructSchemaUtil.getAsPrimitive(((Struct) value).getValue(field));
            case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(((Document) value).getFieldsMap().get(field));
            case ENTITY -> EntitySchemaUtil.getAsPrimitive(((Entity) value).getPropertiesMap().get(field));
            case MESSAGE -> MessageSchemaUtil.getAsPrimitive((PubsubMessage) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public Map<String, Object> asPrimitiveMap() {
        return asPrimitiveMap(null);
    }

    public Map<String, Object> asPrimitiveMap(final Collection<String> fieldNames) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                if(fieldNames == null || fieldNames.isEmpty()) {
                    yield new HashMap<>(map);
                }
                yield fieldNames.stream().collect(Collectors.toMap(f -> f, map::get));
            }
            case ROW -> RowSchemaUtil.asPrimitiveMap((Row) value);
            case AVRO -> AvroSchemaUtil.asPrimitiveMap((GenericRecord) value);
            case STRUCT -> StructSchemaUtil.asPrimitiveMap((Struct) value);
            case DOCUMENT -> DocumentSchemaUtil.asPrimitiveMap((Document) value);
            case ENTITY -> EntitySchemaUtil.asPrimitiveMap((Entity) value);
            case MESSAGE -> MessageSchemaUtil.asPrimitiveMap((PubsubMessage) value);
            default -> throw new IllegalArgumentException();
        };
    }

    public Object getAsStandardValue(final Schema.Field field) {
        return Optional.ofNullable(value)
                .map(v -> switch (type) {
                    case ELEMENT -> getAsStandardValue(field.getFieldType(), ((Map<String, Object>) value).get(field.getName()));
                    case AVRO -> AvroSchemaUtil.getAsStandard((GenericRecord) value, field.getName());
                    case ROW -> RowSchemaUtil.getAsStandard((Row) value, field.getName());
                    case STRUCT -> StructSchemaUtil.getAsStandard(((Struct) value).getValue(field.getName()));
                    case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(((Document) value).getFieldsMap().get(field.getName()));
                    case ENTITY -> EntitySchemaUtil.getAsPrimitive(((Entity) value).getPropertiesMap().get(field.getName()));
                    case MESSAGE -> MessageSchemaUtil.getAsPrimitive((PubsubMessage) value, field.getName());
                    default -> throw new IllegalArgumentException("Not supported type: " + type);
                })
                .orElse(null);
    }

    public Map<String, Object> asStandardMap(final Schema schema, final Collection<String> fieldNames) {
        return Optional.ofNullable(value)
                .map(v -> switch (type) {
                    case ELEMENT -> {
                        final Map<String, Object> standardValues = new HashMap<>();
                        final Map<String, Object> primitiveValues = (Map<String, Object>) value;
                        for(final Schema.Field field : schema.getFields()) {
                            if(fieldNames != null && !fieldNames.isEmpty() && !fieldNames.contains(field.getName())) {
                                continue;
                            }
                            final Object standardValue = getAsStandardValue(field.getFieldType(), primitiveValues.get(field.getName()));
                            standardValues.put(field.getName(), standardValue);
                        }
                        yield standardValues;
                    }
                    case AVRO -> AvroSchemaUtil.asStandardMap((GenericRecord) value, fieldNames);
                    case ROW -> RowSchemaUtil.asStandardMap((Row) value, fieldNames);
                    case STRUCT -> StructSchemaUtil.asStandardMap((Struct) value, fieldNames);
                    case DOCUMENT -> DocumentSchemaUtil.asStandardMap((Document) value, fieldNames);
                    case ENTITY -> EntitySchemaUtil.asStandardMap((Entity) value, fieldNames);
                    case MESSAGE -> MessageSchemaUtil.asPrimitiveMap((PubsubMessage) value);
                    default -> throw new IllegalArgumentException("Not supported type: " + type);
                })
                .orElseGet(HashMap::new);
    }

    public static Object getAsStandardValue(final Schema.FieldType fieldType, final Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool -> switch (primitiveValue) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case string, json -> switch (primitiveValue) {
                case String s -> s;
                case ByteBuffer bb -> new String(Base64.getDecoder().decode(bb.array()), StandardCharsets.UTF_8);
                case byte[] b -> new String(b, StandardCharsets.UTF_8);
                case Map map -> ElementToJsonConverter.convert(fieldType.getElementSchema(), map);
                case Object o -> o.toString();
            };
            case bytes -> switch (primitiveValue) {
                case ByteBuffer bb -> bb;
                case byte[] b -> ByteBuffer.wrap(b);
                case String s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
                case ByteString bs -> bs.asReadOnlyByteBuffer();
                default -> throw new IllegalArgumentException();
            };
            case int32 -> switch (primitiveValue) {
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                case Boolean b -> b ? 1 : 0;
                default -> throw new IllegalArgumentException();
            };
            case int64 -> switch (primitiveValue) {
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                case Boolean b -> b ? 1L : 0L;
                default -> throw new IllegalArgumentException();
            };
            case float32 -> switch (primitiveValue) {
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                case Boolean b -> b ? 1F : 0F;
                default -> throw new IllegalArgumentException();
            };
            case float64 -> switch (primitiveValue) {
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                case Boolean b -> b ? 1D : 0D;
                default -> throw new IllegalArgumentException();
            };
            case date -> switch (primitiveValue) {
                case Number n -> LocalDate.ofEpochDay(n.longValue());
                case String s -> DateTimeUtil.toLocalDate(s);
                default -> throw new IllegalArgumentException();
            };
            case time -> switch (primitiveValue) {
                case Number n -> LocalTime.ofNanoOfDay(n.longValue() * 1000L);
                case String s -> DateTimeUtil.toLocalTime(s);
                default -> throw new IllegalArgumentException();
            };
            case timestamp -> switch (primitiveValue) {
                case Number n -> DateTimeUtil.toInstant(n.longValue());
                case String s -> DateTimeUtil.toInstant(s);
                default -> throw new IllegalArgumentException();
            };
            case enumeration -> switch (primitiveValue) {
                case Number n -> n.intValue() < fieldType.getSymbols().size() ? fieldType.getSymbols().get(n.intValue()) : null;
                case String s -> fieldType.getSymbols().contains(s) ? s : null;
                default -> throw new IllegalArgumentException();
            };
            case element, map -> switch (primitiveValue) {
                case Map map -> {
                    final Map<String, Object> standardValues = new HashMap<>();
                    for(final Schema.Field field : fieldType.getElementSchema().getFields()) {
                        standardValues.put(field.getName(), getAsStandardValue(field.getFieldType(), map.get(field.getName())));
                    }
                    yield standardValues;
                }
                default -> throw new IllegalArgumentException();
            };
            case array -> switch (primitiveValue) {
                case List list -> {
                    final List<Object> standardValues = new ArrayList<>();
                    for(final Object v : list) {
                        final Object standardValue = getAsStandardValue(fieldType.getArrayValueType(), v);
                        standardValues.add(standardValue);
                    }
                    yield  standardValues;
                }
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException();
        };
    }

    public String getAsString(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                final Object fieldValue = ElementSchemaUtil.getValue(map, field);
                yield Optional
                        .ofNullable(fieldValue)
                        .map(Object::toString)
                        .orElse(null);
            }
            case ROW -> RowSchemaUtil.getAsString(((Row) value), field);
            case AVRO -> Optional.ofNullable(((GenericRecord) value).get(field)).map(Object::toString).orElse(null);
            case STRUCT -> StructSchemaUtil.getAsString((Struct) value, field);
            case DOCUMENT -> DocumentSchemaUtil.getAsString((Document) value, field);
            case ENTITY -> EntitySchemaUtil.getAsString((Entity) value, field);
            case MESSAGE -> MessageSchemaUtil.getAsString((PubsubMessage) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public ByteBuffer getAsBytes(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Object fieldValue = ((Map<String, Object>) value).get(field);
                yield switch (fieldValue) {
                    case String s -> ByteBuffer.wrap(Base64.getDecoder().decode(s));
                    case ByteBuffer bf -> bf;
                    case byte[] b -> ByteBuffer.wrap(b);
                    default -> throw new IllegalArgumentException();
                };
            }
            case AVRO -> AvroSchemaUtil.getAsBytes((GenericRecord) value, field);
            case ROW -> RowSchemaUtil.getAsBytes((Row) value, field);
            case STRUCT -> StructSchemaUtil.getAsBytes((Struct) value, field);
            case DOCUMENT -> DocumentSchemaUtil.getAsBytes((Document) value, field);
            case ENTITY -> EntitySchemaUtil.getAsBytes((Entity) value, field);
            case MESSAGE -> MessageSchemaUtil.getAsBytes((PubsubMessage) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public Double getAsDouble(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                final Object fieldValue = map.get(field);
                yield switch (fieldValue) {
                    case Double v -> v;
                    case Number n -> n.doubleValue();
                    case ByteBuffer b -> BigDecimal.valueOf(new BigInteger(b.array()).longValue(), 9).doubleValue();
                    case Boolean b -> b ? 1D : 0D;
                    case String s -> Double.parseDouble(s);
                    case null, default -> null;
                };
            }
            case ROW -> RowSchemaUtil.getAsDouble((Row) value, field);
            case AVRO -> AvroSchemaUtil.getAsDouble((GenericRecord) value, field);
            case STRUCT -> StructSchemaUtil.getAsDouble((Struct) value, field);
            case DOCUMENT -> DocumentSchemaUtil.getAsDouble((Document) value, field);
            case ENTITY -> EntitySchemaUtil.getAsDouble((Entity) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public Long getAsLong(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                final Object fieldValue = map.get(field);
                yield switch (fieldValue) {
                    case Long v -> v;
                    case Number n -> n.longValue();
                    case ByteBuffer b -> BigDecimal.valueOf(new BigInteger(b.array()).longValue(), 9).longValue();
                    case Boolean b -> b ? 1L : 0L;
                    case String s -> Long.parseLong(s);
                    case null, default -> null;
                };
            }
            case ROW -> RowSchemaUtil.getAsLong((Row) value, field);
            case AVRO -> AvroSchemaUtil.getAsLong((GenericRecord) value, field);
            case STRUCT -> StructSchemaUtil.getAsLong((Struct) value, field);
            case DOCUMENT -> DocumentSchemaUtil.getAsLong((Document) value, field);
            case ENTITY -> EntitySchemaUtil.getAsLong((Entity) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public BigDecimal getAsBigDecimal(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                final Object fieldValue = map.get(field);
                yield switch (fieldValue) {
                    case BigDecimal d -> d;
                    case Double v -> BigDecimal.valueOf(v);
                    case Number n -> BigDecimal.valueOf(n.doubleValue());
                    case ByteBuffer b -> BigDecimal.valueOf(new BigInteger(b.array()).longValue(), 9);
                    case Boolean b -> b ? BigDecimal.valueOf(1D) : BigDecimal.valueOf(0D);
                    case String s -> BigDecimal.valueOf(Double.parseDouble(s));
                    case null, default -> null;
                };
            }
            case ROW -> RowSchemaUtil.getAsBigDecimal((Row) value, field);
            case AVRO -> AvroSchemaUtil.getAsBigDecimal((GenericRecord) value, field);
            case STRUCT -> StructSchemaUtil.getAsBigDecimal((Struct) value, field);
            case DOCUMENT -> DocumentSchemaUtil.getAsBigDecimal((Document) value, field);
            case ENTITY -> EntitySchemaUtil.getAsBigDecimal((Entity) value, field);
            default -> throw new IllegalArgumentException();
        };
    }

    public org.joda.time.Instant getAsJodaInstant(final String field) {
        if(value == null) {
            return null;
        }
        return switch (type) {
            case ELEMENT -> {
                final Map<String, Object> map = (Map<String, Object>) value;
                final Object fieldValue = map.get(field);
                if(fieldValue == null) {
                    yield null;
                }
                yield DateTimeUtil.toJodaInstant((Long) fieldValue);
            }
            case ROW -> RowSchemaUtil.getAsInstant((Row) value, field);
            case AVRO -> DateTimeUtil.toJodaInstant((Long)((GenericRecord) value).get(field));
            case STRUCT -> StructSchemaUtil.getTimestamp((Struct) value, field, null);
            case DOCUMENT -> DocumentSchemaUtil.getTimestamp((Document) value, field, null);
            case ENTITY -> EntitySchemaUtil.getTimestamp((Entity) value, field, null);
            default -> throw new IllegalArgumentException();
        };
    }

    public static Object getValue(Map<String, Object> primitiveMap, String fieldName, Schema.FieldType fieldType) {
        final Object primitiveValue = primitiveMap.get(fieldName);
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case element -> {
                final Map<String, Object> input = (Map<String, Object>) primitiveValue;
                final Map<String, Object> map = new HashMap<>();
                for(Schema.Field childField : fieldType.getElementSchema().getFields()) {
                    final Object value = getValue(input, childField.getName(), childField.getFieldType());
                    map.put(childField.getName(), value);
                    //map.put(childField.getName(), input.get(childField.getName()));
                }
                yield map;
            }
            case array -> {
                final List<?> list = switch (primitiveValue) {
                    case List<?> l -> l;
                    case Map<?, ?> m -> List.of(m);
                    case Object o -> List.of(o);
                };
                if(Schema.Type.element.equals(fieldType.getArrayValueType().getType())) {
                    final List<Object> mapList = new ArrayList<>();
                    for(final Object value : list) {
                        final Map<String, Object> outputMap = new HashMap<>();
                        final Map<String, Object> childMap = switch (value) {
                            case Map map -> map;
                            default -> throw new IllegalArgumentException("Illegal each value: " + value);
                        };
                        for(Schema.Field childField : fieldType.getArrayValueType().getElementSchema().getFields()) {
                            final Object childValue = getValue(childMap, childField.getName(), childField.getFieldType());
                            outputMap.put(childField.getName(), childValue);
                        }
                        mapList.add(outputMap);
                    }
                    yield mapList;
                } else {
                    yield list;
                }
            }
            default -> primitiveValue;
        };
    }

    public static class Builder implements Serializable {

        private int index = 0;
        private final Map<String, Object> values;
        private long epochMillis = 0L;

        private Builder() {
            this.values = new HashMap<>();
        }

        private Builder(MElement element) {
            this.index = element.getIndex();
            this.values = element.asPrimitiveMap();
            this.epochMillis = element.getEpochMillis();
        }

        private Builder(Map<String, Object> values) {
            this.values = values;
        }

        public Builder withPrimitiveValue(String field, Object value) {
            this.values.put(field, value);
            return this;
        }

        public Builder withPrimitiveValues(Map<String, Object> map) {
            if(map != null) {
                this.values.putAll(map);
            }
            return this;
        }

        public Builder withBool(String field, Boolean value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withString(String field, String value) {
            return withPrimitiveValue(field, Optional.ofNullable(value).map(String::toString).orElse(null));
        }

        public Builder withBytes(String field, byte[] value) {
            return withPrimitiveValue(field, ByteBuffer.wrap(value));
        }

        public Builder withInt32(String field, Integer value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withInt64(String field, Long value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withFloat32(String field, Float value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withFloat64(String field, Double value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withDecimal(String field, BigDecimal value) {
            return withPrimitiveValue(field, value);
        }

        public Builder withDate(String field, LocalDate value) {
            final Integer epochMicros = Optional.ofNullable(value)
                    .map(LocalDate::toEpochDay)
                    .map(Long::intValue)
                    .orElse(null);
            return withPrimitiveValue(field, epochMicros);
        }

        public Builder withTime(String field, LocalTime value) {
            final Long epochMicros = Optional.ofNullable(value)
                    .map(LocalTime::toNanoOfDay)
                    .map(n -> n / 1000L)
                    .orElse(null);
            return withPrimitiveValue(field, epochMicros);
        }

        public Builder withTimestamp(String field, Instant value) {
            final Long epochMicros = Optional.ofNullable(value)
                    .map(DateTimeUtil::toEpochMicroSecond)
                    .orElse(null);
            return withPrimitiveValue(field, epochMicros);
        }

        public Builder withTimestamp(String field, org.joda.time.Instant value) {
            final Long epochMicros = Optional.ofNullable(value)
                    .map(i -> i.getMillis() * 1000L)
                    .orElse(null);
            return withPrimitiveValue(field, epochMicros);
        }

        public Builder withElement(String field, MElement value) {
            return withPrimitiveValue(field, value.asPrimitiveMap());
        }

        public Builder withMap(String field, Map<String, ? extends Object> map) {
            final Map<String, Object> values = new HashMap<>();
            for(final Map.Entry<String, ?> entry : map.entrySet()) {
                final Object value = convertPrimitiveValue(entry.getValue());
                values.put(entry.getKey(), value);
            }
            return withPrimitiveValue(field, values);
        }

        public Builder withBoolList(String field, List<Boolean> values) {
            return withPrimitiveValue(field, values);
        }

        public Builder withStringList(String field, List<String> values) {
            return withPrimitiveValue(field, values);
        }

        public Builder withBytesList(String field, List<ByteBuffer> values) {
            return withPrimitiveValue(field, values);
        }

        public Builder withElementList(String field, List<MElement> values) {
            final List<Map<String, Object>> maps;
            if(values != null) {
                maps = values.stream().map(MElement::asPrimitiveMap).toList();
            } else {
                maps = new ArrayList<>();
            }
            return withPrimitiveValue(field, maps);
        }

        public Builder withIndex(int index) {
            this.index = index;
            return this;
        }

        public Builder withEventTime(Instant eventTime) {
            this.epochMillis = DateTimeUtil.toEpochMicroSecond(eventTime) / 1000L;
            return this;
        }

        public Builder withEventTime(org.joda.time.Instant eventTime) {
            this.epochMillis = eventTime.getMillis();
            return this;
        }

        public MElement build() {
            return new MElement(index, DataType.ELEMENT, this.values, epochMillis);
        }

        private Object convertPrimitiveValue(final Object value) {
            return switch (value) {
                case String s -> s;
                case Boolean b -> b;
                case Float f -> f;
                case Double d -> d;
                case Integer i -> i;
                case Long l -> l;
                case ByteBuffer b -> b;
                case LocalDate l -> Long.valueOf(l.toEpochDay()).intValue();
                case LocalTime l -> DateTimeUtil.toMicroOfDay(l);
                case Instant i -> DateTimeUtil.toEpochMicroSecond(i);
                case org.joda.time.Instant i -> i.getMillis() * 1000L;
                case Map<?,?> m -> m.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> convertPrimitiveValue(e.getValue())));
                case List<?> l -> l.stream()
                        .map(this::convertPrimitiveValue)
                        .toList();
                case null -> null;
                default -> throw new IllegalArgumentException("Not supported type value: " + value);
            };
        }

    }

    public static com.mercari.solution.module.Schema dummySchema() {
        return Schema.builder()
                .withField("field", Schema.FieldType.STRING)
                .build();
    }

    public static MElement createDummyElement(final org.joda.time.Instant timestamp) {
        return MElement.builder()
                .withString("field", "")
                .withEventTime(timestamp)
                .build();
    }

    @Override
    public String toString() {
        return String.format("""
{ "type": "%s", "index": %d, "timestamp": %s, "value": %s }""", type, index, org.joda.time.Instant.ofEpochMilli(epochMillis), value);
    }

}
