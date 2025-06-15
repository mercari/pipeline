package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Bytes implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean encode;
    private final boolean ignore;

    Bytes(String name, String field, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean encode, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.encode = encode;
        this.ignore = ignore;
    }

    public static Bytes of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean encode, boolean ignore) {
        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField bytes: " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final String type;
        if(!jsonObject.has("type")) {
            if(!encode) {
                throw new IllegalArgumentException("SelectField bytes: " + name + " requires type parameter");
            }
            type = "bytes";
        } else {
            type = jsonObject.get("type").getAsString();
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField bytes: " + name + " missing inputField: " + field);
        } else if(!Schema.Type.bytes.equals(inputFieldType.getType()) && !encode) {
            throw new IllegalArgumentException("SelectField bytes: " + name + " input inputField must be bytes but : " + inputFieldType.getType());
        }
        fields.add(Schema.Field.of(field, inputFieldType));

        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));
        return new Bytes(name, field, fields, outputFieldType, encode, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Object primitiveValue = ElementSchemaUtil.getValue(input, field);
        if(primitiveValue == null) {
            return null;
        }
        if(encode) {
            return encode(primitiveValue);
        } else {
            return switch (primitiveValue) {
                case byte[] bytes -> decode(outputFieldType.getType(), bytes);
                case ByteBuffer byteBuffer -> decode(outputFieldType.getType(), byteBuffer.array());
                default -> throw new IllegalArgumentException();
            };
        }
    }

    private static Object decode(final Schema.Type type, byte[] bytes) {
        return switch (type) {
            case bool -> org.apache.hadoop.hbase.util.Bytes.toBoolean(bytes);
            case string, json -> org.apache.hadoop.hbase.util.Bytes.toString(bytes);
            case int16 -> org.apache.hadoop.hbase.util.Bytes.toShort(bytes);
            case int32, date -> org.apache.hadoop.hbase.util.Bytes.toInt(bytes);
            case int64, time, timestamp -> org.apache.hadoop.hbase.util.Bytes.toLong(bytes);
            case float32 -> org.apache.hadoop.hbase.util.Bytes.toFloat(bytes);
            case float64 -> org.apache.hadoop.hbase.util.Bytes.toDouble(bytes);
            case decimal -> org.apache.hadoop.hbase.util.Bytes.toBigDecimal(bytes);
            case bytes -> ByteBuffer.wrap(bytes);
            default -> throw new IllegalArgumentException("");
        };
    }

    private static ByteBuffer encode(final Object object) {
        if(object == null) {
            return null;
        }
        final byte[] bytes = switch (object) {
            case Boolean b -> org.apache.hadoop.hbase.util.Bytes.toBytes(b);
            case String s -> org.apache.hadoop.hbase.util.Bytes.toBytes(s);
            case Short s -> org.apache.hadoop.hbase.util.Bytes.toBytes(s);
            case Integer i -> org.apache.hadoop.hbase.util.Bytes.toBytes(i);
            case Long l -> org.apache.hadoop.hbase.util.Bytes.toBytes(l);
            case Float f -> org.apache.hadoop.hbase.util.Bytes.toBytes(f);
            case Double d -> org.apache.hadoop.hbase.util.Bytes.toBytes(d);
            case BigDecimal b -> org.apache.hadoop.hbase.util.Bytes.toBytes(b);
            case ByteBuffer b -> b.array();
            case byte[] b -> b;
            default -> throw new IllegalArgumentException();
        };
        return ByteBuffer.wrap(bytes);
    }

}
